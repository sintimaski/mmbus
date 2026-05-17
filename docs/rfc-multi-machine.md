# RFC: Multi-machine bridge

**Status:** Draft.  Scoped for a post-1.0 separate sub-project (own
crate, own binary).  Documenting now so the local API choices we make
in v0.x don't paint us into a corner.
**Owner:** _unassigned_

## 1. Problem

Today mmbus is strictly single-machine.  A process can only publish to
or subscribe from the same kernel that owns the `mmap` file.  For
multi-machine workloads users have to add a real broker (Redis,
ZeroMQ over TCP, NATS) — which is exactly the dependency mmbus exists
to avoid.

We want to extend the reach to multiple machines *without changing the
local API*.  An app written against `Bus::subscribe("events")` should
keep working whether the publisher is on the same host or three
datacenters away.

## 2. Design

A separate process, **`mmbus-bridge`**, runs on each participating
machine.  Its job:

* Subscribe locally to a configured set of topics.
* Forward each received message to peer bridges over the network.
* Receive forwarded messages from peers and republish them locally.

Local apps are unaware of the bridge: they keep using the normal
`Bus`.  The bridge looks like an extra subscriber (when forwarding
out) and an extra publisher (when republishing in).

```
┌────────── machine A ──────────┐         ┌────────── machine B ──────────┐
│                               │         │                               │
│  app1 ───► mmbus  ◄─── app2   │         │  app3 ───► mmbus  ◄─── app4   │
│             │                 │         │             ▲                 │
│             ▼                 │         │             │                 │
│       mmbus-bridge ───── TCP/QUIC ─────► mmbus-bridge                  │
│                               │         │                               │
└───────────────────────────────┘         └───────────────────────────────┘
```

Topology: **full mesh** of bridges (each bridge knows every peer).
Hub-and-spoke would be simpler but loses pub/sub fan-out symmetry and
adds a single-point-of-failure that defeats the "no broker" pitch.

## 3. Wire protocol

Length-prefixed binary frames over a stream transport:

```text
struct Frame {
    u32  version       // = 1
    u32  frame_type    // 0=msg, 1=ping, 2=topic-subscribe, 3=peer-hello
    u64  origin_id     // 64-bit random per bridge — loop prevention
    u64  origin_seq    // monotonic per origin_id — gap detection
    u32  topic_len
    [u8] topic_bytes
    u32  payload_len
    [u8] payload_bytes
}
```

- **`origin_id`**: random per-bridge identifier.  A bridge ignores
  incoming messages whose `origin_id` matches its own — that's how we
  prevent infinite loops when a message goes A → B → A.
- **`origin_seq`**: monotonic counter per origin per topic.  Lets the
  receiver detect drops (gaps) and request resync if a WAL is present
  (see WAL RFC).
- **`frame_type`**: `peer-hello` exchanged on connect for protocol
  version negotiation + origin_id announcement; `topic-subscribe`
  lets a peer request a specific topic instead of receiving the
  bridge's default forward set.

## 4. Transport

Three viable options, picked by config (default: `quic`):

| Transport | Pros | Cons |
|---|---|---|
| **Raw TCP** | trivial, no deps, works everywhere | no encryption, head-of-line blocking, separate keepalive |
| **QUIC** (via [`quinn`]) | encryption built-in, multiplexed streams per topic, fast reconnect | TLS cert mgmt; ~5k LOC of dep |
| **WebSocket** | works through corporate proxies | extra framing layer, perf cost |

[`quinn`]: https://crates.io/crates/quinn

QUIC is the recommended default because (a) the connection-per-topic
mapping aligns with QUIC's per-stream flow control — a slow topic
can't block others, and (b) the certificate dance is "self-signed
peer-pinned by default" which the bridge can manage itself (no PKI).

**Authentication**: pre-shared key in the `peer-hello`, exchanged via
config file.  No CA, no rotation story in v1.

## 5. Failure modes

| Failure | Behaviour |
|---|---|
| Peer offline | Bridge buffers up to N messages (configurable) per topic per peer; drops oldest on overflow; reconnects with exponential backoff. |
| Peer slow | Per-stream QUIC flow control naturally backpressures the slow peer without blocking others. |
| Network partition | Both sides keep accepting local publishes; on heal, each forwards the messages buffered during the partition.  Total order across machines is **not** preserved — only per-origin order. |
| Bridge process crash | Local apps keep working (no dependency on bridge for local pub/sub).  Cross-machine delivery pauses until the bridge restarts.  Messages published locally during the gap are lost unless a WAL is present (see WAL RFC). |
| Duplicate delivery | The `(origin_id, origin_seq)` pair is unique; the receiving bridge can dedupe in a small LRU per origin if desired (config). |

## 6. API impact

**Zero change to the local `Bus` API.**  The bridge is invisible to
local code.

New surfaces:

* Binary `mmbus-bridge` (in a new `bridge/` crate alongside `fuzz/`).
* Config file `mmbus-bridge.toml`:

  ```toml
  bus = "my-app"
  base_dir = "/tmp/mmbus"

  [[topics]]
  name = "events"
  forward = true
  receive = true

  [[peers]]
  name = "machine-b"
  endpoint = "machine-b.internal:4443"
  preshared_key = "..."
  ```

* Optional Python helper `mmbus.bridge.start(config_path)` for users
  who want to embed the bridge in a Python process.

## 7. MVP scope

For a v0.1 bridge release (separate from mmbus v1.0):

1. Mesh of 2 peers (no discovery, hardcoded endpoint list).
2. TCP transport only (QUIC is v0.2).
3. Per-peer buffer bounded by message count, drop-oldest on overflow.
4. Loop prevention via `origin_id`.
5. No dedup, no resync — best-effort delivery.
6. systemd service file + binary release.

Demos that should work end-to-end:
* Two-machine WebSocket fan-out: publisher on A, web clients on both.
* Cross-machine job queue: jobs published on A, workers on B.

Out of MVP scope (later):
* Discovery (mDNS, Consul, etc.) — config is fine for v1.
* Topic-level ACLs.
* TLS / mTLS / cert rotation — preshared key only.

## 8. Open questions

- **Topic naming under bridging**: do we prefix received messages with
  `origin/`?  Pro: avoids ambiguity when the same topic name exists
  locally and remotely.  Con: changes the topic name local apps see,
  breaking the "transparent bridge" promise.  *Lean: no prefix; require
  topic names to be globally unique across the federation.  Document.*
- **Ordering across peers**: a receiver gets messages from multiple
  origins interleaved.  Should we provide a per-origin ordering view?
  Probably yes via API on Subscription that exposes the source origin,
  but it's additive; not blocking.
- **Backpressure feedback**: if a local consumer is slow and the local
  ring fills, should the bridge slow down its remote peer?  Today the
  bridge would just see `BusFullError` and drop.  Maybe acceptable for
  v1.
- **Replay on reconnect**: requires the WAL (see WAL RFC) on both ends.
  Without WAL, mid-disconnect messages are lost.  Document the
  tradeoff; recommend WAL for bridge use cases.

## 9. Why not just use NATS / Redis / Kafka?

The pitch of mmbus is "no broker for local IPC".  The bridge extends
that pitch coherently: each *machine* still has no broker for its
local pub/sub, and the cross-machine link is a single-purpose process
the user can opt into.  The user gets:

* Local pub/sub stays zero-copy / lock-free (we don't lose the perf
  story on the inside).
* Cross-machine has a *simple*, *purpose-built* protocol — not a
  general-purpose broker with hundreds of features they don't use.
* The bridge process can crash without taking down local pub/sub.
