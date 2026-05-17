# RFC: mmbus-bridge QUIC transport (B4b)

**Status:** Draft.  Spec for the QUIC transport deferred from B4 of
`docs/rfc-multi-machine.md` + `docs/plan-rfcs.md`.  TCP transport
already ships (B0..B4a); the PSK authentication shape established in
B4a transfers directly.

**Owner:** _unassigned_

---

## 1. Why QUIC

Three properties matter for cross-machine bridging that TCP doesn't
give us out of the box:

1. **Built-in encryption.** TCP+TLS would also work but adds a fragile
   layering (cert termination + framing) we'd have to reinvent.
   QUIC's transport is TLS 1.3, full stop.
2. **Independent stream flow control.** A slow topic on one stream
   doesn't block other streams.  With TCP we get head-of-line
   blocking across all topics that share the connection.
3. **Fast reconnect.** QUIC's 0-RTT resumption is meaningful when
   bridges churn (rolling deploys, leader election) — TCP+TLS pays
   a full handshake per reconnect.

We do **not** need:
- Multi-path / connection migration (single-NIC peers in v1).
- 0-RTT data on first connect (out of scope; mutual PSK auth still
  needs a round trip).

---

## 2. Crate choice: `quinn`

[`quinn`](https://crates.io/crates/quinn) (~5 kLOC main crate + deps)
is the most actively-maintained Rust QUIC implementation; it's
async-only (built on `tokio`), uses `rustls` for the TLS layer, and
exposes a `Connection::open_bi()` / `accept_bi()` API that maps
cleanly onto our existing per-stream Frame model.

Alternatives considered + rejected:

| Crate | Why not |
|-------|---------|
| `s2n-quic` (AWS) | C-backed, build complexity (cmake), worse async story |
| `neqo` (Mozilla) | not maintained as a crate; primarily a Firefox internal |
| roll our own | absurd for this scope |

---

## 3. Async runtime: tokio (single dedicated thread)

The rest of the bridge is synchronous (sync threads + `std::sync`
primitives + the in-house `queue` module).  We don't want to flip
*everything* to async — too much churn for one new transport.

Resolution: spawn a dedicated tokio runtime on its own thread when
the bridge starts and any peer uses QUIC.  All QUIC I/O lives inside
that runtime.  Sync↔async bridges go through `std::sync::mpsc`
channels that the runtime polls via `tokio::task::spawn_blocking` (on
the sync→async direction) and via direct `Sender::send` (on the
async→sync direction).

Sketch:

```rust
let quic_runtime = tokio::runtime::Builder::new_multi_thread()
    .worker_threads(2)         // bound — most workloads are tiny
    .enable_all()
    .thread_name("mmbus-bridge-quic")
    .build()?;
let quic_handle = quic_runtime.handle().clone();
let quic_thread = std::thread::spawn(move || {
    quic_runtime.block_on(quic_main(...));
});
```

Two worker threads is plenty for typical bridge traffic (the
expensive ops are network I/O, not compute).  `worker_threads` is a
config knob in case anyone needs to tune it.

---

## 4. Cargo feature gating

Add a `quic` feature in `bridge/Cargo.toml`:

```toml
[features]
default = []
quic = ["dep:quinn", "dep:tokio", "dep:rustls", "dep:rcgen"]

[dependencies]
quinn = { version = "0.11", optional = true, default-features = false, features = ["runtime-tokio", "rustls"] }
tokio = { version = "1", optional = true, features = ["rt-multi-thread", "macros", "io-util", "sync"] }
rustls = { version = "0.23", optional = true, default-features = false, features = ["ring"] }
rcgen = { version = "0.13", optional = true, default-features = false, features = ["ring"] }
```

Building without `--features quic` leaves the bridge identical to
today's TCP-only build.  Wheels and the systemd-bound deployment
default to `--features quic` once it ships.

`rcgen` is for self-signed cert generation at first start; `rustls`
is QUIC's TLS layer; `quinn` is the QUIC protocol; `tokio` is the
runtime.  All four pinned to stable releases at draft time.

---

## 5. Config surface

Per-peer transport selection extends [`PeerConfig`]:

```toml
[[peers]]
name = "machine-b"
endpoint = "machine-b.internal:4443"
preshared_key = "..."
transport = "quic"          # NEW; "tcp" (default) | "quic"
peer_cert_fingerprint = "sha256:abc123…"  # NEW; required when transport = "quic"
```

Bridge-level config gains:

```toml
listen_quic = "0.0.0.0:4443"   # optional; same shape as `listen`
quic_cert_path = "/var/lib/mmbus/bridge.cert.pem"  # default
quic_key_path  = "/var/lib/mmbus/bridge.key.pem"   # default
quic_worker_threads = 2                            # default
```

If `quic_cert_path` doesn't exist on startup, the bridge generates a
fresh self-signed cert via `rcgen` and writes both files (key:
chmod 600).  The cert's SHA-256 fingerprint is logged at startup so
the operator can copy it into the peer's `peer_cert_fingerprint`.

`listen` (TCP) and `listen_quic` are independent — a bridge can
listen on both transports simultaneously to support a heterogeneous
mesh during a transport migration.

---

## 6. Trust model: self-signed peer-pinned

No CA, no rotation story.  Same operational model as SSH known_hosts:

1. Bridge A generates a self-signed cert at first start.  Logs
   `bridge: QUIC fingerprint = sha256:abc123…`.
2. Operator copies that fingerprint into bridge B's
   `peer_cert_fingerprint` for A.
3. On B's `quinn::Endpoint::connect_with(server_config_pinning_A)`,
   the rustls `ServerCertVerifier` validates that the offered cert's
   fingerprint matches the pinned value; anything else is a hard
   abort.
4. PSK auth still runs on top (PeerHello on the first QUIC stream).
   Cert pin proves "this is the bridge process I expect to be at this
   IP"; PSK proves "and it knows the shared secret" — defence in
   depth against a cert-private-key compromise that the operator
   hasn't yet noticed.

Rotation flow (not automated in v1):

1. Operator generates a new cert on bridge A.
2. Pushes the new fingerprint to bridge B's config.
3. SIGHUP / restart B (config reload is post-v1).
4. SIGHUP / restart A.

If anyone wants `acme` integration later, it slots in as another
`cert_provider` enum variant in the bridge config.

---

## 7. Stream model

Each (origin, peer) pair gets:

- **One control stream** (bidirectional, opened on connect): carries
  PeerHello + ping/pong; closed = peer death.
- **One forward stream** (unidirectional, opened lazily): carries
  serialized `Msg` Frames as a single byte sequence with the existing
  length-prefixed format (no per-Msg framing change — the existing
  `frame::decode` works as-is).

A future enhancement is "one forward stream per topic" so a slow
topic doesn't HOL-block other topics in QUIC's per-stream window.
For v1 we share a single stream — simpler, and the existing
drop-oldest queue at the sender already handles per-topic backpressure
before bytes hit the wire.

The receive side `accept_bi()` loop dispatches:
- First frame is PeerHello (auth check identical to B4a).
- After auth, the control stream becomes an idle keep-alive channel.
- The peer can `open_uni()` to start the forward stream; receiver
  reads + decodes Frames + republishes locally.

---

## 8. Lifecycle integration

`Bridge::start` becomes:

```rust
let tcp_peers = cfg.peers.iter().filter(|p| p.transport.is_tcp());
let quic_peers = cfg.peers.iter().filter(|p| p.transport.is_quic());

// existing path
spawn_tcp_subscribers(...);
spawn_tcp_forwarders(tcp_peers);
maybe_spawn_tcp_listener(cfg.listen);

// new path (only if quic_peers OR cfg.listen_quic is non-empty)
let quic_state = if quic_needed {
    let rt = build_quic_runtime(cfg.quic_worker_threads);
    let handle = rt.handle().clone();
    let endpoints = build_quic_endpoints(cfg, &handle)?;
    let thread = std::thread::spawn(move || rt.block_on(quic_main(endpoints)));
    Some((handle, thread))
} else {
    None
};
```

Subscriber threads remain unchanged — they push encoded Frame bytes
into a per-peer `queue::Sender<Vec<u8>>` regardless of transport.
The QUIC runtime is one more consumer of that channel; it forwards
the bytes by writing to its peer's open forward stream.

`Bridge::shutdown` adds: signal `quic_runtime.shutdown_background()`
+ join the runtime thread.

---

## 9. Failure modes

| Failure | TCP behaviour | QUIC behaviour |
|---|---|---|
| Peer offline at connect | exponential backoff, retry forever | same (quinn's connect API is fail-fast; we wrap it in our backoff loop) |
| Peer dies mid-stream | TCP RST → forwarder reconnects | QUIC connection error → reconnect; idle-timeout fires after `max_idle_timeout` (default 30 s) if peer is silent |
| Bridge-local out queue overflows | drop-oldest (B3) | same (the queue lives in the sync world, before bytes hit the QUIC stream) |
| Cert fingerprint mismatch | n/a | hard abort on connect — log and back off; never retry without operator config change |
| Cert key file unreadable at startup | n/a | bridge refuses to start; `BridgeError::Quic` propagated |

---

## 10. Open questions

- **`open_bi` vs `open_uni` for the forward path**: bidirectional
  would let the receiver send per-frame acks for at-least-once
  semantics, but adds latency and we already get at-least-once from
  WAL (when Phase B ships).  Lean: unidirectional.
- **Cert rotation without restart**: viable via quinn's `set_server_config`
  hot-swap but adds config-reload complexity.  Defer to v0.2.
- **mTLS instead of cert-pinning**: matches enterprise PKI shops but
  requires a CA story.  Defer; cert-pin is the v1 trust model.
- **0-RTT for fast reconnect**: quinn supports it but the cert-pin
  trust model needs care (0-RTT data can be replayed).  Disable in
  v1; revisit if reconnect latency becomes a complaint.
- **MTU discovery**: quinn does Path-MTU search by default; should we
  expose tuning?  Probably not in v1 — defaults are fine on LAN/WAN.

---

## 11. Implementation staging

| Stage | Scope |
|-------|-------|
| **B4b-1** | Cargo feature + dep additions; `transport::Transport` enum + per-peer `transport` config; refactor existing TCP into `transport::tcp` (no behavioural change) |
| **B4b-2** | `transport::quic` module: cert gen via rcgen, endpoint binding via quinn, peer-pin verifier in rustls.  Outbound only — accept-side is B4b-3 |
| **B4b-3** | QUIC listener — `accept_bi`, dispatch to reader, share the existing publisher channel.  Auth gate identical to B4a |
| **B4b-4** | Integration tests: cert pin success + mismatch, forward + receive over QUIC, mixed TCP + QUIC mesh (each peer can independently choose its transport) |
| **B4b-5** | Wheels: `wheels.yml` builds with `--features quic`.  Systemd unit notes the new file paths.  Operator docs in `bridge/README.md` |

Each stage is independently green; B4b-5 is the flip from "available
behind feature flag" to "default-on in the release wheel".

---

## 12. Acceptance criteria

- A bridge with `transport = "quic"` peers successfully exchanges
  PeerHello + Msg frames byte-for-byte identical to the TCP path.
- A cert fingerprint mismatch is detected at QUIC connect, the
  connection drops, and no Frame data is processed.
- A peer with `transport = "tcp"` and another peer with `transport =
  "quic"` in the same `[[peers]]` list both work simultaneously
  (transport choice is per-peer).
- `cargo test --features quic` (in `bridge/`) is green on Linux +
  macOS.  Windows QUIC is a follow-up (quinn supports Windows but
  our cross-compile pipeline hasn't been validated for it).
- `cargo bench` shows QUIC throughput within 30% of TCP at 1 MiB
  Msg payloads on localhost.  (Gap is dominated by the AES-GCM
  cost; acceptable for the encryption gain.)

---

## 13. Out of scope (this RFC)

- HTTP/3 over the QUIC connection (we're using raw QUIC streams).
- WebTransport for browser peers — interesting, but adds a different
  trust model + would be its own RFC.
- Connection migration across NICs.
- Server Name Indication routing on the QUIC endpoint (we're
  single-tenant per bind address).
