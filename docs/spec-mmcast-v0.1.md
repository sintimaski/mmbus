# Spec — `mmcast` v0.1

Status: accepted, 2026-06-03.  Implementation tracked by task list (T1–T10).
Supersedes any prior verbal scope.  This is the contract; the code follows it,
not the other way around.

---

## Goal

One sentence: **`uvicorn --workers 4` with WebSocket broadcast, no Redis
container.**

`mmcast` is a thin pure-Python library on top of `mmbus` that gives ASGI
apps (FastAPI / Starlette first) the same channel-broadcast pattern
`encode/broadcaster` offers, with two differences:

1. The transport is mmbus (same-host, mmap, ~720 ns wakeup) instead of
   Redis pub/sub.
2. New subscribers can replay the last N in-ring messages on connect — a
   capability Redis pub/sub does not have without switching to Streams.

## Non-goals (v0.1)

- **Cross-host fan-out inside the lib.**  `mmbus[bridge]` already does
  this at the `Bus` level; mmcast inherits it transparently.  The docs
  link to it; the lib has no awareness.
- **Auth / authz.**  Same-host trust model = mmbus's
  (`SECURITY.md`).  Web-app auth lives in the app.
- **Pattern subscriptions** (`chat.*`).  Defer until requested.
- **Django Channels layer.**  FastAPI/Starlette only for v0.1.
- **Codecs beyond a JSON helper.**  `publish_json` /
  `subscribe_json` are convenience wrappers; protobuf/msgpack/numpy
  are out.
- **Per-connection auth, RPC, scheduling, locks, queues.**  These are
  separate libraries in the `mmbus-*` family if they happen at all.

---

## Public API

```python
from mmbus_cast import Broadcast, Event, SlowConsumer

# Constructor mirrors mmbus.Bus kwargs (base_dir, capacity, slot_size, …)
broadcast = Broadcast("my-app", base_dir=None, capacity=None, slot_size=None)

async with broadcast:                       # opens the underlying Bus
    # ── Publish ──────────────────────────────────────────────────────────
    await broadcast.publish("chat", b"hello")
    await broadcast.publish_json("chat", {"user": "dd", "text": "hi"})

    # ── Subscribe (async iterator over Event) ────────────────────────────
    async with broadcast.subscribe(
        "chat",
        replay_last=20,                     # in-ring history; 0 = live only
        slow_policy="drop_oldest",          # | "drop_newest" | "disconnect"
        queue_depth=1024,                   # per-subscriber outbound buffer
    ) as sub:
        async for event in sub:             # event: Event(data: bytes, ...)
            handle(event.data)

    # ── Presence (opt-in, separate mmbus topic) ──────────────────────────
    async with broadcast.presence(
        "chat",
        member_id="alice",
        ttl_secs=15.0,
        heartbeat_secs=5.0,
    ) as p:
        print(p.members)                    # {"alice", "bob"}: snapshot
        async for change in p:              # PresenceChange(member, joined)
            ...
```

### Types

```python
@dataclass(frozen=True, slots=True)
class Event:
    data: bytes
    cursor: int                             # mmbus ring cursor
    # JSON helper (lazy):
    def json(self) -> Any: ...

@dataclass(frozen=True, slots=True)
class PresenceChange:
    member: str
    joined: bool                            # False == left

class SlowConsumer(RuntimeWarning):
    """Emitted (via `logging` + per-subscription `slow_count`) when the
    per-subscriber outbound queue overflows."""
```

### Exceptions

Re-exported from mmbus where they bubble through unchanged:
`BusFullError`, `MessageTooLargeError`, `ConnectTimeoutError`,
`AlreadyPublishingError`.

New: `BroadcastClosedError` (raised when publish/subscribe is called on
a closed `Broadcast`).

---

## Decision points (resolved)

| # | Question                  | Decision (v0.1)                                     | Why |
|---|---------------------------|-----------------------------------------------------|-----|
| 1 | Slow-client policy default | `drop_oldest` + emit `SlowConsumer` warning + bump `sub.slow_count` | Matches the existing mmbus `BackpressurePolicy::DropOldest` semantic at the lib layer.  WS connections wedge silently on backpressure — dropping is more debuggable than blocking. |
| 2 | Presence backend          | Separate mmbus topic `_presence:<channel>` carrying `(member_id, heartbeat_ts)` tuples; TTL eviction on the consumer side.  Cross-process presence rides the Broadcast's sharding — no extra plumbing. | No second state store.  Heartbeat-based expiry handles ungraceful disconnects without a coordinator. |
| 3 | Replay semantics          | In-ring history only (`bus.subscribe_with_history`) | The boring, free win.  Durable-WAL replay (`replay_from_cursor=`) lands in v0.2 once we've seen which apps want it. |
| 4 | PyPI name                 | `mmbus-cast` (import: `mmbus_cast`)                 | Mirrors `mmbus-bridge` precedent; signals family relationship; avoids any name-availability surprise. |

---

## Multi-worker constraint (load-bearing — read before coding)

mmbus enforces **single publisher per topic across processes** (the
per-topic `producer.lock` flock; see core `CLAUDE.md:11`).  That means
in a 4-worker FastAPI deployment, only *one* worker can be the
publisher of `chat`; the other three would get
`AlreadyPublishingError` on `bus.topic("chat")`.

v0.1 handles this by **per-worker sharding**: each worker publishes to
its own shard topic `chat.<worker_id>` and subscribes to *all* peer
shards.  The internal helper API:

```python
broadcast = Broadcast(
    "my-app",
    worker_id="w0",                      # this process's shard
    peers=["w0", "w1", "w2", "w3"],      # all shards to fan in from
)
```

- `publish("chat", data)` writes to `chat.w0`.
- `subscribe("chat")` opens N subscriptions, one per peer, merges them
  into the consumer's queue in arrival order.

Defaults: `worker_id=None`, `peers=None` → single-publisher mode (one
process owns the topic, others are subscribers only).  The FastAPI
helper (T7) derives `worker_id` + `peers` from the deployment env.

The N×N subscription count is bounded by mmbus's `max_subscribers`
default of 16 — fine for ≤ 4 workers, raise via Bus kwargs for more.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│ FastAPI app                                                  │
│   @app.websocket("/ws")                                      │
│   async def ws(socket, broadcast = Depends(get_broadcast)):  │
│       async with broadcast.subscribe("chat") as sub:         │
│           async for event in sub:                            │
│               await socket.send_bytes(event.data)            │
└──────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│ mmbus_cast.Broadcast                                         │
│ ┌──────────────────────────────────────────────────────────┐ │
│ │ Per-channel `Channel` wrapping mmbus.AsyncSubscription   │ │
│ │   • single mmbus subscriber per channel-process pair     │ │
│ │   • fanout to per-consumer asyncio.Queue                 │ │
│ │   • slow-policy enforced at the per-consumer queue       │ │
│ └──────────────────────────────────────────────────────────┘ │
│ ┌──────────────────────────────────────────────────────────┐ │
│ │ Presence: separate mmbus topic + TTL eviction loop       │ │
│ └──────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
                              │
                              ▼
                       mmbus.Bus  (mmap ring + WAL)
```

**One mmbus subscriber per channel per process.** The first
`broadcast.subscribe("chat")` call in a process opens an mmbus
subscription and starts a background asyncio task that fans out into
per-consumer queues.  Subsequent calls reuse the existing subscription.
This bounds mmbus subscriber count by `O(channels)` not
`O(websocket connections)` — important since
mmbus has a `max_subscribers` ceiling per topic (default 16).

---

## Dependency direction (load-bearing rule)

- `mmbus_cast` imports `mmbus`.  `mmbus` **never** imports `mmbus_cast`.
- mmcast may only use the public mmbus Python API (`Bus`, `AsyncSubscription`,
  exceptions).  No reaching into `_mmbus`, no monkey-patching.
- If mmcast needs something not in the public mmbus API, the path is:
  open an mmbus issue → mmbus minor bump → mmcast picks it up via the
  pinned range.  No stealth imports.

## Version pinning

| Phase                  | mmcast pins mmbus as              |
|------------------------|-----------------------------------|
| mmbus pre-1.0 (now)    | `mmbus>=0.5.1,<0.6`               |
| mmbus 1.0+             | `mmbus>=1,<2`                     |

mmcast versions independently.  First release: `mmbus-cast 0.1.0`,
tagged `mmcast-v0.1.0` (prefix avoids collision with `mmbus-v*` and
`bridge-v*` tags in the same repo).

---

## encode/broadcaster parity audit

Reference: <https://github.com/encode/broadcaster> (v0.3.x surface).

| broadcaster symbol             | mmcast v0.1                       | Status     |
|--------------------------------|-----------------------------------|------------|
| `Broadcast(url)`               | `Broadcast(name, **bus_kwargs)`   | ✓ covered (different signature, same role) |
| `await broadcast.connect()`    | `await broadcast.__aenter__()`    | ✓ covered (context manager only) |
| `await broadcast.disconnect()` | `await broadcast.__aexit__(...)`  | ✓ covered |
| `await broadcast.publish(ch, msg)` | `await broadcast.publish(ch, bytes)` | ✓ covered (bytes, not `str`; JSON helper separate) |
| `broadcast.subscribe(channel)` | `broadcast.subscribe(channel, ...)` | ✓ covered + extended (replay_last, slow_policy, queue_depth) |
| `Event.channel`, `Event.message` | `Event.data` (+ `cursor`)        | △ renamed (`data: bytes` instead of `message: str`; channel is implicit in the iterator) |
| Redis / Postgres / Memory / Kafka backends | mmbus-only                | ✗ skipped by design |
| `async for event in subscriber`| `async for event in sub`          | ✓ covered |
| Presence                       | `broadcast.presence(channel, ...)` | + addition (broadcaster doesn't have it; lifted from Phoenix Channels) |
| Pattern subs (`chat.*`)        | —                                 | ✗ deferred |

Migration story for broadcaster users: ~5-line diff (constructor +
`bytes` vs `str` + import path).  Documented in the README.

---

## Error / failure modes

| Surface           | Failure mode                                | mmcast response |
|-------------------|---------------------------------------------|-----------------|
| `publish`         | Bus full (`Error` policy)                   | re-raise `BusFullError` |
| `publish`         | Payload exceeds slot                        | re-raise `MessageTooLargeError` |
| `publish`         | After `__aexit__`                            | raise `BroadcastClosedError` |
| `subscribe`       | mmbus `max_subscribers` exhausted (channel) | re-raise `TooManySubscribersError` (only on *first* subscriber per channel per process; subsequent share the underlying sub) |
| Subscriber queue  | Outbound queue full + `drop_oldest`         | drop, log `SlowConsumer`, bump `sub.slow_count` |
| Subscriber queue  | Outbound queue full + `disconnect`          | raise `SlowConsumer` in the consumer's iterator, close |
| Underlying mmbus  | Publisher restart (generation bump)         | iterator raises `BroadcastClosedError`; caller reconnects |
| Presence loop     | Heartbeat publish fails (Bus full)          | log warning, continue (don't tear down) |

## Security contract

- Trust boundary identical to mmbus: same-user, same-host.
- mmcast does not read or write any new on-disk state outside the
  mmbus bus directory.  The presence topic is an ordinary mmbus topic,
  subject to the same `flock`-based single-publisher rule per channel
  (the presence publisher is the lib instance, one per process per
  channel — collisions raise `AlreadyPublishingError`).
- No network sockets opened by mmcast itself (cross-host is delegated
  to `mmbus[bridge]`).
- **Reserved namespace.**  Channel names starting with `_` are reserved
  for mmcast subsystems (presence uses `_presence.<channel>`).  Public
  `publish`/`subscribe`/`prepare`/`presence` reject `_`-prefixed names,
  path-traversal sequences, and non-allowlist characters with
  `InvalidChannelError` — so an app that derives a channel from
  untrusted input (e.g. a URL path parameter) cannot publish forged
  records onto a subsystem topic or steer the on-disk ring path outside
  the bus directory.  `worker_id`/`peers` are validated on the same
  allowlist (they become topic suffixes → path components).
- **WebSocket endpoints remain a public attack surface.**  mmcast adds
  no auth; the application must authenticate and (for browsers) enforce
  an `Origin` allowlist before `socket.accept()`.  The chat example
  demonstrates both.

## Observability

- Uses `logging.getLogger("mmbus_cast")`; ships at `WARNING` by default.
- Per-`Subscription`: `slow_count`, `delivered_count`.  (A per-subscriber
  `replayed_count` is deferred to v0.2 with the per-subscriber replay
  buffer — v0.1 replay is per-channel at open time, so a replayed-vs-live
  count can't be attributed per subscriber.)
  attributes.
- Per-`Broadcast`: `stats()` returns a snapshot dict per channel
  derived from `mmbus.Bus.stats(topic)` + the lib's queue depths.
- Hot paths are not log-instrumented (matches mmbus convention).

---

## Acceptance criteria (v0.1 ship-gate)

1. `pip install mmbus-cast` (after PyPI publish) installs cleanly on Linux + macOS, Python 3.9–3.13.
2. `Broadcast` + `subscribe` + `publish` round-trip across two asyncio tasks.
3. `replay_last=N` delivers the last N in-ring messages before the live tail.
4. Slow consumer with `drop_oldest` does not grow memory unboundedly; `slow_count` increments; `SlowConsumer` is logged.
5. Presence: two members see each other within one heartbeat; killing one triggers a leave event within `2 * ttl_secs`.
6. FastAPI chat example runs with `uvicorn --workers 4`; messages from one tab arrive in all others within p99 < 50 ms on the demo box.
7. Side-by-side docker-compose (`Redis + broadcaster` vs `mmcast`) produces a results table committed to the README.
8. The `mmbus_cast` package imports nothing from `mmbus._mmbus`; verified by a one-line test.
