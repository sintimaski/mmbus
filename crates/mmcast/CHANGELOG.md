# Changelog — mmbus-cast

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Semver: [SemVer 2.0.0](https://semver.org/spec/v2.0.0.html).

`mmbus-cast` versions independently of `mmbus`.  Releases are tagged
`mmcast-vX.Y.Z` to coexist with `mmbus-v*` and `bridge-v*` in the
shared repo.

## [0.1.0] - 2026-06-04

First release.  Pre-1.0, API may iterate.

### Added

- **`Broadcast(name, *, worker_id=None, peers=None, **bus_kwargs)`** —
  the public async surface.  Wraps `mmbus.Bus` with a broadcast-shaped
  default (`backpressure="drop_oldest"`).
  - `await broadcast.publish(channel, bytes)` /
    `await broadcast.publish_json(channel, obj)`
  - `await broadcast.subscribe(channel, *, replay_last=0, slow_policy="drop_oldest", queue_depth=1024, connect_timeout_secs=30.0)`
  - `await broadcast.prepare(*channels)` — claim publisher slots at
    app startup so the first subscriber doesn't pay a connect-timeout
    wait.
  - `broadcast.presence(channel, *, member_id, ttl_secs=15.0, heartbeat_secs=5.0, changes_queue_max=4096)`
    — async context manager + iterator over `PresenceChange` events;
    backed by a separate `_presence.<channel>` mmbus topic with TTL
    heartbeat eviction.
  - `subscribe()` returns an awaitable async-context-manager, so both
    `await bc.subscribe(...)` and `async with bc.subscribe(...) as sub`
    work.  Arguments are validated eagerly at the call.
- **`Subscription`** — async iterator over `Event(data, cursor)`;
  per-consumer `slow_count` and `delivered_count` counters.
- **`SlowConsumer`** policy matrix: `drop_oldest` (default),
  `drop_newest`, `disconnect`.  Surfaced via `logging.getLogger("mmbus_cast")`
  warnings and the per-`Subscription` counters.
- **`mmbus_cast.fastapi.broadcast_lifespan(name, **kwargs)`** — ASGI
  lifespan helper that opens / closes a `Broadcast` for a FastAPI /
  Starlette app and optionally `prepare()`s a list of channels.
- **`mmbus_cast.fastapi.worker_shard_from_env(workers=N)`** — resolves
  `(worker_id, peers)` from `MMCAST_WORKER_ID` /  `MMCAST_PEERS`
  env vars or a worker count, for the per-worker sharding pattern
  multi-worker FastAPI deployments need.
- **Examples**:
  - `examples/fastapi_chat/` — full FastAPI chat app with HTMX
    frontend, an `Origin` allowlist (`MMCAST_ALLOWED_ORIGINS`), and
    per-message error handling.
  - `examples/benchmark/` — docker-compose harness for the side-by-
    side comparison vs `encode/broadcaster` + Redis (loadgen, both
    Dockerfiles, healthchecks, results JSON).

### Security & robustness

- **Channel-name validation** (`InvalidChannelError`): the `_` prefix is
  reserved for internal subsystems (presence); path-traversal and
  non-allowlist names are rejected at `publish`/`subscribe`/`prepare`/
  `presence`, and `worker_id`/`peers` are validated too (they become
  on-disk path components).  An app deriving a channel from untrusted
  input can't escape into a subsystem topic or an out-of-tree mmap path.
- **Presence hardening**: a single malformed record on the presence topic
  can no longer kill the consume loop (pure, total parser); the change
  queue is bounded (drop-oldest); the member table and member-id length
  are capped; a member never TTL-evicts itself; the heartbeat interval
  has a floor.
- **Concurrency**: per-channel locking prevents duplicate mmbus
  subscriptions under concurrent `subscribe()`; a subscribe racing
  `__aexit__` can't resurrect an orphan channel; consumer close is
  signalled out-of-band (asyncio.Event) so a full queue can't strand an
  iterator.
- **Staggered multi-worker startup**: a peer shard offline at subscribe
  time is retried by a background reconnect loop until convergence, so
  workers can start in any order.

### Notes

- mmbus dependency pinned to `>=0.5.1,<0.6` (0.5.1 is the first
  release with the Python 3.9–3.13 wheel matrix; pre-1.0 floor-and-
  ceiling rule).  Will widen to `>=1,<2` after mmbus's 1.0 freeze.
- Single-publisher mode is the default (one process owns a topic);
  sharded mode kicks in when `worker_id` + `peers` are supplied.
  Sharded mode now covers presence too — Presence rides the same
  per-worker shards as chat publishes, so cross-process presence works
  without extra config.  Verified by `test_presence_multiworker.py`.
- `replay_last=N` is in-ring only (mmbus's
  `subscribe_with_history`).  Durable-WAL replay is planned for v0.2
  along with a per-subscriber replay buffer.
- Tests: 100+ cases across smoke, validation, broadcast core, the
  subscribe API shape, concurrency races, backpressure, replay, presence
  (incl. malformed-input hardening), reconnect convergence, FastAPI
  helpers + a fresh-app integration test (WS↔WS and HTTP-publish→WS),
  Django Channels integration (real `WebsocketCommunicator` + consumer),
  multi-worker sharding (incl. real uvicorn subprocesses), and the
  chat-app end-to-end.  No mocks on the data path — real mmap, real Unix
  sockets, real ASGI.
- **Framework integration**: the generic `Broadcast` API works inside a
  Django Channels `AsyncWebsocketConsumer` (tested) even though mmcast
  ships no Django-specific helper — Django Channels support remains a
  v0.1 non-goal, but "it works with Django" is verified, not assumed.

[0.1.0]: https://github.com/sintimaski/mmbus/releases/tag/mmcast-v0.1.0
