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
  - `broadcast.presence(channel, *, member_id, ttl_secs=15.0, heartbeat_secs=5.0)`
    — async context manager + iterator over `PresenceChange` events;
    backed by a separate `_presence:<channel>` mmbus topic with TTL
    heartbeat eviction.
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
    frontend.
  - `examples/benchmark/` — docker-compose harness for the side-by-
    side comparison vs `encode/broadcaster` + Redis (loadgen, both
    Dockerfiles, results JSON).

### Notes

- mmbus dependency pinned to `>=0.5,<0.6` for the pre-1.0 floor-and-
  ceiling rule.  Will widen to `>=1,<2` after mmbus's 1.0 freeze.
- Single-publisher mode is the default (one process owns a topic);
  sharded mode kicks in when `worker_id` + `peers` are supplied.
- `replay_last=N` is in-ring only (mmbus's
  `subscribe_with_history`).  Durable-WAL replay is planned for v0.2
  along with a per-subscriber replay buffer.
- Tests: 35 cases across smoke, broadcast core, backpressure, replay,
  presence, FastAPI helpers, and the chat-app end-to-end.  No mocks
  on the data path — real mmap, real Unix sockets, real ASGI.

[0.1.0]: https://github.com/sintimaski/mmbus/releases/tag/mmcast-v0.1.0
