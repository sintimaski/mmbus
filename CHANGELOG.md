# Changelog

All notable changes to mmbus are recorded here.  Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - first public release

### Added

#### Core API

- `Bus(name)` — named pub/sub namespace with topic routing.
- `bus.publish(topic, bytes)`.
- `bus.subscribe(topic)` → iterator + context-manager `Subscription`.
- `bus.subscribe_with_history(topic, n_messages_back)` — best-effort
  in-ring replay; capped at ring capacity.
- `bus.subscribe_from(topic, cursor)` — explicit cursor; raises
  `CursorTooOldError` if older than the oldest in-ring slot.
- `bus.subscribe_async(topic)` → `AsyncSubscription` using
  `asyncio.loop.add_reader` — zero thread-pool usage for `recv`.
- `bus.subscribe_anyio(topic)` → `AnyioSubscription` using
  `anyio.to_thread` — cross-backend (trio, asyncio, curio).
- `bus.wait_for_subscribers(topic, n)` — block until *n* subscribers
  are connected.
- `bus.stats(topic) -> TopicStats` — ring tail, per-cursor lags,
  connected sockets.
- `bus.clean_topic(topic)` — dev/test helper that wipes on-disk state;
  refuses if a publisher is active.
- `BackpressurePolicy::{Error, DropOldest}` selectable per-bus.
- Typed exceptions: `BusFullError`, `MessageTooLargeError`,
  `ConnectTimeoutError`, `TooManySubscribersError`,
  `AlreadyPublishingError`, `CursorTooOldError`.

#### Reliability

- **Crash-safe publisher restart** via in-header `generation` counter
  (wire format v3 → v4).  Existing subscribers see the bump on their
  next wakeup and terminate cleanly with `UnexpectedEof` instead of
  reading from the logically-reset ring.  No `ftruncate` on restart,
  so a stale subscriber's mmap can never SIGBUS.
- **Per-slot seqlock** (wire format v4) for correct `DropOldest`
  behaviour under sustained contention.  Subscribers detect torn
  reads and overwritten slots via the seq field and skip forward
  instead of returning garbage.

#### Async / framework integration

- `AsyncSubscription` uses `loop.add_reader` on the wakeup fd (eventfd
  on Linux, Unix socket on macOS).  Disconnect detection via a second
  `add_reader` on the handshake socket on Linux (POLLHUP).
- `AnyioSubscription` adds trio + asyncio + curio compatibility via
  `anyio.to_thread` (one worker thread per recv; the tradeoff vs.
  `AsyncSubscription`'s zero-thread asyncio path is documented).
- `examples/fastapi_broadcast.py` — single-file FastAPI WebSocket
  fan-out demo (each WS connection owns its own mmbus subscription;
  SPMC cursor table does the fan-out).

#### Platforms

- Linux (x86_64, aarch64) — eventfd wakeup, SCM_RIGHTS handshake.
- macOS (x86_64, arm64) — Unix-socket byte wakeup.
- Windows — *not yet* (RFC at `docs/rfc-windows.md`).

#### Distribution

- Pre-built wheels via `maturin` (`pyproject.toml`).
- Python ≥ 3.8.
- CI workflows: `ci.yml` (test + clippy on Linux + macOS),
  `wheels.yml` (build matrix on tag push), `docs.yml` (rustdoc to
  GitHub Pages), `fuzz.yml` (cargo-fuzz smoke on relevant PRs).
- Docker dev environment for Linux testing from macOS.

#### Tooling

- `benches/ring.rs`, `benches/e2e.rs` — Criterion microbenches.
- `tests/stress.rs` — opt-in stress tests (`--ignored`):
  fan-out 100k × 4 subs, DropOldest 50k × 3, 50 rapid restart cycles.
- `tests/crash_recovery.rs`, `tests/replay.rs`, `tests/clean_topic.rs`.
- `fuzz/` — cargo-fuzz harness for the `RingBuffer` API
  (`ring_publish_receive` target); validated locally with ~280k
  iterations and zero crashes.

### Documentation

- `README.md` with quickstart, perf table from local benches,
  competitive comparison, full API table.
- `docs/architecture.md` — layer diagram, data path, lock-free
  invariants.
- `docs/roadmap.md` — phased plan with completion state.
- `docs/research.md` — competitive landscape, market signals.
- `docs/rfc-wal-replay.md` — Phase A shipped, Phase B (durable WAL)
  deferred to a separate project.
- `docs/rfc-multi-machine.md` — design for `mmbus-bridge` relay
  (post-v1 separate sub-project).
- `docs/rfc-windows.md` — design for Windows port.

### Known gaps

- **Windows support**: not yet (RFC ready; ~1 focused week of work).
- **Durable replay**: only in-ring history today (Phase A); a WAL-
  backed Phase B is designed but unimplemented.
- **`AnyioSubscription` perf**: uses a worker thread per recv; for
  asyncio-only workloads `AsyncSubscription` is strictly cheaper.
- **macOS `kqueue` wakeup**: the macOS path uses a Unix-socket byte
  per message.  `kqueue` is not a substitute (it's a multiplexer, not
  a cross-process primitive); a true equivalent of Linux's `eventfd`
  doesn't exist on macOS.  Performance gap vs. Linux is small in
  practice (~720 ns e2e on both).
- **Buffer protocol / `memoryview`**: `recv()` copies the payload out
  of the ring into a Python `bytes` object.  A zero-copy `memoryview`
  path would require pinning slots against publisher overwrite — a
  significant ring redesign; deferred.

### Breaking changes from pre-release

This is the first public release.  Wire format starts at v4.

[Unreleased]: https://OWNER/mmbus/compare/v0.1.0...HEAD
[0.1.0]: https://OWNER/mmbus/releases/tag/v0.1.0
