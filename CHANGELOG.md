# Changelog

All notable changes to mmbus are recorded here.  Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

#### WAL Phase B (opt-in durability)

- `BusConfig::wal` — per-bus write-ahead log.  Opt-in (default
  `WalConfig::disabled()`); enables `subscribe_from(cursor)` to
  replay arbitrarily-old cursors from on-disk segments instead of
  only the in-ring history.
- `WalConfig::fsync_policy` — `None` (no fsync), `Batched` (background
  flusher every `fsync_interval` or `fsync_batch_bytes`; default),
  `Each` (fsync inline per publish).
- `Publisher` opens the WAL when enabled, aligns the ring's tail
  with the WAL's pending cursor on restart, and appends every
  record to the WAL before the ring write.  A failed WAL append
  returns `Error::Wal` and the ring stays untouched.
- `Subscriber::connect_with(StartPos::Explicit(c))` consults the
  WAL when `c` falls behind the ring; transparently feeds records
  through the existing `receive()` / `try_receive()` /
  `receive_timeout()` / `poll_recv()` paths and then transitions
  back to live ring reads.
- `TopicStats.wal: Option<WalStats>` exposes pending / durable /
  oldest cursors, active-segment bytes, and segment count.
- Crash recovery: every `Wal::open` runs `recover_truncate` on
  every segment so a power-loss-torn tail is dropped before any
  reader sees it.
- Retention: oldest segments are deleted once total bytes exceed
  `retention_bytes` (default 1 GiB); subscribers asking for a
  retention-evicted cursor get `Error::CursorTooOld { oldest:
  wal.oldest_cursor }`.

#### Acceptance + perf

- `tests/wal_acceptance.rs` — 7 RFC §15 scenarios across the
  three fsync policies (crash recovery, cursor monotonicity,
  retention semantics, durable-cursor invariants per policy,
  WAL→ring handoff under live publishing).
- `benches/publish_with_wal.rs` — Criterion bench comparing
  publish throughput across baseline / `None` / `Batched` / `Each`.

### Bench results (32 B payload, capacity 4096, macOS 25.4 APFS)

| Policy        | ns/publish (W1-f) | ns/publish (W2 opt) | Overhead vs baseline |
|---------------|------------------:|--------------------:|---------------------:|
| no WAL        |               185 |                 185 |                    — |
| `wal=None`    |               275 |                 272 |                 +47% |
| `wal=Batched` |               767 |                 656 |                +254% |
| `wal=Each`    |         3,700,000 |           3,800,000 | catastrophic (fsync) |

W2 optimisation pass: (a) flusher thread's multi-ms `sync_data` now
runs OUTSIDE the WAL mutex — only the BufWriter flush + fd clone are
under the lock; (b) per-publish `SystemTime::now()` replaced with a
cached wall-clock offset + cheap `Instant::now()`.  Net: Batched
overhead drops 315% → 254% (-14%); other policies are unchanged
within noise.

Still well above the planned <10% gate, so the default stays at
`WalConfig::disabled()`.  Closing the gap further likely needs a
lock-free SPSC handoff between publisher and flusher (a v0.2.x item).

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
- `Bus(name, backpressure="error" | "drop_oldest")` — `"error"`
  (default) raises `BusFullError` when the ring is saturated;
  `"drop_oldest"` silently overwrites the oldest unread slot and the
  subscriber detects the skip via the per-slot seqlock.
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
- **`drop_oldest` recv-side skip signal**: subscribers on a
  `backpressure="drop_oldest"` bus do not currently get a *count* of
  how many messages they skipped; the cursor jump is silent.  The
  next-write seq is detectable at the slot level (the ring code uses
  it internally) but isn't surfaced to Python.  Planned follow-up.
- **macOS `kqueue` wakeup**: the macOS path uses a Unix-socket byte
  per message.  `kqueue` is not a substitute (it's a multiplexer, not
  a cross-process primitive); a true equivalent of Linux's `eventfd`
  doesn't exist on macOS.  Performance gap vs. Linux is small in
  practice (~720 ns e2e on both).
- **Buffer protocol / `memoryview`**: `recv()` copies the payload out
  of the ring into a Python `bytes` object.  A zero-copy `memoryview`
  path would require pinning slots against publisher overwrite — a
  significant ring redesign; deferred.  This copy + lack of message
  batching is the main reason `pyzmq` outperforms mmbus 3–12× for
  small-payload Python pub/sub today (see README "Performance" §3).
  Where mmbus already wins is at the Rust API level (~40M ring ops/s,
  ~25 ns roundtrip) and operationally (no broker / single `pip
  install`).

### Breaking changes from pre-release

This is the first public release.  Wire format starts at v4.

[Unreleased]: https://github.com/OWNER/mmbus/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/OWNER/mmbus/releases/tag/v0.1.0
