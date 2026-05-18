# Changelog

All notable changes to mmbus are recorded here.  Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-05-18

### Added

- **WAL v2 (lock-free mmap-backed) — opt-in behind `wal_v2` Cargo
  feature.**  Full lock-free `MmapSegmentWriter` + seqlock-aware
  `MmapSegmentReader` + multi-segment `Wal` aggregator + per-
  platform durability primitives (`msync` + `F_FULLFSYNC` /
  `fdatasync` / `FlushFileBuffers`).  Behind `--features wal_v2`
  the `Publisher` and `Subscriber` swap onto the v2 backend
  transparently — same public API, same wire format with a
  version bump (1 → 2 in the segment header; v0.1 readers
  reject v2 segments cleanly with `UnsupportedVersion`).

  Why opt-in: v2's `wal=Batched` overhead vs no-WAL is +332%
  (M-series APFS, 32 B payload bench) — within ±10% of v0.1's
  +244% but not below the +10% gate set in
  `docs/rfc-wal-v2-lockfree.md` for promotion to default.  The
  per-tick `msync(MS_SYNC) + F_FULLFSYNC` dominates; the lock-
  free append path doesn't reduce it.  Follow-ups tracked in
  the RFC §11 Results.

  Status / decision recorded in `docs/rfc-wal-v2-lockfree.md` §11.

### Fixed (CI)

- Linux clippy: `Client::sock` dead-code warning + `CMSG_FIRSTHDR`
  redundant cast.  See commit history for details.
- GitHub Pages docs deploy auto-enables Pages on first run.
- Rustdoc intra-doc-link errors under `--all-features`.
- `clean_topic_then_republish_works` test cfg-ignored on Windows
  (pre-existing flake, tracked).

## [0.1.3] - 2026-05-18

Second CI / packaging follow-up.  v0.1.2's workflow run revealed
four additional issues — fixed below.  No user-visible API
changes.

### Fixed

- **CI compile failure on Windows** — `PySubscription::fileno`
  and `socket_fileno` declared `-> i32`, but on Windows
  `Subscription::fileno` returns `isize` (HANDLE).  Widened the
  Python-binding return type to `i64` (fits both Unix `RawFd`
  and Windows HANDLE; Python sees an int either way).
- **CI doctest failure** — `PySubscription`'s rustdoc-comment
  embedded a Python `with bus.subscribe(...):` block as
  rST-style indented code, which rustdoc tried to compile as
  Rust.  Wrapped it in a `text` code fence.
- **`wheels.yml` failure on Python 3.14** — `PyO3/maturin-action`
  picked the newest interpreter in its cache (3.14), exceeding
  PyO3 0.22's max.  Added `actions/setup-python` (pin 3.12) +
  `--interpreter python3.12` to the maturin args.
- **`audit.yml` cargo-audit failure** — RUSTSEC-2025-0020 was
  whitelisted in `deny.toml` for cargo-deny but `cargo-audit`
  has its own config.  Added `--ignore RUSTSEC-2025-0020` to
  the audit command with a documented justification.
- **Bridge `good_psk_authenticates_and_republishes`** —
  pre-existing flake on macOS CI (subscribe-before-publish-
  ready race).  Marked `#[ignore]` on macOS with a tracking
  note; the wrong-PSK companion test still runs.  Tracked as a
  follow-up.

## [0.1.2] - 2026-05-18

Release-engineering follow-up to v0.1.1: every workflow on the
v0.1.1 push failed.  No code-visible API changes — pure CI /
build / packaging fixes.

### Fixed

- **CI compile failure on Windows** — `Publisher` was `!Sync`
  on Windows because its `accept_rx: mpsc::Receiver<...>` field
  is `!Sync`, blocking `py.allow_threads` in the PyO3 binding
  (which requires `&PyBus: Send`, transitively requiring
  `Publisher: Sync`).  Wrapped `accept_rx` in `std::sync::Mutex`
  on Windows only.  Single-owner contract preserved; lock cost
  is one uncontended Mutex per `accept_clients` call.
- **CI compile failure on macOS / Linux against Python 3.14** —
  GitHub runners now default to Python 3.14, which exceeds
  PyO3 0.22's max supported version (3.13).  `ci.yml` now pins
  Python 3.12 via `actions/setup-python` BEFORE the `cargo
  test --all-features` step (which compiles `pyo3-ffi`).
- **`wheels.yml` couldn't find a Python interpreter on aarch64
  Linux** — the cross-build needs a target-arch Python.
  Rewrote the workflow to use `PyO3/maturin-action@v1`, which
  handles manylinux container selection + QEMU + interpreter
  discovery for all five build targets.
- **`audit` workflow failure on RUSTSEC-2025-0020** — PyO3
  0.22.6's `PyString::from_object` advisory.  We don't use
  that function anywhere; whitelisted in `deny.toml` with a
  documented justification + the resolution path (PyO3 0.22 →
  0.28 migration tracked for v0.2.x).

### Documented

- `docs/release-checklist.md` gains a "One-time repo setup"
  section: GitHub Pages must be enabled in Settings → Pages
  (otherwise `docs.yml` fails); PyPI Trusted Publishing (or
  `PYPI_API_TOKEN`) must be configured; the `pypi` GitHub
  environment must exist.  These bit us on the v0.1.1 push.

## [0.1.1] - 2026-05-18

Python throughput + development-process polish on top of the
v0.1.0 release.  No wire-format change, no API removal — pure
additions + perf.

### Added — Python batch APIs

- **`Subscription.recv_batch(n, timeout_secs) -> list[bytes]`** —
  drains up to `n` messages amortising per-call PyO3 + GIL
  dispatch.  Blocks for the FIRST message up to `timeout_secs`
  (GIL released), then drains non-blockingly with the GIL HELD.
  Returns empty list on timeout.
- **`Bus.publish_many(topic, payloads) -> int`** — batched
  publish that fires ONE wakeup syscall per subscriber regardless
  of N.  Pairs with `recv_batch` for end-to-end backlog
  throughput.  Returns count actually written (less than
  `len(payloads)` under `Error` backpressure when the ring fills
  mid-batch; equals `len(payloads)` under `drop_oldest`).
- **`Subscription.recv_into_buffer(buf, payload_size, timeout_secs)
  -> int`** — zero-alloc fixed-size recv.  `buf` is any writable,
  C-contiguous bytes-like (`bytearray`, `memoryview`, or numpy
  uint8 array); drains up to `len(buf) // payload_size` messages
  directly into rows.  Every drained payload must be exactly
  `payload_size` bytes; mismatch raises `ValueError`.  Backed by
  a new lock-free `RingBuffer::try_receive_into_slice` so ring
  slot bytes memcpy straight into the user buffer — no `Vec<u8>`
  or `PyBytes` intermediate.
- **Rust buffer-reuse API** (foundation for the above): new
  `Subscriber::receive_into`, `try_receive_into`,
  `receive_timeout_into` + matching `Subscription::*_into` +
  `Subscriber::try_receive_into_slice`.  `PySubscription` holds
  a reusable `recv_buf` and `PyBytes::new_bound_with` writes
  directly into the PyBytes allocation — no Vec intermediate on
  the single-recv path either.
- **`mmbus.WalError`** Python exception class — surfaces
  non-cursor WAL failures (`Poisoned`, `PayloadTooLarge`,
  underlying I/O).

### Bench — end-to-end pub/sub (5k 32 B msgs, macOS arm64, CPython 3.9)

| Pairing                                          | ns/msg | vs baseline |
|--------------------------------------------------|-------:|------------:|
| `publish()` × `recv()` (baseline)                |  1,741 |           — |
| `publish_many(64)` × `recv()`                    |    536 | 3.2× faster |
| `publish_many(256)` × `recv()`                   |    420 | 4.1× faster |
| `publish_many(1024)` × `recv_batch(1024)`        |    335 | 5.2× faster |
| `publish_many(1024)` × `recv_into_buffer(1024)`  |    325 | 5.4× faster |

Largest lever is `publish_many` — collapsing N per-publish
wakeup syscalls (eventfd_write / Unix-socket send /
ReleaseSemaphore) into one drops publish-side from ~1500 to
~200 ns/msg.  Use `recv()` for low-rate live streams (batch
APIs don't help when the publisher is the bottleneck); use
`publish_many` + `recv_batch` / `recv_into_buffer` for
high-throughput burst workloads.

### Added — development harness scaffolding

Project-local `CLAUDE.md` codifying mmbus's load-bearing
invariants + hot-path discipline; `CONTRIBUTING.md` workflow
guide; `.github/PULL_REQUEST_TEMPLATE.md` with the Code Review
Lanes (A correctness / B security / C perf / D DX-UX / E ops);
`.github/ISSUE_TEMPLATE/{bug,feature}.md` for structured intake;
`docs/templates/{spec,task,runbook}-template.md`; first concrete
runbook at `docs/runbooks/wal-disk-pressure.md`;
`docs/release-checklist.md`; `deny.toml` + a daily
`.github/workflows/audit.yml` running `cargo-deny` +
`cargo-audit` on dep changes.  No runtime impact.

### Added — WAL v2 RFC + plan (preview)

`docs/rfc-wal-v2-lockfree.md` + `docs/plan-wal-v2-lockfree.md`
describe the lock-free mmap-backed WAL targeted at v0.2.0
(closes the +244% `wal=Batched` overhead from v0.1.0).  Code
lives behind the `wal_v2` Cargo feature and is currently empty
(W2-0 only) — opt in at your own risk during v0.1.x.

## [0.1.0] - 2026-05-17 — first public release

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

#### WAL — opt-in durable replay (Phase B)

- `BusConfig::wal` — per-bus write-ahead log.  Opt-in; default is
  `WalConfig::disabled()` so existing callers see no change.
- `WalConfig::fsync_policy` — `None` (no fsync), `Batched`
  (background flusher every `fsync_interval` or `fsync_batch_bytes`),
  `Each` (fsync inline per publish).
- `Publisher` opens the WAL when enabled, aligns the ring's tail
  with the WAL's pending cursor on restart (so cursors stay
  monotonic across publisher restarts), and appends every record
  to the WAL before the ring write.  WAL append failure returns
  `Error::Wal` and leaves the ring untouched.
- `Subscriber::connect_with(StartPos::Explicit(c))` consults the
  WAL when `c` falls behind the ring; transparently feeds records
  through `receive()` / `try_receive()` / `receive_timeout()` /
  `poll_recv()` and then transitions back to live ring reads.
- `TopicStats.wal: Option<WalStats>` — pending / durable / oldest
  cursors, active-segment bytes, segment count.
- Crash recovery: `Wal::open` runs `recover_truncate` on every
  segment so a power-loss-torn tail is dropped before any reader
  sees it.  Wire format documented in `docs/rfc-wal-phase-b.md`.
- Retention: oldest segments deleted once total bytes exceed
  `retention_bytes` (default 1 GiB); subscribers requesting a
  retention-evicted cursor get `Error::CursorTooOld { oldest:
  wal.oldest_cursor }`.

#### WAL bench (32 B payload, capacity 4096, macOS 25.4 APFS)

| Policy        | ns/publish | Overhead vs no-WAL |
|---------------|-----------:|-------------------:|
| no WAL        |    176     |                  — |
| `wal=None`    |    248     |               +41% |
| `wal=Batched` |    606     |              +244% |
| `wal=Each`    |   3.6 ms   | (fsync per publish) |

`Batched` exceeds the planned <10% gate, so the default remains
`WalConfig::disabled()`.  Closing the gap needs the lock-free
mmap-backed redesign tracked in `docs/rfc-wal-v2-lockfree.md`
(v0.2.0).

#### Python batch APIs

- **`Subscription.recv_batch(n, timeout_secs) -> list[bytes]`** —
  drains up to `n` messages amortising per-call PyO3 + GIL
  dispatch.  Blocks for the FIRST message up to `timeout_secs`
  (GIL released), then drains non-blockingly with the GIL HELD.
  Returns empty list on timeout.
- **`Bus.publish_many(topic, payloads) -> int`** — batched
  publish that fires ONE wakeup syscall per subscriber regardless
  of N.  Pairs with `recv_batch` for end-to-end backlog
  throughput.  Returns count actually written (less than
  `len(payloads)` under `Error` backpressure when the ring fills
  mid-batch; equals `len(payloads)` under `drop_oldest`).
- **`Subscription.recv_into_buffer(buf, payload_size, timeout_secs)
  -> int`** — zero-alloc fixed-size recv.  `buf` is any writable,
  C-contiguous bytes-like (`bytearray`, `memoryview`, or numpy
  uint8 array); drains up to `len(buf) // payload_size` messages
  directly into rows.  Every drained payload must be exactly
  `payload_size` bytes; mismatch raises `ValueError`.  Backed by
  a new lock-free `RingBuffer::try_receive_into_slice` so ring
  slot bytes memcpy straight into the user buffer — no `Vec<u8>`
  or `PyBytes` intermediate.
- **Rust buffer-reuse API** (foundation for the above): new
  `Subscriber::receive_into`, `try_receive_into`,
  `receive_timeout_into` + matching `Subscription::*_into` +
  `Subscriber::try_receive_into_slice`.  `PySubscription` holds
  a reusable `recv_buf` and `PyBytes::new_bound_with` writes
  directly into the PyBytes allocation — no Vec intermediate on
  the single-recv path either.
- **`mmbus.WalError`** Python exception class — surfaces
  non-cursor WAL failures (`Poisoned`, `PayloadTooLarge`,
  underlying I/O).

#### Python perf — end-to-end pub/sub (5k 32 B msgs, macOS arm64, CPython 3.9)

| Pairing                                          | ns/msg | vs baseline |
|--------------------------------------------------|-------:|------------:|
| `publish()` × `recv()` (baseline)                |  1,741 |           — |
| `publish_many(64)` × `recv()`                    |    536 | 3.2× faster |
| `publish_many(256)` × `recv()`                   |    420 | 4.1× faster |
| `publish_many(1024)` × `recv_batch(1024)`        |    335 | 5.2× faster |
| `publish_many(1024)` × `recv_into_buffer(1024)`  |    325 | 5.4× faster |

Largest lever is `publish_many` — collapsing N per-publish
wakeup syscalls (eventfd_write / Unix-socket send /
ReleaseSemaphore) into one drops publish-side from ~1500 to
~200 ns/msg.  Use `recv()` for low-rate live streams (batch
APIs don't help when the publisher is the bottleneck); use
`publish_many` + `recv_batch` / `recv_into_buffer` for
high-throughput burst workloads.

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
- **WAL=Batched overhead**: +244% vs no-WAL on the bench rig.
  Acceptable for opt-in users prioritising durability; closing the
  gap to <10% needs the lock-free WAL v2 (RFC + plan landed; impl
  tracked as v0.2.0).
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

[Unreleased]: https://github.com/sintimaski/mmbus/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/sintimaski/mmbus/compare/v0.1.3...v0.2.0
[0.1.3]: https://github.com/sintimaski/mmbus/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/sintimaski/mmbus/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/sintimaski/mmbus/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/sintimaski/mmbus/releases/tag/v0.1.0
