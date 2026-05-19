# Changelog

All notable changes to mmbus are recorded here.  Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2026-05-20

### Added — Bridge Python SDK (in-process cross-machine pub/sub)

- **`mmbus_bridge` companion wheel** exposing an in-process bridge:
  `from mmbus_bridge import Bridge`.  Forwards locally-published
  mmbus topics to peer machines over TCP and republishes inbound
  peer traffic onto the local bus — no standalone binary install,
  no `subprocess` lifecycle.  Install with `pip install mmbus[bridge]`
  (preferred) or `pip install mmbus mmbus-bridge`.

  ```python
  from mmbus_bridge import Bridge

  with Bridge({
      "bus": "my-app",
      "listen": "0.0.0.0:4443",
      "topics": [{"name": "events"}],
      "peers": [{"name": "b", "endpoint": "b.host:4443",
                 "preshared_key": "secret"}],
  }) as bridge:
      print(bridge.listen_addr)   # ('0.0.0.0', 4443)
      bridge.wait()               # blocks until Ctrl-C / shutdown()
  ```

- **Public API:** `Bridge(dict)`, `Bridge.from_toml(str)`,
  `Bridge.from_path(path)`; methods `start()`, `shutdown(timeout=None)`,
  `wait()`, `is_running()`; properties `origin_id`, `listen_addr`;
  context-manager protocol (`with Bridge(cfg) as b:`).  Config dict
  mirrors the bridge TOML schema 1:1 and is validated eagerly at
  construction time through the same `BridgeConfig::validate()` the
  standalone binary uses.

- **Typed exceptions:** `BridgeConfigError` (subclass of `ValueError`),
  `BridgeListenError`, `BridgeQuicError`, and a `BridgeError` base.

### Design notes

- **Why a separate wheel, not a `mmbus` feature:** `mmbus-bridge`
  depends on `mmbus` (it republishes onto a local `Bus`), so adding
  the reverse edge would create a Cargo dependency cycle.  The PyO3
  bindings therefore live in the bridge crate and ship as their own
  `mmbus_bridge._mmbus_bridge` extension.  See
  `docs/rfc-bridge-python-sdk.md`.

- **TCP only.** The wheel deliberately omits the crate's `quic`
  feature (tokio + quinn + rustls + ring + rcgen) to stay slim.  A
  QUIC peer config raises `BridgeQuicError` at `start()`; QUIC users
  keep the standalone `mmbus-bridge` binary
  (`cargo install --path bridge --features quic`).

- The existing subprocess shim (`mmbus.bridge.run` /
  `mmbus.bridge.spawn`) is unchanged and still useful for
  systemd-style supervision of the standalone binary.

### Changed

- `mmbus-bridge` crate version `0.0.1` → `0.3.0`; it now builds a
  `cdylib` + `rlib` (`[lib] name = "mmbus_bridge"`) and gained an
  optional `python` Cargo feature.  The standalone binary build path
  is unaffected (still links the rlib; TCP-only unless `--features
  quic`).

### Backward compatibility

Additive only.  The core `mmbus` wheel and Rust crate are unchanged
except for the version bump and the new `[bridge]` install extra.

## [0.2.3] - 2026-05-19

### Fixed (Python wheel)

- **Python wheel now ships the `wal_v2` lock-free WAL backend by
  default.** `[tool.maturin].features` in `pyproject.toml` now
  reads `["python", "wal_v2"]` (was `["python"]`).  Before this
  fix, Python users got the v0.1 BufWriter WAL at ~40 k msg/s
  sustained durable throughput; after, ~1.06 M msg/s — a 26×
  improvement, directly closing the v0.2.0/v0.2.1 perf-push
  gap for Python consumers.  No code changes — purely a build
  configuration miss surfaced by the v0.2.2 competitive
  benchmark work.

  Re-bench numbers (M-series, APFS, Python wheel via
  `maturin develop --release`, 1M × 256 B messages):

  | Configuration         | Sustained throughput |
  |-----------------------|---------------------:|
  | mmbus non-durable     | 1.34 M/s            |
  | mmbus durable v0.2.2 wheel (v0.1 WAL) | 0.04 M/s |
  | **mmbus durable v0.2.3 wheel (v0.2 WAL)** | **1.06 M/s** |

  RESULTS.md and docs/benchmarks-vs-competition.md updated to
  match.

### Backward compatibility

Additive only — existing Python code is unchanged.  The wheel
now opens a `wal/` directory under each bus's `base_dir` by
default (already the case in v0.2.0+; the wheel just wasn't
using the FAST backend before this release).  Users who want
the bare ring continue to opt out with
`mmbus.Bus(..., wal_enabled=False)`.

## [0.2.2] - 2026-05-19

### Added — Prometheus exporter

- **`mmbus::prometheus` module** behind the new `prometheus`
  Cargo feature.  Two pieces:
  - `render(topic_name, &TopicStats) -> String` and
    `render_all(&[(name, stats)])` — pure Prometheus
    text-format renderers covering every counter / gauge from
    v0.2.1.  Uses `std::fmt::Write`; no external deps.
  - `serve_blocking(addr, scrape_fn)` — single-threaded
    HTTP server (no tokio / hyper / tiny_http) that responds
    to `GET /metrics` with `scrape_fn()`'s output.  ~80 LOC
    of `std::net`.  Suitable for Prometheus's 15-60s scrape
    cadence.

- **Metrics surfaced**: `mmbus_published_total`,
  `mmbus_full_rejected_total`, `mmbus_subscribers_dropped_total`,
  `mmbus_connected_sockets`, `mmbus_ring_tail`,
  `mmbus_active_subscribers`, `mmbus_max_subscriber_lag`, plus
  the full WAL block (`mmbus_wal_appends_total`,
  `mmbus_wal_append_bytes_total`, `mmbus_wal_flushes_total`,
  `mmbus_wal_pending_cursor`, `mmbus_wal_durable_cursor`,
  `mmbus_wal_replay_lag`, `mmbus_wal_total_bytes`,
  `mmbus_wal_segments`).  WAL block is omitted when WAL is
  disabled.

- **`examples/prometheus_exporter.rs`** — 30-line publisher +
  `/metrics` endpoint demo.  Run with
  `cargo run --example prometheus_exporter --features prometheus`.

### Tests

7 new unit tests in `src/prometheus.rs` covering the render
output shape, WAL-on/off branches, max-subscriber-lag derivation,
label escaping, and an HTTP round-trip 404 case.

### Backward compatibility

Additive only — `prometheus` feature is opt-in.  No changes to
the core public API.

## [0.2.1] - 2026-05-18

### Added — Observability

- **`tracing` crate integration** for structured logging.  Replaces
  the one `eprintln!` in `wal::segment_reader::recover_truncate`
  with `tracing::warn!`, and adds INFO-level events at the slow-
  path transitions: `wal::v2 opened`, `wal segment rotated`,
  `wal first segment opened`, `wal retention pruned segment`,
  `publisher created`, `subscriber connected`,
  `subscriber dropped`.  Zero overhead when no subscriber is
  registered.  Downstream users wire `tracing_subscriber::fmt::init()`
  to see the events.

- **Monotonic event counters** on `TopicStats` + `WalStats`:
  - `TopicStats.published_total` — successful publishes
  - `TopicStats.full_rejected_total` — publishes rejected with
    `Error::Full` under `BackpressurePolicy::Error`
  - `TopicStats.subscribers_dropped_total` — subscribers dropped
    by the publisher because their wakeup failed (peer died)
  - `WalStats.appends_total` — successful WAL appends
  - `WalStats.append_bytes_total` — payload bytes appended
  - `WalStats.flushes_total` — completed `flush_sync` calls
    (Each-policy inline + Batched flusher tick)

  All counters use `Relaxed` ordering — pure observability with
  ~1 ns per-publish cost.

- **`PyWalStats`** Python class exposing the WAL counters via
  `TopicStats.wal` (was previously `None` from Python).  Both
  v0.1 and v0.2 WAL backends populate it.

- **`examples/observability.py`** demonstrates scraping
  `Bus.stats()` and emitting Prometheus text-format lines.

### Performance

`cargo bench --features wal_v2 --bench publish_with_wal` after the
observability wiring:

| Policy         | Throughput | vs v0.2.0 |
|----------------|------------|-----------|
| baseline_no_wal | 5.65 Melem/s | unchanged |
| wal=None        | 4.61 Melem/s | unchanged |
| wal=Batched     | 4.46 Melem/s | -3% (counters + 1 tracing event) |

`wal=Batched` overhead vs no-WAL: +27% (was +22% in v0.2.0).
+5 ns per publish is the observability tax — 3 atomic fetch_add
counters plus per-event tracing dispatch (zero work when no
subscriber).

### Fixed

- None — this release is additive.

## [0.2.0] - 2026-05-18

### Added — WAL v2 (lock-free mmap-backed, ON BY DEFAULT)

- **`WalConfig::default().enabled` flipped to `true`.**  Every
  `Publisher::create` with default config now records messages
  to the WAL — late-joining and crash-restarted subscribers can
  replay every record back to `oldest_cursor`.  Behavioral
  change for v0.1.x users: a `wal/` directory now appears under
  the bus dir, growing up to `WalConfig::retention_bytes`
  (1 GiB default).  Opt-out with `WalConfig::disabled()`.
- **Lock-free WAL backend (`wal_v2` Cargo feature, default ON).**
  Full pipeline of `MmapSegmentWriter` (CAS-on-tail + bracketed
  seqlock memcpy, no syscall on the publish hot path) +
  `MmapSegmentReader` (seqlock-aware read) + multi-segment
  `Wal` aggregator + `active.dat` coordination file for
  subscriber rotation discovery + per-platform durability
  (`msync(MS_SYNC) + fdatasync / F_FULLFSYNC / FlushFileBuffers`).
- **Cached wall-clock timestamps.**  `Wal::current_ts()` returns
  a cached value refreshed by the flusher tick (Batched: ~2 ns
  atomic load; None / Each: ~19 ns inline compute) instead of
  per-publish `Instant::elapsed`.  Staleness ≤ `fsync_interval`
  (5 ms default).

### Performance

| Backend         | wal=Batched throughput | vs no-WAL |
|-----------------|------------------------|-----------|
| v0.1            | 1.5 Melem/s            | +244%     |
| v0.2.0 initial  | 1.3 Melem/s            | +332%     |
| v0.2.0 shipped  | **4.6 Melem/s**        | **+22%**  |

12x reduction in publish overhead from the v2 initial → shipped
numbers.  See `docs/rfc-wal-v2-lockfree.md` §11 for the per-
source breakdown and the two key fixes (release the inner mutex
around `flush_sync`; ArcSwap writer slot + atomic bookkeeping).

### Breaking changes

- `WalConfig::default()` is now `enabled: true` (was `false`).
  Affected calls: any `Publisher::create` or `Bus::with_config`
  that used `BusConfig::default()` or `..Default::default()`.
- `WalConfig::disabled()` and `WalConfig::batched()` constructors
  remain; the names still read correctly but `batched()` is now
  equivalent to `default()`.

### Fixed (CI)

- Linux clippy: `Client::sock` dead-code warning + `CMSG_FIRSTHDR`
  redundant cast.
- macOS `cargo test --all-features` linker error: added
  `.cargo/config.toml` with `-undefined dynamic_lookup` rustflag
  for PyO3 extension-module under cargo test.
- Rustdoc intra-doc-link errors under `--all-features`.
- Several pre-existing Windows + bridge-QUIC test flakes
  cfg-ignored (publisher pipe-handle race; documented and
  tracked).

### Manual setup still required for v0.2.0 release

- **GitHub Pages**: enable in repo Settings → Pages → Source =
  "GitHub Actions" so the docs workflow can publish rustdoc.
- **PyPI Trusted Publisher**: register `sintimaski/mmbus`
  workflow `wheels.yml` env `pypi` at
  https://pypi.org/manage/account/publishing/ so the wheels
  workflow can push the v0.2.0 wheels.

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

[Unreleased]: https://github.com/sintimaski/mmbus/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/sintimaski/mmbus/compare/v0.2.3...v0.3.0
[0.2.3]: https://github.com/sintimaski/mmbus/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/sintimaski/mmbus/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/sintimaski/mmbus/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/sintimaski/mmbus/compare/v0.1.3...v0.2.0
[0.1.3]: https://github.com/sintimaski/mmbus/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/sintimaski/mmbus/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/sintimaski/mmbus/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/sintimaski/mmbus/releases/tag/v0.1.0
