# Roadmap

## Current state (as of v0.5.1)

Shipped through v0.5.1: ring core, PyO3 bindings, async (asyncio + anyio),
WAL Phase A (replay) + Phase B (durable mmap WAL v2), Prometheus exporter,
structured logging (`tracing` in Rust + `init_logging` in Python),
`mmbus-bridge` in-process Python SDK (TCP, v0.3.x), single-lookup publish
hot path, zero-copy `publish_many`, `Bus.topic() → TopicPublisher`,
zero-copy `recv_into` (allocation-free receive), wakeup coalescing
(wire v5), and Python 3.9–3.13 wheels.  Published to PyPI.

Sibling library: **`mmbus-cast`** (`crates/mmcast/`) — ASGI WebSocket
broadcast on top of mmbus (v0.1, pending PyPI publish).

Active plans:
- Windows **runtime** support (compile-checked today; tests hang on the
  blocking producer lock — Phase 7 below)
- v1.0 API freeze + release
- True zero-copy receive: expose slot memory as `memoryview` (Phase 2)

---

## Phase 1 — Rust Core ✓

Goal: a working lock-free ring buffer over mmap with Unix socket signaling.

- [x] mmap file creation, header layout, magic/version validation
- [x] Lock-free ring buffer: atomic head/tail, slot format, wrap-around
- [x] Single-producer single-consumer (SPSC) first, then SPMC
- [x] Unix domain socket wakeup (1 byte per message, macOS)
- [x] `eventfd` on Linux (`EFD_SEMAPHORE`) — Unix socket kept as
      handshake / disconnect signal
- [x] `flock` producer lock (per-process `HashSet` for macOS BSD semantics)
- [x] Rust benchmarks: `cargo bench --bench ring && cargo bench --bench e2e`
- [x] SPMC fan-out: per-subscriber cursor table in mmap header
- [x] Crash-safe publisher restart: header `generation` counter (v4 wire
      format).  Subscribers detect the bump on next wakeup and return
      `UnexpectedEof` instead of reading from the logically-reset ring.
      Tests in `tests/crash_recovery.rs`.
- [x] Per-slot seqlock for `DropOldest` torn-read safety (v4): publisher
      brackets the payload write with two Release stores of `seq` (the
      second tagged with a high `WRITING` bit), subscribers retry on
      mismatch.  Validated by `fuzz/fuzz_targets/ring_concurrent.rs`.

Throughput on M-series macOS: ~36M ring ops/s (32 B), ~1.4M msg/s e2e.

---

## Phase 2 — PyO3 Bindings ✓

Goal: expose Rust core to Python with correct GIL semantics.

- [x] `Bus`, `Subscription`, `TopicStats` as PyO3 classes
- [x] `bus.publish(bytes)` — write bytes into ring
- [x] `sub.recv() -> bytes` — copy out of ring into Python `bytes`
- [x] `sub.recv_timeout(secs)` — blocking with timeout, GIL released
- [x] `py.allow_threads()` around all blocking waits
- [x] `maturin develop` workflow; smoke test in `python/smoke_test.py`
- [x] Allocation-free receive: `recv_into(buf)` / `try_recv_into(buf)`
      write the payload straight into a caller buffer (bytearray /
      writable memoryview / numpy uint8), via `pyo3::buffer::PyBuffer`
      (v0.5.0).  Still one memcpy out of the ring, but no per-message
      `PyBytes` alloc.
- [ ] True zero-copy: expose slot memory as a borrowed `memoryview`
      (no copy at all).  Requires pinning the slot against publisher
      overwrite for the borrow's lifetime — the open design problem.
      Meaningful mainly for large arrays (ML / video).
- [x] Publish wheel to TestPyPI / PyPI (live; see Phase 6)

---

## Phase 3 — Python API ✓

- [x] Topic routing: named channels within a `Bus`
- [x] Blocking iterator: `for msg in sub:`
- [x] Context manager: `with bus.subscribe("x") as sub:`
- [x] Typed exceptions: `BusFullError`, `MessageTooLargeError`,
      `ConnectTimeoutError`, `TooManySubscribersError`, `AlreadyPublishingError`
- [x] Type annotations on public API
- [x] Python 3.9+ support (3.8 dropped — EOL; no wheel shipped)
- [~] Serialization helpers — core keeps the raw-bytes contract by
      design; codec sugar lives in siblings (`mmbus-cast` ships
      `publish_json`).  numpy/msgpack helpers can land in a sibling if
      demand appears; not a core concern.

---

## Phase 4 — Async Support ✓

- [x] `async for msg in sub:` — non-blocking async iterator
- [x] `await sub.recv_timeout(timeout)` — awaitable receive
- [x] `asyncio` integration via `loop.add_reader` (eventfd Linux,
      socket macOS); zero thread pool usage for `recv`
- [x] Disconnect detection: second `add_reader` on handshake socket on Linux
      so publisher `POLLHUP` cancels in-flight `await sub.recv()`
- [x] `anyio` / `trio` compatibility via `AnyioSubscription` (one
      worker thread per recv; trade-off vs. zero-thread asyncio path)
- [x] FastAPI WebSocket-broadcast example (`examples/fastapi_broadcast.py`)

---

## Phase 4.5 — Replay (Phase A of rfc-wal-replay.md) ✓

- [x] `Bus::subscribe_with_history(topic, n_messages_back)` — best-
      effort replay of recent in-ring history
- [x] `Bus::subscribe_from(topic, cursor)` — explicit cursor; returns
      `Error::CursorTooOld` if older than the oldest in-ring slot
- [x] New `CursorTooOldError` Python exception class
- [x] `receive()` / `receive_timeout()` now try the ring before
      blocking on a wakeup — also fixes the "subscriber connects but
      the publisher's accept_clients hasn't run yet, so no wakeup
      arrives" hang
- [x] Phase B durable WAL — shipped: lock-free mmap-backed WAL v2
      (`src/wal/v2/`, `wal_v2` feature), append-before-publish ordering,
      crash recovery + replay.  See `docs/rfc-wal-phase-b.md`.

---

## Phase 5 — Hardening & Observability

- [x] Backpressure: `BackpressurePolicy::Error` vs `DropOldest`
- [x] `Bus.stats(topic)` — `tail`, `active_subscribers`, per-cursor `lags`,
      `connected_sockets`
- [x] `subscription.lag` / `.cursor` exposed for slow-consumer detection
- [x] `Bus.slow_subscribers(topic, threshold)` — `[(cursor_idx, lag), ...]`
      for laggards; intended to be called from a periodic monitor thread
      and emit warnings / metrics when non-empty
- [x] Prometheus metrics export (optional `prometheus` feature;
      `src/prometheus.rs` + `examples/prometheus_exporter.rs`)
- [x] Structured logging — `tracing` events in Rust (`logging` feature,
      `src/logging.rs`) + `mmbus.init_logging()` in Python (v0.5.0)
- [x] Fuzz testing of ring buffer under concurrent access — two
      cargo-fuzz targets (`ring_publish_receive` for API-shape coverage,
      `ring_concurrent` for the publisher×subscriber seqlock race that
      surfaced the v4 WRITING-bit fix); both run in `.github/workflows/fuzz.yml`
- [x] Stress tests: `cargo test --release --test stress -- --ignored`
      (fan-out 100k×4, drop-oldest 50k×3, 50× rapid restart cycles)
- [ ] ~~macOS: switch wakeup to `kqueue` for efficiency~~ — *not viable as
      originally framed.* `kqueue` is the event multiplexer (asyncio uses
      it already), not a cross-process wakeup primitive like `eventfd`.
      macOS has no clean eventfd equivalent (`EVFILT_USER` is per-kqueue,
      not transferable via SCM_RIGHTS; Mach ports are a major rewrite for
      marginal gain). Current Unix-socket path is ~720 ns e2e — close
      enough to the Linux eventfd path (~similar order) that the
      complexity isn't justified.

---

## Phase 6 — Distribution & Documentation

- [x] README with quickstart + perf table + competitive comparison
- [x] Cross-process examples (`examples/pub.py`, `examples/sub.py`)
- [x] CI: `cargo test` + `clippy` on Linux + macOS (`.github/workflows/ci.yml`)
- [x] Wheel build matrix: `linux/macos × x86_64/aarch64` (`wheels.yml`)
- [x] Docker dev environment for Linux testing from macOS
- [x] PyPI publish via `wheels.yml` on `v*` tag (live since v0.1.x;
      latest v0.5.1 ships Python 3.9–3.13 wheels for linux/macos +
      Windows)
- [x] API reference: rustdoc published to GitHub Pages (`docs.yml`)
- [ ] API reference: mkdocs site for the Python API (rustdoc done)
- [~] Use-case mini-guides — runnable examples shipped
      (`examples/fastapi_broadcast.py`, `np_pipeline.py`,
      `observability.py`); prose guides still to write

---

## Phase 7 — Windows Support

Goal: extend to Windows 10 1803+ without breaking Linux/macOS.

- [ ] Named pipe as Unix socket fallback
- [ ] `CreateFileMapping` / `MapViewOfFile` as mmap fallback (or use Rust's
  `memmap2` which abstracts this)
- [ ] CI: add `windows-latest` runner
- [ ] Test suite runs clean on Windows

---

## Future Directions (post-v1)

These are not commitments — ideas worth revisiting after v1 ships:

- **Multi-machine bridge**: a small relay process that federates local mmap
  buses over QUIC/WireGuard — turns the library into a distributed system
  without changing the local API
- **Persistence**: optional WAL-style replay for late-joining subscribers
  (like Kafka's offset model, but local and file-backed)
- **Language bindings**: the Rust core could expose bindings for Node.js
  (napi-rs) or Go (cgo) using the same mmap bus files — polyglot IPC
- **Named bus discovery**: a small local registry (SQLite) so processes can
  discover bus names without hardcoding paths

---

## Open Questions

1. **Name** — `mmbus` is a working title. Candidates: `zerobus`, `ringbus`,
   `axon`, `synapse`, `flashbus`. Needs PyPI availability check.
2. **Slot size policy** — fixed max slot size at bus creation, or variable?
   Variable is more flexible but complicates the ring buffer layout.
3. **Multi-producer** — SPMC (one writer, many readers) covers most use cases.
   MPMC (many writers) is harder to get right atomically. Ship SPMC first.
4. **Message TTL** — if a slow subscriber never reads, messages accumulate.
   Options: drop oldest, block producer, evict slow subscriber.
5. **PyPI name** — check availability before committing to final name.
