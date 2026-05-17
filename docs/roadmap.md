# Roadmap

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
- [ ] Crash recovery: dirty-flag protocol, header revalidation on reconnect

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
- [ ] Buffer protocol: expose slot memory as `memoryview` (deferred — only
      meaningful for large arrays; pinned slots would block ring overwrites.
      Revisit if ML / video use cases emerge.)
- [ ] Publish wheel to TestPyPI / PyPI (wheels.yml ready; needs `v0.1.0` tag)

---

## Phase 3 — Python API ✓

- [x] Topic routing: named channels within a `Bus`
- [x] Blocking iterator: `for msg in sub:`
- [x] Context manager: `with bus.subscribe("x") as sub:`
- [x] Typed exceptions: `BusFullError`, `MessageTooLargeError`,
      `ConnectTimeoutError`, `TooManySubscribersError`, `AlreadyPublishingError`
- [x] Type annotations on public API
- [x] Python 3.8+ support
- [ ] Serialization helpers (numpy / msgpack / pickle) — out of scope for v0.1;
      raw bytes is the documented contract

---

## Phase 4 — Async Support ✓

- [x] `async for msg in sub:` — non-blocking async iterator
- [x] `await sub.recv_timeout(timeout)` — awaitable receive
- [x] `asyncio` integration via `loop.add_reader` (eventfd Linux,
      socket macOS); zero thread pool usage for `recv`
- [x] Disconnect detection: second `add_reader` on handshake socket on Linux
      so publisher `POLLHUP` cancels in-flight `await sub.recv()`
- [ ] `anyio` / `trio` compatibility layer
- [ ] FastAPI WebSocket-broadcast example

---

## Phase 5 — Hardening & Observability

- [x] Backpressure: `BackpressurePolicy::Error` vs `DropOldest`
- [x] `Bus.stats(topic)` — `tail`, `active_subscribers`, per-cursor `lags`,
      `connected_sockets`
- [x] `subscription.lag` / `.cursor` exposed for slow-consumer detection
- [ ] Warn when a subscriber's `lag` exceeds threshold (today: caller
      must poll `stats` and decide)
- [ ] Prometheus metrics export (optional dependency)
- [ ] Structured logging (tracing in Rust, `logging` in Python)
- [ ] Fuzz testing of ring buffer under concurrent access
- [ ] Stress test: 24h run under load, validate no message loss
- [ ] macOS: switch wakeup to `kqueue` for efficiency (eventfd-equivalent)

---

## Phase 6 — Distribution & Documentation

- [x] README with quickstart + perf table + competitive comparison
- [x] Cross-process examples (`examples/pub.py`, `examples/sub.py`)
- [x] CI: `cargo test` + `clippy` on Linux + macOS (`.github/workflows/ci.yml`)
- [x] Wheel build matrix: `linux/macos × x86_64/aarch64` (`wheels.yml`)
- [x] Docker dev environment for Linux testing from macOS
- [ ] Tag `v0.1.0` → triggers PyPI publish via wheels.yml
- [ ] API reference docs (rustdoc + mkdocs)
- [ ] Use-case mini-guides (FastAPI, ML pipeline, sensor reader)

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
