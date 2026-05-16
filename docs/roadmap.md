# Roadmap

## Phase 1 — Rust Core

Goal: a working lock-free ring buffer over mmap with Unix socket signaling.
No Python yet. Validate correctness and performance with Rust benchmarks.

- [ ] mmap file creation, header layout, magic/version validation
- [ ] Lock-free ring buffer: atomic head/tail, slot format, wrap-around
- [ ] Single-producer single-consumer (SPSC) first, then MPMC
- [ ] Unix domain socket wakeup (producer sends 1 byte, consumer wakes)
- [ ] `eventfd` on Linux as lower-overhead alternative to Unix socket
- [ ] Crash recovery: dirty-flag protocol, header revalidation on reconnect
- [ ] `fcntl` producer lock (one writer per named bus)
- [ ] Rust benchmarks: latency histogram, throughput, syscall count
- [ ] Basic multi-reader fan-out (each reader tracks its own cursor)

**Success criteria:** matches bslatkin/ringbuffer throughput (~2 GB/s) with
correct behavior under concurrent readers and simulated producer crashes.

---

## Phase 2 — PyO3 Bindings

Goal: expose Rust core to Python with correct GIL semantics and zero-copy.

- [ ] `Bus`, `Publisher`, `Subscriber` as PyO3 classes
- [ ] `pub.publish(bytes)` — write bytes into ring buffer
- [ ] `sub.receive() -> memoryview` — zero-copy read
- [ ] `sub.receive(timeout=N)` — blocking with GIL release
- [ ] `py.allow_threads()` around all blocking waits
- [ ] Buffer protocol: expose slot memory as `memoryview` without copy
- [ ] `maturin develop` workflow; basic Python test suite
- [ ] Publish development wheel to TestPyPI

**Success criteria:** Python round-trip latency under 500 ns for small
messages; numpy array passed as memoryview with no copy verified via
`id()` / buffer address checks.

---

## Phase 3 — Python API

Goal: ergonomic, Pythonic API on top of the raw PyO3 bindings.

- [ ] Topic routing: named channels within a Bus
- [ ] Serialization layer: raw bytes (default), numpy, pickle, msgpack
- [ ] Blocking iterator: `for msg in sub:`
- [ ] Context manager: `with bus.subscriber("x") as sub:`
- [ ] `Publisher.publish(data)` — auto-serializes based on type
- [ ] `Subscriber.receive(timeout)` — typed return
- [ ] `Bus.topics()` — list active topics
- [ ] Error types: `BusFullError`, `BusClosedError`, `TimeoutError`
- [ ] Type annotations throughout
- [ ] Python 3.9+ support

---

## Phase 4 — Async Support

Goal: native asyncio integration without threads.

- [ ] `async for msg in sub.aiter():` — non-blocking async iterator
- [ ] `await sub.receive_async(timeout)` — awaitable receive
- [ ] `asyncio` event loop integration via fd registration (`loop.add_reader`)
- [ ] Compatible with `anyio` / `trio` via abstraction layer
- [ ] FastAPI example: WebSocket broadcast to N clients via mmbus

---

## Phase 5 — Hardening & Observability

Goal: production-ready reliability and debuggability.

- [ ] Backpressure: configurable drop vs. block on full buffer
- [ ] Slow-consumer detection: warn when a subscriber cursor falls behind
- [ ] `Bus.stats()` — throughput, lag, drop count per topic
- [ ] Prometheus metrics export (optional dependency)
- [ ] Structured logging (tracing in Rust, `logging` in Python)
- [ ] Fuzz testing of ring buffer under concurrent access
- [ ] Stress test: 24h run under load, validate no message loss
- [ ] macOS: switch wakeup to `kqueue` for efficiency

---

## Phase 6 — Distribution & Documentation

Goal: frictionless `pip install`, discoverable library.

- [ ] PyPI release under chosen name
- [ ] GitHub Actions CI: build wheels for all targets (manylinux, macOS)
- [ ] `maturin` release workflow
- [ ] README with benchmark comparison (vs ZeroMQ, Redis, multiprocessing)
- [ ] Quickstart guide for each target use case (ML pipeline, FastAPI, edge)
- [ ] API reference docs (mkdocs or rustdoc-style)
- [ ] Example projects: tensor pipeline, multi-worker broadcast, sensor reader

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
