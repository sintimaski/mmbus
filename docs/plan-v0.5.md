# mmbus v0.5 — Zero-Copy Pipeline + Observability

_Written 2026-05-20.  Picks up from v0.4.0 (topic handles + single-lookup
publish + thin LTO + zero-copy `publish_many`)._

---

## Current state analysis

### What we've closed (v0.2–v0.4)

| Layer | Before v0.2 | Now (v0.4) |
|-------|-------------|------------|
| Durable WAL | none | mmap-backed WAL v2, `arc-swap` lock-free hot path, ~4.6M msg/s |
| Python publish hot path | double hashmap lookup + Python wrapper frame | single lookup + direct bound method (~18-20% faster) |
| Python batch publish | N allocations + N memcpy at FFI boundary | zero-copy borrow from PyBytes (v0.4.0) |
| Python prepared handle | none | `Bus.topic() -> TopicPublisher` |
| Cross-machine | none | `mmbus-bridge` in-process Python SDK (v0.3.x) |
| Prometheus | none | text-format exporter, minimal HTTP server (v0.2.2) |

### Remaining cost centers (where time goes on the hot path)

1. **Receive-side allocation** — every `recv()` does `PyBytes::new_bound_with` + memcpy
   from the ring slot.  For a 1 024 B message that's ~300 ns of allocator + GC
   pressure per message.  Nothing in the current API lets callers avoid this.

2. **Per-message wakeup** — `bus.publish()` fires one `write(fd, 1)` per
   connected subscriber per message.  The eventfd/socket is fast (~380-420 ns
   on Linux) but it's still a syscall.  `publish_many` amortises this to one
   wakeup for N messages; `publish` cannot.

3. **GIL acquisition cost** — each `py.allow_threads` → `py.with_gil` round-trip
   is ~40-60 ns on CPython 3.12.  For single-message `publish` this is now the
   dominant per-call cost after the binding optimizations.

4. **`str` → UTF-8 extraction** — `Bus.publish(topic, data)` pays this on every
   call.  `TopicPublisher` eliminates it; `Bus.publish` cannot without changing
   the API contract.

5. **NATS JetStream** — the competitive table has `_pending_` for NATS.  This is
   a documentation gap, not a code gap, but it undermines trust in the durable
   throughput claim.

### Performance ceiling analysis

Current ceiling for Python round-trip (`publish_many` → `recv_batch`):
- Publish: ~1.34 M msg/s (`publish_many` with zero-copy, one wakeup amortised)
- Receive: ~800 K–1 M msg/s (bounded by `PyBytes` alloc per message)
- Pipeline ceiling: ~700-800 K msg/s (receive side bottleneck)

With zero-copy receive (`recv_into`):
- Receive cost drops by ~300-400 ns/msg (no allocation)
- Projected ceiling: 1.0-1.2 M msg/s round-trip

With wakeup coalescing:
- Publish cost at high fan-out (4 subscribers): 4× wakeup syscalls → skip if subscriber has unread
- Projected benefit: 20-30% throughput gain at fan-out ≥ 2, no gain at fan-out = 0

---

## v0.5.0 milestones

### M1: Zero-copy receive (`recv_into`) — P0 ✓ SHIPPED

**Goal:** eliminate the per-message `PyBytes` allocation on the receive side.

> **Shipped** (Unreleased).  Implemented as three single-message methods —
> `recv_into(buf)`, `recv_timeout_into(buf, timeout_secs)`, `try_recv_into(buf)`
> — plus the `max_payload_size` property.  WAL-replay aware.  Measured
> ~1140 ns/msg vs ~4410 ns/msg for `try_recv` on a 1 KB drain bench (~3.9×;
> the projected 300-400 ns figure was conservative — the `PyBytes` alloc + GC
> + second memcpy cost more at 1 KB than estimated).  The blocking variants
> use a wait-then-read loop: the wakeup wait runs with the GIL released and
> never touches the buffer, then a single memcpy reads the ring slot straight
> into the caller's buffer with the GIL held (the held `PyBuffer` pins the
> memory across the wait).  Rust tests: `bus_api_recv_into_slice_*` in
> `tests/spsc.rs`.

**API:**
```python
sub = bus.subscribe("events")

# allocate once
buf = bytearray(64 * 1024)
view = memoryview(buf)

while True:
    n = sub.recv_into(view)     # writes directly into buf, returns byte count
    process(view[:n])           # zero extra copy
```

**Rust side:** `PySubscription::recv_into(&mut self, _py: Python<'_>, buf: &Bound<'_, PyByteArray>) -> PyResult<Option<usize>>`
- Acquire the ring slot pointer, memcpy into `buf.as_bytes_mut()`, advance cursor.
- Returns `None` on timeout / no data; returns `Some(n)` on success.
- GIL held (write into Python-owned buffer requires GIL; the write is fast).
- Blocking variant: `recv_into` (blocks until data); `try_recv_into` (returns immediately).

**Why:** receive is now the throughput bottleneck after publish-side zero-copy.
For ML workloads (numpy arrays, raw tensors), this eliminates the allocator
entirely from the data path.  A caller pre-allocates a `bytearray` matching
their tensor size and reuses it across messages.

**Acceptance criteria:**
- `sub.recv_into(buf)` writes the next message payload into `buf`, returns byte count.
- `sub.try_recv_into(buf)` returns `None` if no message available.
- `len(buf) < message_size` raises `MessageTooLargeError`.
- Existing `recv()` / `recv_timeout()` paths are unchanged.
- Micro-bench shows ~300-400 ns/msg improvement vs `recv()` for 1 KB payloads.

---

### M2: Wakeup coalescing — P1 ✓ SHIPPED

**Goal:** skip wakeup broadcast when subscribers already have unread messages.

> **Shipped** (Unreleased) — the per-subscriber design (wire v4→v5).  A
> per-cursor `needs_wakeup` flag table was added to the ring header; the
> connect handshake now carries `cursor_idx` to the publisher (Linux
> `SCM_RIGHTS` iovec / macOS socket prefix / Windows handshake struct) so
> `broadcast_wakeup` can address each subscriber's flag.  Missed-wakeup
> safety is an eventcount: subscriber `set_wakeflag` + SeqCst fence +
> re-check (`wait_readable`/`arm_wakeup`); publisher tail-store + SeqCst
> fence + `take_wakeflag`.  The asyncio path arms via `arm_wakeup` before
> awaiting and drains via `drain_wakeup`.  Clean-disconnect reaping is
> preserved by also probing clients whose cursor went `UNCLAIMED`.
> Measured ~54% fewer wakeup syscalls on a 20 K-message Python burst.
> Tests: `tests/wakeup_coalescing.rs` (+ fan-out/restart stress green on
> v5).  New `wakeups_sent_total` counter exposes the ratio.

**Problem:** high-throughput producers call `publish()` in a tight loop.  Each
call fires a `write(eventfd, 1)` or `send(socket, b"\x01")` for each subscriber.
If the subscriber is already behind (cursor < tail), the wakeup is redundant —
it will re-drain on its current wake.

**Mechanism:**
- Add a `pending_wakeup: AtomicBool` (or use the existing eventfd counter) in
  the subscriber cursor table.
- Publisher checks: if `cursor < tail - 1` (subscriber is already behind by >1),
  skip the wakeup.
- Subscriber clears the flag after each drain, then re-checks the ring before
  blocking.

**Risk:** the "check then skip" creates a TOCTOU window.  Need a
compare-and-swap protocol to avoid missing a wakeup.  Likely adds ~5 ns to
the hot path; need to bench that tradeoff.

**Acceptance criteria:**
- At fan-out=4 and publisher rate > consumer rate, wakeup syscall count drops
  ≥ 50% vs current.
- No missed wakeup under the fuzz harness (`ring_concurrent`) for 10M iterations.
- `publish_single` benchmark shows no regression at fan-out=0.

---

### M3: Structured logging — P1 ✓ SHIPPED

**Goal:** replace ad-hoc `eprintln!`/stderr with the `tracing` crate throughout.

> **Shipped** (Unreleased).  The Rust core already emitted `tracing` events
> at lifecycle/WAL points; M3 adds the missing piece for Python users —
> `mmbus.init_logging(level=None)` (Rust: `mmbus::init_logging`, behind a new
> opt-in `logging` feature pulling `tracing-subscriber`; the wheel always
> enables it).  `RUST_LOG` takes precedence over the level arg
> (`RUST_LOG=mmbus=debug`, per-target like `mmbus::wal=trace`).  Idempotent.
> Added a publisher-restart `warn` event in `poll_recv`.
>
> **Deviation from the original sketch:** `publish`/`publish_many`/`recv` are
> deliberately NOT `#[instrument]`ed — that path is protected by the
> hot-path discipline (no per-message atomic/span cost).  Events fire at
> lifecycle + error granularity, which is also the right granularity for ops
> (no log line per message).  Verified the no-WAL publish bench is unchanged.

**Current state:** `tracing` is already in `Cargo.toml` and a few
`tracing::info!` calls exist.  The gaps are: no `#[tracing::instrument]` on
key entry points, and no Python-side exposure.

**Plan:**
- Add `#[tracing::instrument(skip(data), fields(topic, len = data.len()))]`
  to `Publisher::publish`, `Publisher::publish_many`, `Subscription::recv`.
- Add `mmbus.init_logging(level="INFO")` in Python — calls
  `tracing_subscriber::fmt::init()` with the requested level.
- Document the `RUST_LOG=mmbus=debug` env var pattern for debugging.

**Acceptance criteria:**
- `RUST_LOG=mmbus=info python pub.py` emits structured events to stderr.
- `mmbus.init_logging()` is a no-op if called more than once (guards with
  `Once`).
- No tracing overhead on the hot path when no subscriber is active (the
  existing `tracing` feature already short-circuits on the atomic check).

---

### M4: NATS JetStream competitive bench — P1 ✓ SHIPPED (CI job)

**Goal:** close the `_pending_` entry in the competitive table.

> **Shipped** (Unreleased) — `.github/workflows/competitive-bench.yml` runs
> the full suite (`benches/competitive/run_all.sh`, which boots Redis + NATS
> Docker containers) on `ubuntu-latest` and uploads `results.json` +
> `RESULTS.md` as an artifact.  The NATS JetStream bench already existed and
> works on Linux Docker (the "pending" cell was a macOS Docker convergence
> problem, not a code one).  `_common.TOTAL_N` now honours a `BENCH_N` env
> override so CI can bound the slow durable runs uniformly across harnesses;
> verified locally on the mmbus harness.
>
> **Deviation from the AC:** triggers on `workflow_dispatch` + release tags
> (`v*`), NOT per-push to main — competitive numbers are noisy and the
> durable runs are slow/costly, so per-merge runs aren't worth it.  The
> resulting NATS number must be pasted into
> `docs/benchmarks-vs-competition.md` + the README durable table after the
> first CI run (left as a release step — can't run Docker/NATS on the dev
> box here).

**Current:** macOS Docker networking makes the NATS container unreliable
(topic convergence timeouts).  The `benches/competitive/run_all.sh` script
aborts early on macOS.

**Plan:**
- Add a GitHub Actions job on `ubuntu-latest` that spins up NATS JetStream
  via `docker compose` and runs `benches/competitive/nats_bench.py`.
- Store results as a CI artifact; update `docs/benchmarks-vs-competition.md`
  and README table.
- Expected result: NATS JetStream durable throughput ~0.2-0.5 M msg/s (similar
  to Redis Streams; both are limited by fsync + loopback TCP overhead).

**Acceptance criteria:**
- CI job `competitive-bench` runs on push to main and on tag pushes.
- README durable table has a concrete NATS number with `(Linux CI)` annotation.

---

### M5: Bridge asyncio wrapper — P2 ✓ SHIPPED

**Goal:** `await bridge.wait_async()` so the bridge can run in an asyncio
event loop without blocking it.

> **Shipped** (Unreleased).  Pure-Python over the existing
> `is_running`/`shutdown` (no Rust change needed — `wait()` was already a
> liveness poll): `wait_async(poll_interval=0.1)` suspends via
> `asyncio.sleep` until the bridge stops; `shutdown_async(timeout=None)`
> joins off-loop via `run_in_executor` (3.8-compatible, not
> `asyncio.to_thread`); `async with Bridge(cfg)` mirrors the sync CM.
> Example: `bridge/examples/bridge_async.py`.  Logic verified against a
> mock; the full bridge wheel build is blocked locally by its
> `mmbus==0.3.0` pin (`bridge/pyproject.toml`) — that pin must be widened
> to admit mmbus 0.4/0.5 when v0.5.0 is tagged.

**Current:** `bridge.wait()` blocks the calling thread indefinitely.  Async
users wrap it in `asyncio.to_thread(bridge.wait)`.  This is documented but
ugly — it should be a first-class async API.

**Plan:**
- `Bridge.wait_async() -> asyncio.Future`: spawns the bridge on a worker
  thread, returns a Future that resolves when the bridge shuts down.
- `Bridge.shutdown_async()`: signals shutdown + awaits completion.
- Mirror the existing sync API 1:1.

**Acceptance criteria:**
- `async with Bridge(...) as b: await b.wait_async()` works in asyncio.
- `bridge.wait()` (sync) is unchanged.
- Example `examples/bridge_async.py` demonstrates the pattern.

---

## v0.6.0 targets (post v0.5 — 6-8 weeks out)

### Windows runtime CI

The Windows-conditional code (`windows-sys` path for `LockFileEx`,
`CreateNamedPipe`, `WaitForMultipleObjects`) type-checks but has no runtime
green CI.  v0.6.0 gates are:

1. `tests/` suite passes on `windows-latest` in CI (currently the suite
   probably panics on the Unix-socket / eventfd paths).
2. `examples/pub.py` + `examples/sub.py` round-trip on Windows.
3. `wheels.yml` Windows wheel slot builds to a `.whl` that installs and runs
   the smoke test.

Estimate: 2-3 weeks of focused work (the named-pipe handshake + wakeup path
is the bulk of it).

### WAL config dict

Currently `Bus(wal_enabled=True/False)` is the only Python-level knob.  Power
users want `fsync_policy`, `segment_size`, `retention_count`.  A dict/dataclass
parameter allows this without a proliferating kwarg surface:

```python
bus = Bus("events", wal={
    "enabled": True,
    "fsync_policy": "batch",   # "each" | "batch" | "none"
    "segment_size": 64 * 1024 * 1024,
    "retention_count": 4,
})
```

### Bridge per-peer/topic stats

`bridge.stats()` currently returns aggregate counters.  Per-peer + per-topic
breakdown needed for production monitoring.  Shape (to finalize):

```python
{
  "peers": {
    "peer-a": {"sent": 12345, "recv": 67890, "lag_ms": 2.1, "connected": True}
  },
  "topics": {
    "events": {"forwarded": 12345, "dropped": 0}
  }
}
```

---

## v1.0 gates

1. API freeze on `Bus`, `Subscription`, `TopicPublisher`, `TopicStats`.
2. Wire format `VERSION = 5` (if layout changes; otherwise `4` stands).
3. Windows runtime CI green (mandatory for PyPI stable).
4. `recv_into` / zero-copy receive shipped.
5. Structured logging shipped.
6. `cargo audit` clean.
7. All `[ ]` items in `docs/release-checklist.md` cleared.

---

## Discarded ideas (with rationale)

- **kqueue wakeup on macOS** — `EVFILT_USER` is per-kqueue instance, not
  transferable across processes via `SCM_RIGHTS`.  The existing Unix-socket
  path is ~720 ns e2e, close enough to Linux eventfd that the rewrite cost
  isn't justified.

- **Variable slot size** — complicates the ring layout (pointer arithmetic
  becomes non-trivial), breaks the O(1) slot-index → pointer math that the
  seqlock depends on.  Fixed slot size is a deliberate trade-off.

- **Multi-producer (MPMC)** — requires either a per-slot CAS loop on the
  tail (high contention at pub rate > 1M msg/s) or a per-producer ring with
  a merge layer.  Neither preserves the current wire format.  SPMC covers
  the dominant use case; MPMC is a post-v1 research item.

- **Fat LTO** — measured at ~257-262 ns/call alongside thin and no-LTO;
  within noise.  Thin gives cross-crate inlining at lower build cost.

- **`allow_threads` around `publish_many`** — requires copying payload data
  before releasing the GIL (lifetimes).  The copy cost exceeds the GIL-hold
  cost for typical batches (< 2 ms of ring writes).  Zero-copy + hold-GIL
  is the correct trade-off.

---

## Open questions for v0.5

1. **`recv_into` blocking semantics**: should it respect a `timeout_secs`
   argument like `recv_timeout`, or should the blocking/non-blocking split
   be two separate methods (`recv_into` / `try_recv_into`)?  Recommendation:
   two methods — cleaner API, no optional-arg ambiguity.

2. **Wakeup coalescing granularity**: coalesce per-subscriber (skip if that
   subscriber is behind) or globally (skip if ANY subscriber is behind)?
   Per-subscriber is more precise but requires per-cursor flags in the mmap
   header — a wire-format consideration.  If we skip per-cursor flags, we
   can do a simpler "skip if head - cursor > threshold" heuristic.

3. **`recv_into` for async**: `AsyncSubscription` and `AnyioSubscription`
   currently return `bytes`.  Adding `recv_into_async` is straightforward
   but doubles the async surface.  Alternative: document `recv_into` as
   sync-only and rely on `asyncio.to_thread` for async callers (acceptable
   given the ML workload target is rarely fully async).
