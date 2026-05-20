# mmbus

> Zero-copy pub/sub over `mmap`. No broker. No server. `pip install` and go.

<!-- Docs URL is published by .github/workflows/docs.yml. -->
[API reference (rustdoc)](https://sintimaski.github.io/mmbus/) ·
[Architecture](docs/architecture.md) ·
[Roadmap](docs/roadmap.md)

`mmbus` is a Python library for single-machine, multi-process publish/subscribe
messaging.  The data path is a lock-free ring buffer in shared memory —
readers read directly from the same physical pages the writer wrote to, with
**zero copies** and no kernel involvement in the data path.  Wakeup signalling
uses `eventfd(2)` on Linux and Unix domain sockets on macOS.

The core is Rust (~1.5k lines); the public API is Python.

```python
from mmbus import Bus

# Publisher
bus = Bus("my-app")
bus.wait_for_subscribers("events", n=1)
bus.publish("events", b"hello")

# Subscriber (another process)
bus = Bus("my-app")
for msg in bus.subscribe("events"):
    print(msg)
```

---

## Why

|                       | broker needed | zero-copy | pub/sub             | install        |
|-----------------------|---------------|-----------|---------------------|----------------|
| Redis pub/sub         | yes (server)  | no        | yes                 | server + client|
| ZeroMQ (`pyzmq`)      | no            | no        | yes                 | C lib + bindings|
| `multiprocessing.Queue`| no           | no        | no (point-to-point) | stdlib         |
| **`mmbus`**           | **no**        | **yes**   | **yes**             | `pip install`  |

If you want pub/sub on one machine without standing up Redis or compiling a
C library, this is what you want.

## Install

```bash
pip install mmbus
```

Pre-built wheels for Linux (x86_64, aarch64) and macOS (x86_64, arm64).
Python ≥ 3.8.

## Quick start

Two files, two terminals.

`pub.py`:
```python
from mmbus import Bus
import time

bus = Bus("demo")
bus.wait_for_subscribers("ticks", n=1)
for i in range(10):
    bus.publish("ticks", f"tick {i}".encode())
    time.sleep(0.2)
```

`sub.py`:
```python
from mmbus import Bus

bus = Bus("demo")
with bus.subscribe("ticks") as sub:
    for msg in sub:
        print(msg)
```

```
$ python sub.py &
$ python pub.py
b'tick 0'
b'tick 1'
...
```

Full working scripts in [`examples/`](examples/).

## Async

`AsyncSubscription` uses `loop.add_reader` on the wakeup fd (eventfd on
Linux, Unix socket on macOS) — no thread pool, the event loop is notified
directly:

```python
import asyncio
from mmbus import Bus

async def main():
    bus = Bus("demo")
    sub = await bus.subscribe_async("events")
    async with sub:
        async for msg in sub:
            print(msg)

asyncio.run(main())
```

For **trio** or other anyio backends, use `AnyioSubscription` (one worker
thread per concurrent recv — strictly slower than `AsyncSubscription`, but
portable):

```python
import anyio
from mmbus import Bus

async def main():
    bus = Bus("demo")
    sub = await bus.subscribe_anyio("events")
    async with sub:
        async for msg in sub:
            print(msg)

anyio.run(main, backend="trio")  # or "asyncio"
```

Needs `pip install anyio`; the import is lazy so it's a true opt-in.

## Performance

Four regimes worth measuring separately.  Numbers from `cargo bench`,
`benches/compare.py`, and `benches/competitive/` on an Apple M-series
laptop (APFS).

**1. Rust ring layer alone** — in-process publish + receive of a slot,
no IPC wakeup:

| Payload | Per-op cost | Throughput   |
|---------|-------------|--------------|
| 32 B    | ~25 ns      | ~40M ops/s   |
| 256 B   | ~35 ns      | ~29M ops/s   |
| 1024 B  | ~57 ns      | ~18M ops/s   |

Single-message round-trip latency (publish + read, 32 B payload): **~11 ns**.

**2. Rust end-to-end** — separate publisher and subscriber threads
including the per-message wakeup syscall (`eventfd` Linux, Unix-socket
byte macOS):

| Payload | Per-msg cost | Throughput     |
|---------|--------------|----------------|
| 32 B    | ~740 ns      | ~1.36M msg/s   |
| 256 B   | ~720 ns      | ~1.39M msg/s   |

The wakeup syscall dominates; for fan-out workloads where the publisher
outpaces any single subscriber, the ring-layer numbers in (1) are what
matter in practice.

**3. From Python** — `benches/compare.py` runs an apples-to-apples
cross-thread bench against `pyzmq` over `ipc://`, 10 000 messages each:

| Payload | mmbus              | pyzmq              | gap |
|---------|--------------------|--------------------|-----|
| 64 B    | ~272 K msg/s       | ~889 K msg/s       | pyzmq ~3× faster |
| 1024 B  | ~198 K msg/s       | ~746 K msg/s       | pyzmq ~4× faster |
| 16384 B | ~17 K msg/s        | ~204 K msg/s       | pyzmq ~12× faster |

**Honest take:** for small *single-message* Python publishes, pyzmq
today is faster than mmbus.  Its Cython wrapper batches messages into a
single socket write on a background I/O thread, while mmbus does one
PyO3 call + one wakeup syscall + one `PyBytes` allocation per message.
The per-call PyO3 cost — not the design — is the bottleneck:
`publish_many` amortizes it and lifts same-host Python throughput to
**~1.34 M msg/s** (table 4).  Where mmbus also wins is at the **Rust
level** (table 1: ~40M ops/s, ~25 ns roundtrip) and in the operational
story: no broker, no daemon, single `pip install`.

**4. vs ZeroMQ / Redis Streams / NATS JetStream** — same machine,
256 B payload, 1M messages, 1 publisher + 1 consumer, batched publishes
where the framework supports them (mmbus uses `publish_many` to amortize
the per-call cost table 3 pays).  Full methodology + caveats in
[`docs/benchmarks-vs-competition.md`](docs/benchmarks-vs-competition.md):

_Non-durable_ (ephemeral same-host pub/sub):

| Framework            | Sustained    |
|----------------------|--------------|
| ZeroMQ PUSH/PULL (`ipc://`) | **1.65 M/s** |
| **mmbus**            | **1.34 M/s** |

_Durable_ (crash-safe, fsync on):

| Framework                          | Sustained    |
|------------------------------------|--------------|
| **mmbus** (Rust criterion, pure publish) | **~4.6 M/s** |
| **mmbus** (Python wheel)           | **1.06 M/s** |
| Redis Streams                      | 0.12 M/s     |
| NATS JetStream                     | _measured by the `competitive-bench` CI job (Linux + Docker); pending first run_ |

ZeroMQ edges mmbus on raw non-durable throughput (more aggressive
outbound buffering) but has no built-in durability.  With durability
on, mmbus's mmap-backed WAL is **~9× faster than Redis Streams** on the
Python comparison (skipping the loopback-TCP + per-second-fsync wall),
and ~38× in pure Rust.  Of the four, mmbus is the only one offering
durable replay for same-host pub/sub without a separate broker or
library.

Reproduce: `cargo bench --bench ring && cargo bench --bench e2e &&
python benches/compare.py`; competitive suite: `cd benches/competitive
&& ./run_all.sh` (needs Docker for the Redis + NATS containers).

## How it works

```
┌────────────┐     mmap ring buffer (shared memory)         ┌──────────────┐
│  Publisher │ ───>┌──┬──┬──┬──┬──┬──┐<──── cursor A  ─── │ Subscriber A │
│            │     └──┴──┴──┴──┴──┴──┘<──── cursor B  ─── │ Subscriber B │
└──────┬─────┘                                              └──────▲───────┘
       │                                                           │
       └── 1-byte wakeup (eventfd on Linux, AF_UNIX on macOS) ─────┘
```

- Writes go into a lock-free SPMC ring; each subscriber tracks its own cursor
  in an atomic table inside the mmap header.
- After every publish the writer sends a 1-byte wakeup to each connected
  subscriber.  The subscriber wakes, reads its slot directly out of the
  shared pages, and advances its cursor.
- On publisher death, subscribers see `POLLHUP` on the handshake socket and
  the iterator terminates cleanly.

See [`docs/architecture.md`](docs/architecture.md) for the full design.

## Target use cases

- **ML inference pipelines** — pass tensors between workers without pickle copies ([`examples/np_pipeline.py`](examples/np_pipeline.py))
- **Multi-worker web apps** — WebSocket broadcast without standing up Redis ([`examples/fastapi_broadcast.py`](examples/fastapi_broadcast.py))
- **Edge / embedded Python** — Raspberry Pi, Jetson; no daemon required
- **Desktop apps** — cross-process events without a server
- **Dev/test environments** — production-quality local pub/sub, no Docker

## Cross-machine pub/sub

mmbus is same-host by design, but the optional **bridge** relays topics
across machines over TCP.  For embedded use (the recommended path), the
`mmbus-bridge` companion wheel runs the bridge in-process:

```bash
pip install mmbus[bridge]      # pulls in the mmbus-bridge wheel
```

```python
from mmbus_bridge import Bridge

with Bridge({
    "bus": "my-app",
    "listen": "0.0.0.0:4443",
    "topics": [{"name": "events"}],
    "peers": [{"name": "b", "endpoint": "b.host:4443",
               "preshared_key": "shared-secret"}],
}) as bridge:
    print(f"listening on {bridge.listen_addr}")
    bridge.wait()              # blocks until Ctrl-C / shutdown()
```

A locally-published `events` message is forwarded to every peer and
republished onto each peer's local bus.  The config dict mirrors the
bridge TOML schema 1:1.

For asyncio services, `async with Bridge(...)` + `await bridge.wait_async()`
runs the bridge without blocking the event loop (the bridge itself runs on
its own threads).  See [`bridge/examples/bridge_async.py`](bridge/examples/bridge_async.py).

**Which bridge entry point?**

| You want…                                   | Use                                    |
|---------------------------------------------|----------------------------------------|
| The bridge inside your Python service       | `mmbus_bridge.Bridge` (this wheel)     |
| systemd / supervised standalone daemon      | `mmbus.bridge.run` / `.spawn` (shells out to the binary) |
| QUIC + TLS transport                        | standalone binary, `cargo install --path bridge --features quic` |

The Python wheel is **TCP-only** — a QUIC peer config raises
`BridgeQuicError` at `start()`.  See
[`docs/rfc-bridge-python-sdk.md`](docs/rfc-bridge-python-sdk.md).

## Supported platforms

|         | Linux        | macOS        | Windows  |
|---------|--------------|--------------|----------|
| `mmbus` | ✓ (eventfd)  | ✓ (AF_UNIX)  | not yet  |

## API surface

| Call                                       | Behaviour                                          |
|--------------------------------------------|----------------------------------------------------|
| `Bus(name, backpressure="error" \| "drop_oldest")` | open a named bus namespace                  |
| `bus.publish(topic, bytes)`                | publish a message                                  |
| `bus.topic(topic) -> TopicPublisher`       | prepared publish handle for hot loops (skips per-call topic lookup) |
| `bus.subscribe(topic) -> Subscription`     | sync subscription (iterator + context manager)     |
| `sub.recv() -> bytes`                      | receive one message (allocates a fresh `bytes`)    |
| `sub.recv_into(buf) -> int`                | allocation-free receive into a writable buffer (`bytearray`/`memoryview`/numpy `uint8`); returns byte count |
| `sub.try_recv_into(buf) -> int \| None`    | non-blocking allocation-free receive; `None` if empty |
| `sub.max_payload_size`                     | largest payload a message can carry (size `recv_into` buffers to this) |
| `bus.subscribe_with_history(topic, n)`     | sync subscription replaying the last *n* in-ring messages |
| `bus.subscribe_from(topic, cursor)`        | sync subscription starting at an explicit cursor   |
| `bus.subscribe_async(topic)`               | asyncio subscription using `add_reader`            |
| `bus.subscribe_anyio(topic)`               | cross-backend (trio + asyncio) via `anyio.to_thread` |
| `bus.wait_for_subscribers(topic, n)`       | block until *n* subscribers connect                |
| `bus.stats(topic) -> TopicStats`           | ring + socket snapshot                             |
| `bus.slow_subscribers(topic, threshold)`   | `(cursor_idx, lag)` for laggards (monitoring)      |
| `bus.clean_topic(topic)`                   | wipe on-disk state (dev/test tooling)              |

Typed exceptions: `BusFullError`, `MessageTooLargeError`,
`ConnectTimeoutError`, `TooManySubscribersError`, `AlreadyPublishingError`,
`CursorTooOldError`.

## Observability

mmbus emits structured [`tracing`](https://docs.rs/tracing) events at
lifecycle points — publisher created, subscriber connected/dropped,
publisher restart, WAL rotation/retention.  They stay silent until a
subscriber is installed.

From Python, turn them on with `init_logging` (prints to stderr):

```python
import mmbus
mmbus.init_logging("info")          # or "debug", "mmbus=trace", …
# …or set RUST_LOG=mmbus=debug in the environment (takes precedence)
```

From Rust, enable the `logging` feature and call
`mmbus::init_logging(Some("info"))`, or wire your own
`tracing_subscriber`.  Filtering follows `RUST_LOG` when set (e.g.
`RUST_LOG=mmbus=debug`, `RUST_LOG=mmbus::wal=trace`), otherwise the
level argument.

**Metrics:** `bus.stats(topic)` returns monotonic counters
(`published_total`, `full_rejected_total`, `subscribers_dropped_total`,
`wakeups_sent_total`) plus ring/WAL gauges.  The optional `prometheus`
feature exposes them in Prometheus text format.

## Development

```bash
# Rust core
cargo test
cargo bench --bench ring && cargo bench --bench e2e   # local perf
cargo test --release --test stress -- --ignored       # stress tests

# Fuzz the ring-buffer API (needs nightly + cargo-fuzz)
cd fuzz && cargo +nightly fuzz run ring_publish_receive -- -max_total_time=60

# Miri is intentionally not part of the test loop: every unsafe block in
# this crate either calls libc (eventfd, flock, sendmsg, recv) or maps a
# file (memmap2), neither of which Miri can execute.  Coverage of the
# unsafe surface comes from the fuzz harness + stress tests instead.

# Python bindings (native build, macOS or Linux)
python -m venv .venv && .venv/bin/pip install maturin
.venv/bin/maturin develop --features python

# Linux test from anywhere (Docker)
docker compose run --rm test
```

## Status

Pre-release.  Core protocol is stable; API may still change before 1.0.
See [`CHANGELOG.md`](CHANGELOG.md) for what's in the current release and
[`docs/roadmap.md`](docs/roadmap.md) for what's planned.

## Security

Single-machine, same-user IPC.  See [`SECURITY.md`](SECURITY.md) for
the threat model and how to report issues.

## Docs

- [`docs/architecture.md`](docs/architecture.md) — technical design
- [`docs/research.md`](docs/research.md) — competitive landscape and signals
- [`docs/benchmarks-vs-competition.md`](docs/benchmarks-vs-competition.md) — mmbus vs ZeroMQ / Redis Streams / NATS, methodology + numbers
- [`docs/roadmap.md`](docs/roadmap.md) — development phases
- [`docs/plan-v0.5.md`](docs/plan-v0.5.md) — v0.5 plan: zero-copy receive, wakeup coalescing, structured logging
- [`docs/rfc-wal-replay.md`](docs/rfc-wal-replay.md) — Phase A (in-ring history, shipped)
- [`docs/rfc-wal-phase-b.md`](docs/rfc-wal-phase-b.md) — Phase B (durable WAL) full spec
- [`docs/plan-wal-phase-b.md`](docs/plan-wal-phase-b.md) — Phase B implementation plan (W1-0 thru W1-f)
- [`docs/rfc-multi-machine.md`](docs/rfc-multi-machine.md) — design for the `mmbus-bridge` relay
- [`docs/rfc-b4b-quic.md`](docs/rfc-b4b-quic.md) — bridge B4b QUIC transport spec
- [`docs/rfc-bridge-python-sdk.md`](docs/rfc-bridge-python-sdk.md) — in-process bridge Python SDK (`mmbus_bridge.Bridge`, v0.3.x)
- [`docs/rfc-windows.md`](docs/rfc-windows.md) — design for Windows support

## License

MIT.
