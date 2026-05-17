# mmbus

> Zero-copy pub/sub over `mmap`. No broker. No server. `pip install` and go.

<!-- Docs URL is wired up via .github/workflows/docs.yml — replace OWNER/REPO
     once the project is published to GitHub. -->
[API reference (rustdoc)](https://OWNER.github.io/mmbus/) ·
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

Two layers worth measuring separately. Numbers from `cargo bench` on an
Apple M-series laptop (`benches/ring.rs` + `benches/e2e.rs`).

**Ring layer alone** — in-process publish + receive of a slot, no IPC wakeup:

| Payload | Per-op cost | Throughput   |
|---------|-------------|--------------|
| 32 B    | ~25 ns      | ~40M ops/s   |
| 256 B   | ~35 ns      | ~29M ops/s   |
| 1024 B  | ~57 ns      | ~18M ops/s   |

Single-message round-trip latency (publish + read, 32 B payload): **~11 ns**.

**End-to-end** — separate publisher and subscriber threads, including the
per-message wakeup syscall (`eventfd` on Linux, Unix-socket byte on macOS):

| Payload | Per-msg cost | Throughput     |
|---------|--------------|----------------|
| 32 B    | ~740 ns      | ~1.36M msg/s   |
| 256 B   | ~720 ns      | ~1.39M msg/s   |

The wakeup syscall dominates the e2e number; for fan-out workloads where
the publisher is faster than any single subscriber, the ring-layer
numbers are what matters in practice.  Reference points from public IPC
benchmarks (arXiv 2508.07934 and Linux IPC shootouts) for comparison:

| Transport               | P50 latency | Throughput      |
|-------------------------|-------------|-----------------|
| **mmbus (e2e)**         | **~740 ns** | **~1.36M msg/s**|
| POSIX message queue     | ~2.7 µs     | 364K msg/s      |
| ZeroMQ IPC              | ~20–40 µs   | 481K msg/s      |
| Redis pub/sub           | ~17 µs      | 59K msg/s       |
| `multiprocessing.Queue` | ~6 µs       | 80–175K msg/s   |

Reproduce: `cargo bench --bench ring && cargo bench --bench e2e`.

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

- **ML inference pipelines** — pass tensors between workers without pickle copies
- **Multi-worker web apps** — WebSocket broadcast without standing up Redis
- **Edge / embedded Python** — Raspberry Pi, Jetson; no daemon required
- **Desktop apps** — cross-process events without a server
- **Dev/test environments** — production-quality local pub/sub, no Docker

## Supported platforms

|         | Linux        | macOS        | Windows  |
|---------|--------------|--------------|----------|
| `mmbus` | ✓ (eventfd)  | ✓ (AF_UNIX)  | not yet  |

## API surface

| Call                                       | Behaviour                                          |
|--------------------------------------------|----------------------------------------------------|
| `Bus(name)`                                | open a named bus namespace                         |
| `bus.publish(topic, bytes)`                | publish a message                                  |
| `bus.subscribe(topic) -> Subscription`     | sync subscription (iterator + context manager)     |
| `bus.subscribe_async(topic)`               | asyncio subscription using `add_reader`            |
| `bus.wait_for_subscribers(topic, n)`       | block until *n* subscribers connect                |
| `bus.stats(topic) -> TopicStats`           | ring + socket snapshot                             |

Typed exceptions: `BusFullError`, `MessageTooLargeError`,
`ConnectTimeoutError`, `TooManySubscribersError`, `AlreadyPublishingError`.

## Development

```bash
# Rust core
cargo test
cargo bench --bench ring && cargo bench --bench e2e   # local perf
cargo test --release --test stress -- --ignored       # stress tests

# Fuzz the ring-buffer API (needs nightly + cargo-fuzz)
cd fuzz && cargo +nightly fuzz run ring_publish_receive -- -max_total_time=60

# Python bindings (native build, macOS or Linux)
python -m venv .venv && .venv/bin/pip install maturin
.venv/bin/maturin develop --features python

# Linux test from anywhere (Docker)
docker compose run --rm test
```

## Status

Pre-release.  Core protocol is stable; API may still change before 1.0.
See [`docs/roadmap.md`](docs/roadmap.md).

## Docs

- [`docs/architecture.md`](docs/architecture.md) — technical design
- [`docs/research.md`](docs/research.md) — competitive landscape and signals
- [`docs/roadmap.md`](docs/roadmap.md) — development phases

## License

MIT.
