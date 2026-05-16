# mmbus *(working title)*

> Pub/sub for Python. No Redis. No broker. Just `pip install`.

A zero-copy message bus for local inter-process communication, built on
`mmap` + Unix domain sockets, with a Rust core exposed via PyO3.

---

## The problem

Most Python pub/sub and task queue libraries require an external server:

- **Celery / RQ / Dramatiq** → need Redis or RabbitMQ
- **Redis pub/sub** → need a Redis daemon running
- **ZeroMQ** → no server, but C library, no zero-copy, complex API

For single-machine workloads — ML pipelines, multi-worker web apps, edge
devices, desktop apps — running a broker is pure operational overhead.

## The solution

```bash
pip install mmbus          # 1-line install, pre-built wheel, no Rust needed
```

```python
from mmbus import Publisher, Subscriber

# process A
pub = Publisher("sensors")
pub.publish(frame)          # zero-copy into shared mmap

# process B  
sub = Subscriber("sensors")
frame = sub.receive()       # zero-copy read from same mmap pages
```

No server. No configuration. Works on Linux and macOS.

---

## Performance

| Transport              | Latency (P50) | Throughput   |
|------------------------|---------------|--------------|
| **mmbus (mmap)**       | **~127 ns**   | **7.9M/s**   |
| POSIX message queues   | ~2,741 ns     | 364K/s       |
| ZeroMQ IPC             | ~20,000 ns    | 481K/s       |
| Redis pub/sub          | ~17,000 ns    | 59K/s        |
| multiprocessing.Queue  | ~6,000 ns     | 80–175K/s    |

~16x lower latency than ZeroMQ. ~130x lower latency than Redis.

---

## Architecture

```
┌──────────────────────────────────────┐
│  Python API                          │  topics, serialization, async/await
├──────────────────────────────────────┤
│  PyO3 bindings                       │  Publisher, Subscriber, Bus classes
├──────────────────────────────────────┤
│  Rust core                           │  mmap lifecycle, lock-free ring
│                                      │  buffer, atomics, Unix socket
│                                      │  signaling, crash safety
└──────────────────────────────────────┘
```

See [`docs/architecture.md`](docs/architecture.md) for full design.

---

## Target use cases

- ML inference pipelines — pass tensors between workers without pickle copies
- FastAPI / Uvicorn multi-worker — WebSocket broadcast without Redis
- Edge / embedded Python — Raspberry Pi, Jetson, no daemon required
- Desktop apps — cross-process events, no server
- Dev/test environments — production-quality local pub/sub, no Docker

---

## Docs

- [`docs/research.md`](docs/research.md) — market research, competitors, signals
- [`docs/architecture.md`](docs/architecture.md) — technical design
- [`docs/roadmap.md`](docs/roadmap.md) — development phases
