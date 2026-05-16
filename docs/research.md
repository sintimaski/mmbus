# Market Research

## The Gap

No production-quality, pip-installable, pure-Python library combines:
- Zero-copy mmap data transfer
- A proper pub/sub API (topics, fan-out)
- Multi-process communication (not just intra-thread)
- No external server or C compilation required

---

## Competitive Landscape

### Direct competitors

| Library       | Stars  | Downloads/wk | Zero-copy | No server      | Pub/sub          | Notes                                      |
|---------------|--------|-------------|-----------|----------------|------------------|--------------------------------------------|
| pyzmq         | 4,100  | 25M         | No        | No (C lib)     | Yes              | De facto standard; C dependency; no mmap   |
| diskcache     | 2,700  | 9.6M        | No        | Yes (SQLite)   | No               | Cache only; proves appetite for no-server  |
| huey          | 5,900  | 30K         | No        | Yes (SQLite)   | No (task queue)  | Task queue model, not pub/sub              |
| pypubsub      | 174    | 20K         | No        | Yes            | In-process only  | Cannot cross process boundaries            |
| pynng         | 324    | low         | No        | No (C lib)     | Yes              | nanomsg wrapper; less adoption than ZMQ    |
| kombu         | ~2,500 | 10.8M       | No        | No (Redis/RMQ) | Yes              | Downloads are Celery drag, not real users  |
| ezmsg         | 28     | tiny        | Yes       | Yes            | Yes              | Closest analog; specialized for science    |
| lahja         | 383    | minimal     | No        | Yes            | Yes              | Unmaintained; Ethereum-specific            |
| **mmbus**     | -      | -           | **Yes**   | **Yes**        | **Yes**          | The gap                                    |

### Notable non-Python art

- **iceoryx2** (C++/Rust, 2.1K stars) — true zero-copy IPC for robotics/automotive;
  our Python library is the Python-native equivalent for the long tail of use cases
- **Plasmite** (Rust, 2026) — mmap ring-buffer IPC, no broker; strong HN reception
  with the exact same pitch; validates the concept
- **bslatkin/ringbuffer** (Python, 136 stars) — raw mmap ring buffer, 2 GB/s;
  proves the primitive works; no pub/sub API, not on PyPI

---

## Performance Numbers

From the 2025 brokerless library benchmark (arXiv 2508.07934) and IPC mechanism benchmarks:

| Transport              | Latency P50  | Latency P99  | Throughput    | Syscalls (1M msgs) |
|------------------------|-------------|-------------|---------------|-------------------|
| mmap ring buffer       | ~127 ns     | ~850 ns     | 7.9M msg/s    | ~4 total           |
| POSIX message queues   | ~2,741 ns   | ~12 µs      | 364K msg/s    | 2M                 |
| ZeroMQ IPC             | ~20–40 µs   | —           | 481K msg/s    | high               |
| Redis pub/sub          | ~17 µs      | —           | 59K msg/s     | high               |
| multiprocessing.Queue  | ~6,000 ns   | —           | 80–175K msg/s | high (+ pickle)    |

Key insight: mmap generates only 4 syscalls total for 1M messages (vs 2M for
message queues). The data path never touches the kernel.

For payloads above ~1 KB, the zero-copy advantage compounds — each competing
transport incurs two memory copies (user→kernel, kernel→user) plus potential
serialization, while mmap readers read directly from shared physical pages.

---

## Market Signals

### Developer pain (documented)

- **82% of Python developers do not use Redis** (JetBrains 2024 Python Developer
  Survey). Many are still forced to run Redis just for pub/sub or simple task
  queues in otherwise non-Redis stacks.

- **2024 Redis license change** (BSD → SSPL/RSALv2) triggered mass search for
  alternatives, spawning the Linux Foundation's Valkey fork. Shows strategic
  risk aversion to Redis dependency is mainstream.

- **Hacker News "Do you need Redis?" (2024, item 42036303):** Top comments:
  *"If only one local process will use Redis, you're better off using data
  structures available in your programming language."* — direct validation.

- **Hacker News Plasmite (2026, item 47511435):** Rust mmap IPC library,
  brokerless, received strong reception with identical pitch to mmbus.

- **PostgreSQL LISTEN/NOTIFY** is frequently cited as a Redis pub/sub
  alternative in HN threads — but has an 8 KB message size limit, requires
  Postgres, and has much higher latency. Shows developers are actively looking.

### Comparable adoption stories

- **diskcache** — 2,700 stars, 42M downloads/month — pure Python, no server,
  fills a gap Redis and stdlib don't fill. Proves "no server" positioning works.
- **huey** — 5,900 stars, SQLite mode — same positioning in task queues. Funded
  through Charles Leifer's consulting reputation.
- **pyzmq** — 25M downloads/week — driven partly by Jupyter kernel protocol
  adoption (Jupyter's 30M+ users pull pyzmq). Shows institutional adoption
  can drive massive download numbers.

---

## Underserved Use Cases

### 1. ML inference pipelines (single machine)
Multiple Python worker processes (preprocessor → model runner → postprocessor)
pass large tensors between stages. Current options:
- `multiprocessing.Queue` + pickle: ~250 ms just for serialization of a 10M-element array
- Manual `shared_memory`: no pub/sub semantics, manual synchronization
- Ray: solves it at scale but adds enormous operational complexity

**mmbus gives them zero-copy tensor passing with a clean pub/sub API.**

### 2. FastAPI / Uvicorn multi-worker
Running multiple workers means in-process state isn't shared. WebSocket
broadcast and internal events require "add Redis" — painful for single-server
deployments. No good alternative exists today.

### 3. Edge / embedded Python
Raspberry Pi, Jetson Nano, industrial PLCs running Python. Multiple processes
(sensor reader, control loop, logger) need pub/sub with low overhead. Redis
daemon is wasteful; ZeroMQ requires C.

### 4. Desktop Python apps
Complex apps (PyQt, wxPython) with worker processes need event buses.
`pypubsub` works intra-process only. Developers roll their own or add ZeroMQ
complexity.

### 5. Development and testing
Developers writing against Redis pub/sub APIs run a Redis server for tests.
A library that is production-quality with no external server eliminates this
entirely — same code in dev and prod.

---

## Sources

- arXiv 2508.07934 — Performance Evaluation of Brokerless Messaging Libraries (2025)
- Howtech Substack — IPC Mechanisms: Shared Memory vs Message Queues benchmark
- JetBrains Python Developer Survey 2024
- PyPI Stats: pyzmq, diskcache, huey, kombu, pypubsub
- GitHub: pyzmq, huey, diskcache, ezmsg, lahja, bslatkin/ringbuffer, iceoryx2
- Hacker News: item 42036303 (Redis), item 47511435 (Plasmite)
