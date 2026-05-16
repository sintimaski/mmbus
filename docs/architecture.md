# Architecture

## Layers

```
┌──────────────────────────────────────────────────┐
│  Python API                                      │
│  - Topic routing                                 │
│  - Serialization (bytes, numpy, pickle, msgpack) │
│  - async/await support (asyncio)                 │
│  - Context managers, type hints                  │
├──────────────────────────────────────────────────┤
│  PyO3 bindings                                   │
│  - Publisher, Subscriber, Bus Python classes     │
│  - GIL release during blocking waits            │
│  - memoryview / buffer protocol for zero-copy   │
├──────────────────────────────────────────────────┤
│  Rust core                                       │
│  - mmap lifecycle (open, resize, unmap)          │
│  - Lock-free ring buffer (SPMC / MPSC)           │
│  - Atomic head/tail pointers (AcqRel ordering)  │
│  - Unix domain socket signaling (wakeup)         │
│  - Crash safety / recovery                       │
│  - Platform abstraction (Linux / macOS)          │
└──────────────────────────────────────────────────┘
```

---

## Data Path (zero-copy)

```
Producer process                    Consumer process
─────────────────                   ────────────────
pub.publish(data)
  │
  ▼
Write data into                     mmap region
shared mmap region ◄──────────────► (same physical
(advance tail ptr)                   memory pages)
  │                                      ▲
  ▼                                      │
Send 1-byte wakeup    Unix socket    read wakeup
via Unix socket   ──────────────────►    │
                                         ▼
                                   read data directly
                                   from mmap (no copy)
                                   sub.receive() → memoryview
```

The mmap region is the **only** place data lives. The Unix socket carries
a single wakeup byte — it never carries payload data. This means:

- Producer: 0 copies (writes directly into shared pages)
- Consumer: 0 copies (reads directly from shared pages via memoryview)
- Kernel boundary: crossed only for the wakeup signal, not data

---

## Ring Buffer Design

```
┌────────────────────────────────────────────────────┐
│  Header (fixed size, at mmap offset 0)             │
│  ┌────────────┬────────────┬──────────────────────┐│
│  │ magic      │ version    │ capacity             ││
│  │ head (atomic) │ tail (atomic) │ ...           ││
│  └────────────┴────────────┴──────────────────────┘│
├────────────────────────────────────────────────────┤
│  Slots [0..N]                                      │
│  ┌────────────────────────────────────────────────┐│
│  │ slot_len (atomic u32) │ payload bytes...       ││
│  └────────────────────────────────────────────────┘│
└────────────────────────────────────────────────────┘
```

- **head** — next slot to read (consumer-owned, atomic)
- **tail** — next slot to write (producer-owned, atomic)
- Each slot has a fixed maximum size (configurable at Bus creation)
- Variable-length messages: slot stores actual length prefix
- Wrap-around: tail wraps modulo capacity; head chases tail
- Full buffer: producer blocks or drops (configurable policy)

### Multi-reader fan-out

Each subscriber tracks its own read cursor independently. The ring buffer
retains messages until the slowest subscriber has read them (or a TTL
expires). This is O(1) per subscriber regardless of subscriber count.

---

## Signaling (wakeup mechanism)

| Platform | Mechanism | Notes |
|----------|-----------|-------|
| Linux    | `eventfd` | kernel-native, minimal overhead |
| Linux    | Unix domain socket | fallback, slightly more overhead |
| macOS    | Unix domain socket | `kqueue` for efficient waiting |
| Windows  | Named pipe | fallback; limits fan-out |

Subscribers call `select`/`epoll`/`kqueue` on the wakeup fd. The Rust core
releases the Python GIL during this wait, so Python threads are not blocked.

---

## Crash Safety

A crashed producer must not leave consumers in an inconsistent state:

1. **Ring buffer header has a `dirty` flag** — set before writing a slot,
   cleared after. On recovery, consumers skip any slot with `dirty=1`.
2. **mmap is backed by a file** (not anonymous) — survives process crashes.
   On reconnect, a new producer validates the header magic and version.
3. **Atomic tail advance** — tail pointer is only advanced after the slot
   payload is fully written. Consumers never see partial writes.
4. **Bus file lock** (`fcntl.flock`) — only one producer per named bus
   can hold the write lock; second producer waits or fails fast.

---

## Why Rust over Pure Python

| Concern | Pure Python | Rust + PyO3 |
|---------|------------|-------------|
| Atomic ring buffer ops | `ctypes` hacks, unsafe | `AtomicUsize` with memory ordering |
| GIL during subscriber wait | Held (blocks other threads) | Released via `py.allow_threads()` |
| Zero-copy to numpy | Limited | Native buffer protocol / memoryview |
| mmap lifecycle | `__del__` unreliable | `Drop` trait, guaranteed cleanup |
| Crash safety invariants | Runtime checks only | Compiler-enforced + runtime checks |
| Cross-platform | Python abstracts most | `cfg(target_os)` per-platform code |

---

## Python API Design (target)

```python
from mmbus import Bus, Publisher, Subscriber
import numpy as np

# create or connect to a named bus
bus = Bus("my-app", slot_size=1024 * 1024, capacity=256)

# publisher (any process)
pub = bus.publisher("video-frames")
frame = np.zeros((1080, 1920, 3), dtype=np.uint8)
pub.publish(frame)                        # zero-copy into mmap

# subscriber (any other process)
sub = bus.subscriber("video-frames")
for frame in sub:                         # blocking iterator
    process(frame)                        # frame is a memoryview, no copy

# async variant
async for frame in sub.aiter():
    await process(frame)

# context manager
with bus.subscriber("video-frames") as sub:
    frame = sub.receive(timeout=1.0)
```

---

## File Layout on Disk

```
/tmp/mmbus/
└── my-app/
    ├── ring.mmap          # the shared memory ring buffer file
    ├── ring.lock          # flock-based producer lock
    └── signal.sock        # Unix domain socket for wakeup signals
```

Location is configurable; defaults to `/tmp/mmbus/<name>/` on Linux/macOS.

---

## Platform Support

| Platform | Status | Transport |
|----------|--------|-----------|
| Linux x86_64 | Primary | eventfd + mmap |
| Linux aarch64 | Primary | eventfd + mmap |
| macOS arm64 | Primary | Unix socket + mmap |
| macOS x86_64 | Primary | Unix socket + mmap |
| Windows 10 1803+ | Planned | Named pipe + mmap |
| Windows < 1803 | Not supported | — |

---

## Build & Distribution

- **Rust core + PyO3**: built with `maturin`
- **Wheels**: published to PyPI for all primary platforms via GitHub Actions +
  `manylinux` Docker containers for Linux
- **User install**: `pip install mmbus` — downloads pre-built wheel, no Rust
  toolchain required
- **Developer install**: `maturin develop` (requires Rust toolchain)

### CI matrix

```yaml
targets:
  - x86_64-unknown-linux-gnu    (manylinux2014)
  - aarch64-unknown-linux-gnu   (manylinux2014)
  - x86_64-apple-darwin
  - aarch64-apple-darwin
```
