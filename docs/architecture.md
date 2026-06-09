# Architecture

This is the as-built architecture of mmbus 0.1.0 (wire format v4).  It
matches the code in `crates/mmbus/src/`; the design predecessors and rationale live
in `docs/research.md` and the RFCs.

## Layers

```
┌──────────────────────────────────────────────────┐
│  Python public API  (python/mmbus/__init__.py)   │
│  - Bus, Subscription, AsyncSubscription,         │
│    AnyioSubscription, TopicStats                 │
│  - asyncio integration via loop.add_reader       │
│  - anyio integration via to_thread (lazy import) │
├──────────────────────────────────────────────────┤
│  PyO3 bindings  (src/python/)                    │
│  - _RustBus, Subscription, TopicStats, errors    │
│  - py.allow_threads() around every blocking call │
│  - Bytes copy out of ring into PyBytes per recv  │
├──────────────────────────────────────────────────┤
│  Rust core  (src/)                               │
│  - mmap lifecycle (memmap2, file-backed)         │
│  - SPMC ring buffer with per-slot seqlock        │
│  - AtomicU64 tail, per-subscriber cursor table,  │
│    generation counter (header v4)                │
│  - eventfd (Linux) or AF_UNIX (macOS) wakeup     │
│  - SCM_RIGHTS fd-passing handshake (Linux)       │
│  - flock-based single-publisher invariant        │
└──────────────────────────────────────────────────┘
```

The data path stops at the Rust core: every payload lives in exactly one
place (the mmap-backed file) until a Python `recv()` call copies it into
a `PyBytes`.  A buffer-protocol / `memoryview` path that avoids this
final copy is on the roadmap but requires pinning slots against
publisher overwrite — see `CHANGELOG.md` "Known gaps".

---

## Data path

```
Publisher process                        Subscriber process
─────────────────                        ──────────────────
bus.publish(topic, data)                 sub = bus.subscribe(topic)
  │                                        │
  ▼                                        ▼
RingBuffer::write_slot(tail, data)       claim a free cursor slot
  1. seq[idx] = tail|WRITING (Release)     in the per-topic ring,
  2. write len, write payload              connect to publisher's
  3. seq[idx] = tail        (Release)      AF_UNIX handshake socket,
  4. tail = tail + 1        (Release)      receive wakeup fd via
                                           SCM_RIGHTS (Linux only;
  send 1 byte to wakeup fd ──────────►     macOS uses the same socket
  (per-subscriber)                         for byte wakeups).
                                           │
                                           ▼
                                         try_receive() reads slot
                                         in-place out of the mmap
                                         (one copy into the caller's
                                         out: Vec<u8>; from Python
                                         that becomes a PyBytes).
                                         Cursor stored back in the
                                         shared cursor table.
```

The mmap region is the **only** place payload data lives.  The wakeup
channel carries a single byte; no message data ever crosses the kernel
boundary as IPC payload.

---

## Ring buffer design (wire format v4)

```
┌────────────────────────────────────────────────────────┐
│  Header (64 B, mmap offset 0..64)                      │
│    u64  magic         (0x6D6D627573000004)             │
│    u32  version       (= 4)                            │
│    u32  capacity      (slot count)                     │
│    u32  slot_payload_size                              │
│    u32  max_subscribers                                │
│    u64  generation    (AtomicU64, bumped on restart)   │
│    u64  tail          (AtomicU64, producer cursor)     │
│    u24  _pad to 64-byte cache line                     │
├────────────────────────────────────────────────────────┤
│  Subscriber cursor table  (8*max_subscribers bytes,    │
│    offset 64 onwards)                                  │
│    cursor[i] : AtomicU64                               │
│      CURSOR_UNCLAIMED (u64::MAX) = slot free           │
│      any other value = subscriber's next-read position │
├────────────────────────────────────────────────────────┤
│  Slots [0 .. capacity-1]  (slot_stride bytes each)     │
│    u64  seq      (AtomicU64; high bit = WRITING flag)  │
│    u32  len                                            │
│    [u8] payload  (slot_payload_size bytes, zero-pad)   │
└────────────────────────────────────────────────────────┘
```

### Invariants

- **SPMC**: one publisher, many subscribers.  Single-publisher is
  enforced at the FS boundary by `flock(LOCK_EX|LOCK_NB)` on a
  per-topic lockfile (`crates/mmbus/src/producer_lock.rs`).
- **Tail-advance rule**: the producer may only advance `tail` while
  `tail - min(active_cursors) < capacity` (default `Error` policy).
  Under `DropOldest`, the producer skips that check; subscribers detect
  overwrite via the seq field.
- **Per-subscriber cursors**: each subscriber atomically updates its
  own slot in the cursor table after every successful read.  No shared
  consumer "head" exists — fan-out is O(1) per subscriber.

### Per-slot seqlock (the v4 contribution)

Without a seqlock, a slow reader under sustained `DropOldest` can
observe a torn slot: the publisher rewrites len + payload faster than
the reader can copy them out.  Solution:

- **Publisher write order**: `seq.store(tail | WRITING, Release)` →
  write `len` and `payload` (non-atomic) → `seq.store(tail, Release)`.
  Bracketing the payload write with two Release stores gives readers a
  signal that "this slot is currently being written" and a separate
  commit point.
- **Subscriber read order**: Acquire-load `seq`; if `WRITING` bit set,
  spin-retry.  Otherwise read len + payload, then Acquire-load `seq`
  again; if different from the first load, retry (the slot was
  overwritten between the two loads).  After a successful read, store
  `cursor = effective + 1` so the publisher can free the slot.

This is the standard seqlock pattern, adapted to a wrap-around ring:
the `seq` value is the *write generation* (`tail` at the time of
write), so a subscriber that's been overrun discovers it by seeing a
seq value larger than its cursor and can skip forward.

The `WRITING` flag is bit 63 of the `seq` u64.  At 1 G publishes/s, a
tail counter would take ~292 years to roll over bit 63, so reusing it
as a flag is sound.

---

## Signalling (wakeup)

| Platform | Mechanism | Notes |
|----------|-----------|-------|
| Linux    | `eventfd(EFD_SEMAPHORE)` | one fd per subscriber, passed publisher → subscriber via `SCM_RIGHTS` on a per-topic AF_UNIX socket |
| macOS    | AF_UNIX byte-wakeup       | the same socket carries 1-byte wakeups (no eventfd equivalent on Darwin) |

Subscribers `read()` / `poll()` the wakeup fd from a thread with the
GIL released (`py.allow_threads`).  The asyncio path
(`AsyncSubscription`) registers the fd with `loop.add_reader` and
returns control to the event loop instead of blocking a worker thread.

Disconnect detection: on Linux, a second `add_reader` on the handshake
socket signals POLLHUP when the publisher dies; on macOS the wakeup
read returns 0 bytes (EOF) which the iterator treats as end-of-stream.

---

## Crash safety

mmbus is process-crash-safe (publisher or subscriber may die at any
point) without requiring a startup recovery scan.  The mechanisms:

1. **No `ftruncate` on publisher restart.**  `RingBuffer::create_or_reuse`
   opens an existing compatible ring and bumps its `generation` counter
   instead of truncating, so a subscriber whose mmap was made before
   the restart never SIGBUSes on read.
2. **Generation counter.**  After a restart, existing subscribers wake
   on the next message, observe the bumped `generation`, and terminate
   cleanly with `UnexpectedEof` instead of reading from the
   logically-reset ring.
3. **Per-slot seqlock.**  A subscriber that resumes mid-publish (e.g.
   after a wakeup race) detects torn / overwritten reads via the seq
   field and retries or skips forward, rather than returning garbage.
4. **flock-based publisher exclusion.**  Only one process at a time can
   hold the publisher lock for a given topic; a second publisher fails
   fast with `AlreadyPublishing` rather than corrupting the ring.
5. **Subscriber cursor self-release.**  `release_cursor` runs in
   `Drop` for `Subscription`; an orphaned cursor is force-released the
   next time the publisher restarts (because the bumped `generation`
   makes the old subscriber tear down its mmap and `Drop` runs).

The `dirty`-flag scheme described in earlier design drafts was
replaced by the per-slot seqlock — see `CHANGELOG.md` entry for v4.

---

## File layout on disk

```
/tmp/mmbus/<bus-name>/
  └── <topic>/
       ├── ring.mmap       # the shared memory ring buffer file
       ├── ring.lock       # flock-based publisher exclusion
       └── signal.sock     # AF_UNIX handshake + wakeup socket
```

`base_dir` is overridable via `Bus(name, base_dir=...)`; the default is
`/tmp/mmbus/`.  `clean_topic(topic)` removes the entire `<topic>/`
subdirectory and refuses if a publisher is active.

---

## Why Rust over pure Python

| Concern                       | Pure Python                     | Rust + PyO3 |
|-------------------------------|---------------------------------|-------------|
| Atomic ring ops               | `ctypes` hacks, unsafe          | `AtomicU64` with Acquire/Release ordering |
| GIL during subscriber wait    | Held (blocks other threads)     | Released via `py.allow_threads()` |
| mmap lifecycle                | `__del__` unreliable            | `Drop` trait, guaranteed cleanup |
| Crash safety invariants       | Runtime checks only             | Compiler-enforced + runtime checks |
| Cross-platform wakeup         | `selectors` over fragile fds    | `cfg(target_os)` per-platform code |

---

## Build & distribution

- **Rust core + PyO3**: built with `maturin`.
- **Wheels**: built per (os × target) by `.github/workflows/wheels.yml`
  on tag push (`v*`), published to PyPI via OIDC trust.
- **End-user install**: `pip install mmbus` — pre-built wheel, no Rust
  toolchain required.
- **Developer install**: `maturin develop --features python` (needs
  Rust stable).

### Build matrix

| Target                       | Wheel | Tested in CI |
|------------------------------|-------|--------------|
| x86_64-unknown-linux-gnu     | yes   | yes (ci.yml + Docker) |
| aarch64-unknown-linux-gnu    | yes   | (build only) |
| x86_64-apple-darwin          | yes   | yes (ci.yml) |
| aarch64-apple-darwin         | yes   | yes (ci.yml) |
| Windows                      | no    | — (see `docs/rfc-windows.md`) |
