# RFC: WAL v2 — lock-free mmap-backed journal

**Status:** Draft (proposed for v0.2.0).

**Owner:** _unassigned_

**Supersedes (partial):** the BufWriter-backed implementation of
`mmbus::wal` shipped in v0.1.x.  The on-disk segment format from
`rfc-wal-phase-b.md` (length-prefixed CRC-tagged records, segment
header) is preserved bit-for-bit so v0.1 segments remain readable.

---

## 1. Why

v0.1's WAL ships durable replay but at +41% overhead for
`wal=None` and +244% for `wal=Batched` over the no-WAL ring.  Two
costs dominate:

1. **Mutex on every publish.**  Publisher and flusher contend on a
   `std::sync::Mutex<Inner>` that wraps the `BufWriter` and the
   segment index.
2. **BufWriter copy + syscall.**  `write_all` memcpy's into a user-
   space buffer; periodic flushes call `write(2)` to push into the
   kernel page cache.

For the niche we're targeting — "Kafka-style durable pub/sub at
ring speed on one machine" — we need the WAL publish path to look
like the ring buffer's: a single atomic `fetch_add` on a tail
counter, a memcpy into a memory-mapped slot, an atomic `store` of
the seqlock commit value.  No mutex, no syscall.

Existing solution that demonstrates this is shippable: Chronicle
Queue (Java).  Sub-microsecond persistent enqueue.  There's no
Rust+Python equivalent today.

## 2. Goals & non-goals

### Goals

- **Publish latency target:** ≤ +10% vs the no-WAL ring under
  `wal=Batched`.  That's ≤ ~195 ns / publish on the current bench
  rig (32 B payload, capacity 4096, macOS 25.4 APFS).
- **Crash-safe at-least-once delivery** under publisher process
  death (kill -9, segfault, panic, OOM) — same guarantee as v0.1
  Batched.
- **Power-loss durability** under `wal=Each` and within one
  `fsync_interval` under `wal=Batched`.
- **Wire-compatible** with v0.1 segment files.  A v0.1 WAL can be
  read by v0.2; a v0.2 segment is readable by v0.1's reader.
- **Cross-platform:** Linux + macOS + Windows.  Per-platform
  durability primitives, single core abstraction.

### Non-goals

- Network distribution (covered by `rfc-multi-machine.md` and the
  shipped bridge in `crates/mmbus-bridge`).
- Multi-publisher per topic.  Single-publisher remains a hard
  invariant (the same `producer.lock` continues to enforce it).
- Replacing the in-memory ring.  The ring is still the live path
  for current subscribers; the WAL is the catch-up + crash-recovery
  path.
- Schema-aware records.  Records remain opaque bytes.

## 3. Architecture

```
                      ┌────────────────────────────────┐
   Publisher ─publish→│  active segment (mmap'd RW)    │←─── readers
   (single)           │  [header][rec][rec][rec] ...   │     (mmap RO)
                      │   ▲                            │
                      │   │ pub appends here           │
                      │   │ readers read up to here    │
                      └───┼────────────────────────────┘
                          │
                          │ tail = AtomicU64 in segment header
                          │ bracketed by per-record seqlock
                          │
                      ┌───┴───── background flusher thread ─────┐
                      │ every fsync_interval:                    │
                      │   msync(MS_ASYNC) on active mmap         │
                      │   advance durable_cursor atomic          │
                      └──────────────────────────────────────────┘

  Rotation: when tail + max_record_size > segment_size:
    write SKIP_TO_END marker at current tail
    create + ftruncate + mmap new segment file
    bump active_segment_first_cursor in coordination header
    old segment stays mmap'd until last reader releases it
```

### 3.1 Segment file layout

Bit-for-bit identical to v0.1.x:

- Bytes 0–32: `SegmentHeader` (magic, version=1, first_cursor,
  created_unix_nanos).
- Bytes 32–`segment_size_max`: append-only record region.
- Record framing: `[u32 record_len][u64 cursor][u64 ts][u32
  payload_len][payload...][u32 crc32c]` (28 B framing + payload).

What changes for v2:

- The segment file is `ftruncate`'d to `segment_size_max` at
  creation, not grown incrementally.
- The publisher writes via mmap (`MmapMut`), not `BufWriter<File>`.
- A `tail: AtomicU64` is added at byte offset 24 in the segment
  header — a v0.1 reader sees this as part of the
  `created_unix_nanos` field but never reads it past parsing time,
  so the format stays compatible.  We will use a fresh 8 B reserved
  field if v0.1 turns out to need backporting.

### 3.2 Lock-free publish

```rust
fn publish(&self, payload: &[u8]) -> Result<u64, WalError> {
    let record_len = RECORD_FRAMING + payload.len();

    // Step 1: reserve space by bumping tail.  This is the ONLY
    // synchronisation point with other potential writers (we have
    // none — SPMC — so the fetch_add is uncontended).
    let offset = self.tail.fetch_add(record_len as u64, Ordering::AcqRel);

    // Step 2: rotation check.  If we'd overrun the segment, write
    // SKIP_TO_END at our offset and rotate.
    if offset + record_len as u64 > self.segment_size {
        return self.rotate_and_retry(payload);
    }

    // Step 3: bracketed seqlock write.  The first u32 doubles as
    // record_len AND a WRITING flag (high bit set = in-flight).
    let slot = self.mmap_ptr.add(offset);
    write_atomic_u32(slot, (record_len as u32) | WRITING_BIT);
    write_record_body(slot, cursor, ts, payload);
    let crc = crc32c(&slot[4..record_len - 4]);
    write_atomic_u32(slot + record_len - 4, crc);
    // Final commit: clear the WRITING bit.
    write_atomic_u32(slot, record_len as u32);

    Ok(cursor)
}
```

No mutex.  One `fetch_add`, one mmap memcpy, two `u32` atomic
stores.  Estimated cost: 30–50 ns + the memcpy (payload-dependent).

### 3.3 Lock-free read

Reader's per-record loop:

```rust
loop {
    let len_field = atomic_load_u32(slot);
    if len_field == 0 {
        // Slot not yet allocated — at the live tail.
        return Ok(None);  // caller waits + retries
    }
    if len_field & WRITING_BIT != 0 {
        spin_loop_hint();
        continue;          // publisher mid-write — retry
    }
    if len_field == SKIP_TO_END {
        return rotate_to_next_segment();
    }
    // Read body + CRC; verify.  CRC mismatch = corruption =
    // signal recovery.
}
```

The seqlock pattern matches the ring buffer's; subscribers
already know how to wait + retry on the WRITING bit.

### 3.4 Rotation

The hairy part.  Sequence:

1. Publisher detects `offset + record_len > segment_size`.
2. Publisher writes `SKIP_TO_END` (a u32 sentinel with WRITING_BIT
   clear and value `u32::MAX`) at `offset`.
3. Publisher creates new segment file
   `{next_cursor:020}.seg`, `ftruncate`'s to `segment_size_max`,
   `mmap`'s, writes segment header, updates `active_segment` in
   the coordination file (`wal/active.dat` — a 16-byte file with
   `[first_cursor: u64][segment_id: u64]`).
4. Publisher writes the actual record at the new segment's offset.
5. Old segment stays mmap'd in publisher's address space until
   the flusher has confirmed all dirty pages are written
   (`msync(MS_SYNC)` on the dying segment).

Readers detect rotation via `SKIP_TO_END`: they re-read the
coordination file, open the next segment, and continue.  Multi-
reader coordination is just "open file, mmap RO, read."  Old
segments are kept on disk until retention deletes them; their
mmaps in reader processes stay valid until the reader closes
them.

### 3.5 Durability

Per-platform flushers:

| Platform | Background flush (Batched) | Inline flush (Each)            |
|----------|----------------------------|---------------------------------|
| Linux    | `msync(MS_ASYNC)`          | `msync(MS_SYNC)` + `fdatasync`  |
| macOS    | `msync(MS_ASYNC)`          | `msync(MS_SYNC)` + `fcntl(F_FULLFSYNC)` |
| Windows  | `FlushViewOfFile`          | `FlushViewOfFile` + `FlushFileBuffers` |

`msync(MS_ASYNC)` is cheap — kicks the kernel writeback queue
without waiting.  This is what gives Batched its target overhead.
The publisher path never calls msync.

### 3.6 Recovery

On `Wal::open`:

1. Scan `wal/*.seg` in cursor order.
2. For each segment, walk records forward (same loop as today).
3. The first record with `WRITING_BIT` set, CRC mismatch, or
   `len_field == 0` past the segment header marks the end of the
   intact log.  `ftruncate` the segment there.
4. Active segment is whichever holds the highest intact cursor +
   has room left; otherwise rotate to a fresh one.

Identical semantics to v0.1's `recover_truncate`; the WRITING_BIT
check is new (an in-flight write at crash time looks like a torn
record, which is exactly how we'd want to handle it).

## 4. Wire compatibility

- v0.1 segment files open identically — the format is the same.
- v0.2 segment files open in a v0.1 reader as long as v0.2 doesn't
  push the tail field into a bit a v0.1 reader requires zero.  We
  put it in `created_unix_nanos` initially because v0.1 readers
  load that field but never act on it past header parsing; this
  works in practice but the safer move is a brand-new reserved
  field (see §3.1 follow-up).
- The Python API surface (`Bus`, `Subscriber`, `WalConfig`) is
  unchanged.

## 5. Performance targets

Bench rig: 32 B payload, capacity 4096, macOS 25.4 APFS, 10-sample
3 s windows.

| Policy        | v0.1 today | v0.2 target | v0.2 floor (likely) |
|---------------|-----------:|------------:|--------------------:|
| no WAL        |    176 ns  |     176 ns  |              176 ns |
| `wal=None`    |    248 ns  |   ≤ 195 ns  |              ~150 ns |
| `wal=Batched` |    606 ns  |   ≤ 195 ns  |              ~180 ns |
| `wal=Each`    |    3.6 ms  | 3.6 ms (fsync-bound) |        3.6 ms |

If `wal=Batched` lands under the +10% gate at acceptance time
(`WalConfig::default().enabled` flips to true; the WAL becomes the
default behaviour in v0.2.0).

## 6. Risks & mitigations

| Risk                                    | Mitigation                                    |
|-----------------------------------------|-----------------------------------------------|
| Pre-allocated 64 MiB segments waste disk on low-traffic topics | `WalConfig::segment_size_max` is already configurable; default could drop to 4 MiB for v0.2 |
| Cross-platform mmap durability nuances  | Per-platform flusher module; explicit acceptance test per platform |
| Reader lifecycle vs retention deletion  | Old segments deletable only after `msync(MS_SYNC)`; readers re-validate on `SKIP_TO_END` |
| Seqlock starvation under torn publish   | Bounded retry count (16, matching ring); on exhaustion treat as drop |
| `ftruncate(64 MiB)` perceived as "disk full" by ops dashboards | Document; ship `du -sh` example in runbook |
| Bug in lock-free path corrupts segments | Ship v2 behind a feature flag (`wal_v2`) for one release; default to v0.1 path; promote in v0.2.1 after burn-in |

## 7. Open questions

1. **Tail in header vs in a sidecar file.**  Putting `tail` in
   the segment header means readers see writes the instant the
   `fetch_add` returns — but v0.1 readers will interpret those
   bytes as `created_unix_nanos`.  A sidecar `tail.atomic` file
   (mmap'd shared) avoids the compat risk but adds an extra mmap.
   Lean: sidecar, with a `format_version` bump in the segment
   header to gate the v2 reader path.
2. **Single segment_size for both directions.**  v0.1 uses one
   size; v0.2 could shrink to 4 MiB to reduce disk waste.  Lean:
   keep 64 MiB default, document `segment_size_max = 4 * 1024 *
   1024` for low-traffic buses.
3. **Replace `WalConfig::fsync_policy::None`?**  With mmap, "no
   fsync" is the natural default (`msync` only happens on Batched
   or Each).  `None` becomes the cheapest, Batched is +1 cheap
   syscall per `fsync_interval`.  Keep all three for parity.

## 8. Implementation staging

See `docs/plan-wal-v2-lockfree.md` for the task-template
decomposition.  Two-line summary:

```
W2-0 RFC + scaffold        W2-5 Publisher integration
W2-1 mmap segment writer   W2-6 Subscriber integration
W2-2 mmap segment reader   W2-7 Per-platform flushers
W2-3 Rotation              W2-8 Acceptance + perf + default flip
W2-4 Lock-free aggregator
```

Behind `wal_v2` Cargo feature for the first release; default
promotion in v0.2.1.
