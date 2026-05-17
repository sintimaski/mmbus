# RFC: WAL Phase B — durable replay

**Status:** Shipped (W1-0 through W1-f).  Opt-in via
`BusConfig::wal`; default remains `WalConfig::disabled()` pending the
perf optimisation pass (see "Results" below).

**Owner:** _unassigned_

## Results (W1-f bench, 2026-05-17)

`benches/publish_with_wal.rs`, 32 B payload, capacity 4096, macOS
25.4 APFS, 10-sample / 2s measurement window:

| Policy        | ns/publish | Overhead vs baseline |
|---------------|-----------:|---------------------:|
| no WAL        |        185 |                    — |
| `wal=None`    |        275 |                 +49% |
| `wal=Batched` |        767 |                +315% |
| `wal=Each`    |  3,700,000 | catastrophic (fsync) |

`Batched` overshoots the planned <10% regression gate, so the
default-policy flip is deferred.  Investigation candidates:

1. `SystemTime::now()` per publish — replace with a coarse clock
   or make the timestamp publisher-supplied.
2. Mutex contention between `publish()` and the flusher thread —
   the writer's `BufWriter::write_all` happens under the same lock
   the flusher acquires every `fsync_interval`.
3. A lock-free SPSC ring between publisher and a dedicated WAL
   writer thread.

Acceptance tests (`tests/wal_acceptance.rs`, 7 scenarios) all pass
under every policy.

**Relationship to Phase A:** The Phase A API surface
(`Bus::subscribe_from(topic, cursor)`) is forward-compatible with
Phase B — Phase B extends the meaning of `cursor` from "ring position"
to "WAL position that may be older than the ring".  No new public API
should be needed for the v1 WAL deliverable; existing
`CursorTooOldError` becomes "older than the configured WAL retention"
instead of "older than the ring".

---

## 1. Goals & non-goals

### Goals

- **Late-joining subscribers** can replay messages older than what the
  ring still holds, up to the configured WAL retention window.
- **At-least-once delivery** under crash: a publish that returned `Ok`
  to the caller is durable (subject to the configured fsync policy)
  and replayable after a process or machine crash.
- **Bounded disk use**: rotation + retention keep the WAL from
  growing unbounded; operators set the cap.
- **No regression on the hot path**: callers who don't enable WAL pay
  zero overhead.  Callers who do enable WAL with `fsync_policy =
  "batched"` pay one extra background-thread append per publish, no
  added syscall on the publish path itself.

### Non-goals (this RFC)

- **Cross-bus replay** — handled by `rfc-multi-machine.md`.
- **Exactly-once delivery** — WAL gives at-least-once; consumers
  remain responsible for idempotency.
- **Encrypted at-rest WAL** — separate concern; the WAL file can be
  placed on an encrypted filesystem if needed.
- **Multi-publisher WAL** — mmbus is SPMC by design; the WAL inherits
  that.  A future MPSC ring would need a re-think.
- **Cross-machine WAL replication** — the bridge (`mmbus-bridge`) is
  the federation story; WAL is per-machine durability.

---

## 2. Design space recap

From `rfc-wal-replay.md` §2.B, the open trade-offs were:

| Question | Resolved here |
|----------|----------|
| File format | length-prefixed records + per-record CRC32C; segment magic header (§4) |
| Fsync policy | three modes: `none` / `batched` (default) / `each` (§5) |
| Rotation | size-based, default 64 MiB / segment (§6) |
| Retention | rolling window by total disk bytes; default 1 GiB (§7) |
| Index | in-memory `BTreeMap<cursor → segment+offset>` rebuilt on open (§8) |
| WAL ↔ ring handoff race | WAL write happens BEFORE ring publish, both stamped with the same `tail` value (§9) |
| Crash semantics | torn-write detection via CRC + segment-tail truncation on open (§10) |

The rest of this RFC fleshes out each of those.

---

## 3. Lifecycle overview

```
Publish hot path:
  publisher → write_record(WAL)         ← payload+meta serialized to RAM buffer
            → fsync (policy-dependent)  ← may be deferred to background flusher
            → ring.publish(payload)     ← visible to live subscribers
            → return Ok to caller

Subscribe path (subscribe_from):
  if cursor within ring   → claim ring cursor, normal SPMC reads
  elif cursor within WAL  → spin up a WAL replayer that feeds the
                            subscriber from disk until it catches up,
                            then hands off to the live ring
  else                    → Error::CursorTooOld { requested, oldest }

Background flusher (when fsync_policy = batched):
  every fsync_interval_ms or every fsync_batch_bytes (whichever first):
    fsync(active_segment_fd)
    update durable_cursor in a separate small file ("cursor.sync")

Background rotator:
  when active_segment.size >= segment_size_max:
    fsync + close active segment
    open segment[N+1]
  when total_wal_bytes > retention_bytes:
    delete oldest segment
```

---

## 4. On-disk format

```text
WAL file layout (per topic):

  base_dir/<bus>/<topic>/
    ring.mmap
    signal.sock
    producer.lock
    wal/
      cursor.sync          ← latest durable cursor (u64 LE + CRC32C)
      0000000000.seg       ← segment file, name = first cursor it contains
      0000004223.seg
      0000009810.seg       ← active (most recently appended)


Segment file:

   ┌──────────────────────────────────────────────┐
   │  Segment header (32 B, written on create)    │
   │    u64  magic    = 0x6D6D6275735741_4C01     │  "mmbusWAL" + v1
   │    u32  version  = 1                         │
   │    u32  reserved = 0                         │
   │    u64  first_cursor                         │  first record's cursor
   │    u64  created_unix_nanos                   │
   └──────────────────────────────────────────────┘
   ┌──────────────────────────────────────────────┐
   │  Record 0  (length-prefixed, CRC32C-tagged)  │
   │    u32  record_len  (== len(payload) + 28)   │
   │    u64  cursor                               │
   │    u64  ts_unix_nanos                        │
   │    u32  payload_len                          │
   │    [u8] payload                              │
   │    u32  crc32c       (over cursor..payload)  │
   └──────────────────────────────────────────────┘
   ┌── Record 1, Record 2, …  appended in order ──┐
```

### Field rationale

- **Outer `record_len` prefix** lets a reader skip a corrupt record
  without parsing its body — important for cheap segment-scan when
  building the in-memory index on open.
- **`cursor` per record** is the `tail` value the publisher used at
  the time of write; matches the ring's seq exactly so a subscriber
  transitioning from WAL → ring uses the same cursor space.  Cursors
  are monotonic per (publisher generation, topic); a generation bump
  starts a new segment (§9.2).
- **`ts_unix_nanos`** enables a future "replay since timestamp" API
  without a wire-format change; currently unused but free to record.
- **`payload_len`** is redundant with `record_len` (just `record_len
  - 28`) but eliminates one arithmetic step on every record read.
- **`crc32c`** on the body (cursor..payload) detects torn writes.
  CRC32C (Castagnoli polynomial, hardware-accelerated on x86_64
  via `SSE4.2` and on aarch64 via `CRC` instructions) is fast — a
  hot-path WAL must not bottleneck on CRC.

### Endianness

All multi-byte fields are **little-endian**.  Matches the ring header
+ bridge wire format; one less surprise.

---

## 5. fsync policy

Three modes, selectable per-bus (default `batched`):

| Mode      | Per-publish cost        | Durability on crash       | Use case |
|-----------|-------------------------|---------------------------|----------|
| `none`    | append to OS page cache | last few seconds may be lost | dev, tests |
| `batched` | append + signal flusher | last `fsync_interval_ms` may be lost (default 5 ms) | most prod |
| `each`    | append + fsync          | nothing lost since the most recent successful publish | financial / audit |

### Batched policy detail

Publishers append the record to the segment file's `write()` buffer
(buffered I/O) and update an in-memory `pending_cursor` watermark.  A
single background flusher thread fires every `fsync_interval_ms` *or*
whenever `pending_bytes >= fsync_batch_bytes`, calls
`fsync(segment_fd)`, and updates `cursor.sync` to the new
`durable_cursor`.

Subscribers requesting `subscribe_from(cursor)` are clamped to
`durable_cursor` at connect time — they cannot read records that
might still be lost on crash.  Once `durable_cursor` advances, the
subscriber's `recv()` unblocks naturally.

### `each` policy detail

Publish calls `write()` then `fsync()` inline.  No flusher thread.
`durable_cursor = pending_cursor` always.  Slowest mode but the
strictest durability guarantee.

### `none` policy detail

No flusher, no `cursor.sync` file.  `durable_cursor` is always
`pending_cursor`.  On crash, the OS may have dropped recent writes;
late subscribers won't observe them.  Acceptable for "WAL as a debug
log" deployments.

---

## 6. Rotation

A segment is *closed* (no further appends) when either:

- `segment.size >= segment_size_max` (default 64 MiB), or
- the publisher detects a fresh generation (i.e. itself just
  restarted; see §9.2).

Rotation steps (on the hot path; ~one syscall pair):

1. `fsync(active_segment_fd)` (only if fsync_policy != `each`; in
   `each` mode it's already durable)
2. close the active segment
3. open a new segment file named after the next cursor
4. write the 32-byte segment header

The publish that triggered rotation is appended to the **new**
segment.  This keeps each segment self-contained — no record straddles
two files.

---

## 7. Retention

A background rotator thread enforces `retention_bytes` (default 1
GiB).  When `sum(segment.size for all segments) > retention_bytes`,
the rotator deletes the **oldest** segment (lowest `first_cursor`).
Subscribers replaying through that segment must be far enough behind
that they've already moved past it — if a replayer is mid-segment when
deletion races, the next read returns `Error::CursorTooOld` (the
subscriber's caller decides whether to retry from `tail`, fail, or
wait).

Alternative policies considered + rejected:

- **By time** (`retention_duration`): requires reading every record's
  `ts_unix_nanos` to find the cutoff; size-based is O(stat-segment).
  Defer to a future enhancement.
- **By message count**: same problem as time; size is the
  operationally-meaningful cap (disk pressure).

Both could be added later as additive config knobs without breaking
the size-based default.

---

## 8. Index

Subscribers requesting `subscribe_from(cursor)` need O(log N) lookup
from `cursor → (segment_id, byte_offset)`.  We rebuild this in-memory
on open by:

1. listing segment files (already sorted by `first_cursor` since the
   filename is the cursor)
2. for each segment, recording `(first_cursor, segment_path)` in a
   `BTreeMap<u64, PathBuf>`

Within a segment, `cursor → offset` is computed by scanning forward
from the segment header — segments are bounded at 64 MiB so the
worst-case scan is a few thousand records, well under 1 ms.

For very large WALs we may add a per-segment sparse index file later
(`<cursor>.seg.idx` with every 4096-th record's offset), but the v1
design is "scan the segment".

**Open question**: should the index include `ts_unix_nanos` so a
future timestamp-based subscribe doesn't need to scan?  Probably yes
once the timestamp API exists.  For v1, no.

---

## 9. Ring ↔ WAL handoff

The core correctness invariant: a subscriber that opens
`subscribe_from(cursor)` and is fed records from WAL must eventually
catch up to the live ring without missing or duplicating any cursor.

### 9.1 Write ordering

Publishers do **WAL write → ring write → return Ok**, both stamped
with the same cursor:

```rust
fn publish(&mut self, data: &[u8]) -> Result<()> {
    let cursor = self.ring.next_tail();    // peek without advancing
    self.wal.append(cursor, data)?;        // record durably (per fsync_policy)
    self.ring.publish(data);               // advances ring.tail to cursor+1
    Ok(())
}
```

If the WAL append fails, the ring is not advanced; the publish
returns the WAL error.  If the WAL succeeds but the ring publish
panics (the ring panics on out-of-bounds slots; otherwise it returns
`Err`), we have a WAL record with no corresponding live wakeup — a
late subscriber would still see it on replay, but live subscribers
miss it.  Documented as "live and WAL agree only when publish returns
Ok".

### 9.2 Generation handling

When a publisher restarts, mmbus bumps the header `generation`
counter (today: stale subscribers see the bump on next wakeup and
terminate).  For WAL: the new publisher must start writing to a
**fresh segment** (don't append to the segment the dead publisher was
mid-writing — its tail might be torn).  The handoff:

1. On `Publisher::create_or_reuse`, scan the existing WAL.
2. Find the highest cursor with an intact CRC (per §10 scan).
3. The new publisher's tail starts from `max(highest_wal_cursor + 1,
   ring.current_tail() + 1)`.
4. Open a new segment file named after that cursor.
5. Bump `generation` in the ring header as usual.

This means a WAL replayer that observed cursor C from the old
generation can transition cleanly to the new generation's records at
cursor C+k.  The cursor space is contiguous across generations — the
generation counter is opaque to subscribers using `subscribe_from`.

### 9.3 Subscriber handoff

A `subscribe_from(cursor)` subscriber:

1. Computes `oldest_wal_cursor = first segment's first_cursor`.
2. If `cursor < oldest_wal_cursor`: `Error::CursorTooOld`.
3. If `cursor < ring.oldest_cursor()`: WAL-mode.  Spawn a replayer
   that reads records from disk and pushes them through a
   `Subscription` shim until the replayed cursor catches up to
   `ring.oldest_cursor()`.
4. Then claim a real ring cursor at the caught-up position and
   transition to the normal SPMC path.

The transition is the subtle part.  Naive approach: replay everything
in the WAL, then ring.subscribe.  Race: by the time replay finishes,
the ring has advanced further.  Fix: re-resolve the ring tail after
the last WAL record, then continue from WAL into the gap, repeat
until WAL's last cursor ≥ ring's oldest cursor.  Then do a single
atomic handoff (claim cursor in ring at the right position).

```rust
loop {
    let last_wal = replay_wal_from(cursor)?;
    let ring_oldest = ring.current_oldest();
    if last_wal + 1 >= ring_oldest {
        // No gap — caught up.
        return ring.claim_at(last_wal + 1);
    }
    // Gap: WAL has been rotated beyond what we just read.
    cursor = last_wal + 1;
}
```

Bounded by the rotator's rate (we'll catch up within one rotation
cycle in steady state).

---

## 10. Crash semantics

On open, every WAL segment is validated by:

1. Read + validate the 32-byte segment header.
2. Linear scan: for each record, read `record_len`, read the body,
   compute CRC32C, compare.
3. On first CRC mismatch or short read: `ftruncate` the segment to
   the byte offset just before the bad record.  Log a WARN.

This handles:

- **Power loss mid-write**: the partially-written record fails CRC
  and is truncated away.
- **Filesystem corruption**: same outcome — bad records get dropped.
- **`cursor.sync` mismatch with actual segment tail**: trust the
  segment tail (which we just validated); rewrite `cursor.sync` to
  match.

The startup scan cost is bounded: at 64 MiB / segment and 1 GiB
default retention, that's 16 segments * 64 MiB = 1 GiB to scan,
which a sequential `read()` + CRC32C-hw does in ~50 ms on modern
hardware.  Acceptable for a publisher cold start.

---

## 11. Configuration surface

```rust
pub struct WalConfig {
    /// If None, no WAL is created.  This is the default — opt-in.
    pub enabled: bool,                          // default: false

    /// Per-record fsync policy.
    pub fsync_policy: FsyncPolicy,              // default: Batched
    pub fsync_interval: Duration,               // default: 5ms (Batched only)
    pub fsync_batch_bytes: usize,               // default: 1 MiB (Batched only)

    /// Rotation.
    pub segment_size_max: usize,                // default: 64 MiB

    /// Retention.
    pub retention_bytes: u64,                   // default: 1 GiB
}

pub enum FsyncPolicy { None, Batched, Each }
```

The Python wrapper extends `Bus(name, wal="batched")` (or
`wal=False`/`wal={...}`).  Sensible string-or-dict shape; reject
unknown strings at construction.

---

## 12. Observability

New `Bus::stats` fields when WAL enabled:

```rust
pub struct WalStats {
    pub pending_cursor: u64,    // last record written (may not be durable)
    pub durable_cursor: u64,    // last record fsynced
    pub oldest_cursor: u64,     // first cursor still on disk
    pub active_segment_bytes: u64,
    pub total_wal_bytes: u64,
    pub segments: usize,
}
```

Plus per-publish counters (Prometheus-style if/when we add a metrics
backend):

- `mmbus_wal_appends_total`
- `mmbus_wal_fsyncs_total`
- `mmbus_wal_torn_records_total` (incremented during recovery scan)
- `mmbus_wal_replay_starts_total`

---

## 13. Open questions

- **`fdatasync` vs `fsync`**: `fdatasync` skips the inode-metadata
  flush (mtime), which we don't care about for our segment files.
  Worth ~30% speedup on some filesystems.  Probably yes; benchmark
  in v1 implementation.
- **`io_uring` for batched appends**: would let one syscall queue
  many writes + a single fsync.  Linux-only.  Defer to a future
  optimisation pass; the v1 spec is portable.
- **Per-topic vs per-bus WAL**: this RFC assumes per-topic
  (`base_dir/<bus>/<topic>/wal/`).  Per-bus would be one WAL stream
  shared across topics + an extra `topic_id` field in each record.
  Per-topic is simpler operationally (rotate / retain independently
  per topic's traffic shape).  Lean: per-topic; matches the current
  ring file layout.
- **WAL on tmpfs**: a deployment that mounts `base_dir` on tmpfs
  defeats fsync's durability promise.  Document, don't try to detect.
- **Compaction**: should we ever rewrite a segment to drop records
  the subscriber has already advanced past?  Adds complexity (need to
  track per-subscriber durable-acknowledge, which subscribers don't
  send today).  Defer.

---

## 14. Implementation staging

Five reviewable PRs, in order:

| Stage | Scope |
|-------|-------|
| **W1a** | `wal::SegmentWriter` (single segment, append + CRC) + tests |
| **W1b** | `wal::SegmentReader` + recovery scan (CRC-detect + truncate) + tests |
| **W1c** | `wal::Wal` (writer + reader + rotation + retention + background flusher) + tests |
| **W1d** | `Publisher` integration: hot-path append, `create_or_reuse` recovery handoff (§9.2) |
| **W1e** | `Subscriber::connect_from(cursor)` integration: WAL replay → ring handoff (§9.3) + integration tests covering all three fsync policies |

Each stage is independently green; W1d is the first commit where
publish-time behaviour changes for WAL-enabled buses.

---

## 15. Acceptance criteria

For a `wal = "batched"` (default) bus:

- A subscriber that calls `Bus::subscribe_from(cursor)` where
  `cursor` is older than the ring but younger than the oldest WAL
  segment must receive **every** message from `cursor` onwards, in
  order, with no duplicates.
- A publisher restart preserves: any message whose publish returned
  Ok before the crash is observable to a `subscribe_from(0)`
  subscriber after the restart.
- On a power-loss simulation (`umount -f` of the WAL filesystem) the
  recovery scan must truncate cleanly — no panic, no read past the
  last good record.
- `cargo bench --bench publish_with_wal` shows < 10% throughput
  regression vs no-WAL `cargo bench --bench publish_no_wal` at the
  default `batched` settings (32 B payloads, 256-slot ring).

For `wal = "each"`:

- Same correctness as `batched`, plus: no message is lost on a
  publisher SIGKILL between successful publishes.

For `wal = "none"`:

- Same throughput as no-WAL.  WAL files exist but are not fsynced.

---

## 16. Why not just use SQLite?

Briefly considered.  Reasons against:

- **Hot path cost**: an INSERT per publish is multiple orders of
  magnitude slower than an `append + crc` (SQLite does WAL+BTREE+
  txn).  We'd need to batch writes and lose `each` semantics anyway.
- **Recovery surface**: SQLite is robust but the recovery surface is
  not specified by us — opaque dependency on SQLite's invariants.
- **Disk layout opacity**: a `.sqlite` file is harder for ops to
  inspect than `seg-NNN` files containing length-prefixed records.
- **Dep weight**: ~2 MB of compiled SQLite vs ~300 LOC of our own
  segment writer + reader.

The trade is "20× more code" for "100× faster + transparent layout +
no dep".  Reasonable.
