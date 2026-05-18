# WAL v2 — lock-free mmap implementation plan

Decomposition of `docs/rfc-wal-v2-lockfree.md` into reviewable
tasks using the universal template from `~/.claude/CLAUDE.md`.

Read order: RFC for design + trade-offs, this doc for execution
slicing.  Each task is sized for one or two reviewable commits.
Behind `wal_v2` Cargo feature for the entire v0.2.0 cycle; default
promotion in v0.2.1.

## Decomposition shape

Nine stages, mostly linear:

```
W2-0 (RFC + scaffold) ──► W2-1 (writer) ──► W2-2 (reader) ──► W2-3 (rotation)
                                                                    │
                                                                    ▼
                                                      W2-4 (Wal aggregator)
                                                                    │
                                                                    ▼
                              W2-7 (per-platform flushers) ────► W2-5 (Publisher)
                                                                    │
                                                                    ▼
                                                          W2-6 (Subscriber)
                                                                    │
                                                                    ▼
                                                W2-8 (acceptance + perf + flip)
```

W2-7 is parallelisable with W2-5; both block on W2-4.

---

### Task W2-0: RFC + scaffold + feature flag

- **Description:**
  Land `rfc-wal-v2-lockfree.md` (this RFC); add the `wal_v2` Cargo
  feature gating a new empty `src/wal/v2/` module tree; wire it
  into `src/wal/mod.rs` behind `#[cfg(feature = "wal_v2")]`.  No
  behavioural change.
- **Priority:** P1
- **Acceptance criteria:**
  - AC1: `cargo build --features wal_v2` succeeds; `cargo test`
    (default features) is unchanged.
  - AC2: `mmbus::wal::v2` is a publicly-reachable namespace when
    the feature is on, empty otherwise.
  - AC3: README + CHANGELOG note the experimental v0.2 path.
- **Inputs:** RFC §3.
- **Outputs:**
  - `docs/rfc-wal-v2-lockfree.md`, `docs/plan-wal-v2-lockfree.md`.
  - `Cargo.toml` gets `[features] wal_v2 = []`.
  - `src/wal/v2/mod.rs` placeholder.
- **Dependencies:** None.
- **Constraints:** Zero impact on v0.1.x path.
- **Steps:**
  1. Commit the RFC + plan docs.
  2. Add the `wal_v2` feature to `Cargo.toml`.
  3. Create `src/wal/v2/mod.rs` with module doc-comment.
- **Validation:**
  - Automated: `cargo build --features wal_v2`,
    `cargo test --features wal_v2`.
- **Idempotency:** Yes.
- **Status:** Todo

---

### Task W2-1: `wal::v2::MmapSegmentWriter` — lock-free append

- **Description:**
  Single-segment writer backed by `memmap2::MmapMut` over a
  pre-allocated file.  Append is `tail.fetch_add` + bracketed
  seqlock memcpy.  No mutex, no syscall on the hot path.
- **Priority:** P1
- **Acceptance criteria:**
  - AC1: `MmapSegmentWriter::create(path, segment_size, first_cursor)`
    pre-allocates (`ftruncate`), mmaps, writes the v0.1-compatible
    32 B header, initialises `tail` past the header.
  - AC2: `append(cursor, ts, payload) -> Option<u64>` returns
    `Some(byte_offset)` on success and `None` if the record would
    overrun the segment (caller rotates).
  - AC3: Format-compatible: a v0.1 `SegmentReader` reads back every
    record written by `MmapSegmentWriter`.
  - AC4: 10 unit tests covering: header on open, single record
    round-trip via v0.1 reader, multi-record round-trip, overrun
    returns `None`, in-flight WRITING bit is visible briefly,
    `fetch_add` is the only sync point.
- **Inputs:** RFC §3.1, §3.2.
- **Outputs:**
  - `src/wal/v2/mmap_segment_writer.rs`.
- **Dependencies:** W2-0.
- **Constraints:**
  - Append must do exactly one `fetch_add`, one memcpy, two atomic
    `u32` stores.  No allocations.
  - Reuses `wal::record::{encode_record_into, RECORD_FRAMING,
    SEGMENT_HEADER_LEN}` so the bit-layout stays in lockstep with
    v0.1.
- **Steps:**
  1. Add `MmapSegmentWriter` struct: `MmapMut`, base pointer,
     `tail: AtomicU64` (in-mmap), `segment_size`, `first_cursor`.
  2. `create()` — open + ftruncate + mmap + write header.
  3. `append()` — fetch_add, slot pointer math, bracketed write.
  4. Helper `write_atomic_u32` for the WRITING bit dance.
  5. Tests including a concurrent reader (v0.1 SegmentReader on
     the same file).
- **Error handling:**
  - Expected: ENOSPC on `ftruncate`, EIO on mmap.  Both surface
    via the constructor.  Append cannot fail (no syscall).
- **Validation:**
  - Automated: `cargo test --features wal_v2 wal::v2::mmap_segment_writer`.
  - Manual: write 1k records via v2, read via v0.1 reader; bytes
    match.
- **Idempotency:** Append is naturally append-only.
- **Status:** Todo
- **Ambiguity:**
  - Should `tail` live in the segment header or a sidecar?  RFC
    leans sidecar — implement that way unless bench shows the
    extra mmap is the bottleneck.

---

### Task W2-2: `wal::v2::MmapSegmentReader` — seqlock-aware reader

- **Description:**
  Mmap a v2 segment read-only and iterate records via the
  bracketed-seqlock protocol.  Distinguishes "not yet written"
  (len=0) from "in-flight" (WRITING bit set) from "torn"
  (CRC mismatch).
- **Priority:** P1
- **Acceptance criteria:**
  - AC1: `MmapSegmentReader::open(path)` mmaps RO, parses header.
  - AC2: `next_record()` returns `Some(Ok(Record))`, `Some(Err(_))`
    on corruption, or `None` if at the live tail (caller waits +
    retries).
  - AC3: Concurrent publisher writing the segment via the v2
    writer: reader sees every committed record in order, retries
    cleanly on observed WRITING bits.
  - AC4: 8 tests covering: empty segment yields None, multi-record
    walk, retry on WRITING bit, CRC mismatch reports error,
    SKIP_TO_END marker signals rotation, multi-threaded
    publisher×reader integrity test (1k records, no loss / dup).
- **Inputs:** RFC §3.3.
- **Outputs:**
  - `src/wal/v2/mmap_segment_reader.rs`.
- **Dependencies:** W2-1 (need a writer to produce test data).
- **Constraints:**
  - No mutation of the file.
  - Seqlock retry bounded at 16 (same as ring).
- **Steps:**
  1. Mmap RO + header parse.
  2. `next_record()` state machine.
  3. SKIP_TO_END detection (returns a distinct `Rotated` outcome).
  4. Tests including a concurrent publisher thread.
- **Error handling:**
  - CRC mismatch → return Err, caller decides (subscriber surfaces
    `CursorTooOld`; recovery scan truncates).
- **Validation:** `cargo test --features wal_v2 wal::v2::mmap_segment_reader`.
- **Idempotency:** Yes (read-only).
- **Status:** Todo

---

### Task W2-3: Rotation + coordination

- **Description:**
  Multi-segment writer that handles rollover when the active
  segment fills.  Atomically updates a coordination file
  (`wal/active.dat`) so readers know which segment is current.
  Writes a `SKIP_TO_END` marker at the dying segment's tail so
  in-flight readers chase to the next file cleanly.
- **Priority:** P1
- **Acceptance criteria:**
  - AC1: When `tail + record_len > segment_size`, the publisher
    writes `SKIP_TO_END`, mmaps a new segment, updates
    `active.dat`, and continues.
  - AC2: A reader observing `SKIP_TO_END` re-reads `active.dat`
    and opens the new segment without losing a record.
  - AC3: 5 tests: forced rotation by tiny `segment_size`, reader
    follows rotation, two consecutive rotations in one publish
    loop, retention deletes old segments while readers hold them
    (delete should be deferred or readers should keep mmap valid),
    crash mid-rotation leaves both files in a recoverable state.
- **Inputs:** RFC §3.4.
- **Outputs:**
  - `src/wal/v2/rotation.rs` (or fold into the writer / aggregator).
  - `wal/active.dat` format spec (16 B: `[active_first_cursor u64]
    [active_segment_id u64]`).
- **Dependencies:** W2-1 + W2-2.
- **Constraints:**
  - Single-writer (SPMC) — no inter-publisher rotation race; the
    `producer.lock` continues to enforce this.
  - Old segment must stay mmap'd in the writer's address space
    until the flusher confirms its dirty pages are written.
- **Steps:**
  1. `active.dat` writer + reader helpers.
  2. Rotation function: write SKIP marker, create new segment,
     update `active.dat`.
  3. Reader-side rotation handler.
  4. Tests.
- **Error handling:**
  - Mid-rotation crash: recovery picks the segment with the higher
    `first_cursor`; the old segment's SKIP_TO_END (if absent) is
    inferred from its byte count.
- **Validation:** `cargo test --features wal_v2 wal::v2::rotation`.
- **Idempotency:** Rotation is naturally one-shot; coord file
  updates are idempotent (overwrite with same value is fine).
- **Status:** Todo

---

### Task W2-4: `wal::v2::Wal` aggregator

- **Description:**
  The user-facing handle.  Owns the active `MmapSegmentWriter`
  and the segment index.  Drives retention.  Shape mirrors v0.1's
  `Wal` so the Publisher/Subscriber integration in W2-5/W2-6 is
  small.
- **Priority:** P1
- **Acceptance criteria:**
  - AC1: `Wal::open(dir, cfg)` reads `active.dat` (or detects
    fresh WAL), opens the active segment, builds the in-memory
    index, runs recovery on any open-but-unclean segment.
  - AC2: `append(cursor, ts, payload)` calls
    `MmapSegmentWriter::append`; on `None` (overrun), invokes
    rotation and retries.
  - AC3: `read_from(cursor)` returns a `WalReplayer` matching
    v0.1's API.
  - AC4: `stats()` returns `WalStats` with the same fields as v0.1.
  - AC5: 10 unit tests mirroring `wal::wal::tests` (open empty,
    append + read, rotate, retention, stats, reopen, etc.).
- **Inputs:** RFC §3 entire.
- **Outputs:**
  - `src/wal/v2/wal.rs`.
- **Dependencies:** W2-1, W2-2, W2-3.
- **Constraints:**
  - Public API surface (`Wal::open`, `append`, `read_from`,
    `stats`, `bump_generation`, `durable_cursor`, `pending_cursor`,
    `oldest_cursor`) must match v0.1 exactly so the Publisher
    can swap implementations behind a feature flag.
- **Steps:** 1. Open / recovery loop.  2. append + rotation.
  3. read_from iterator (same WalReplayer shape).  4. retention
  drain.  5. tests.
- **Error handling:**
  - Same `WalError` variants as v0.1.
- **Validation:** `cargo test --features wal_v2 wal::v2::wal`.
- **Idempotency:** Open is idempotent on a clean dir; runs
  recovery on a dirty dir.
- **Status:** Todo

---

### Task W2-5: Publisher integration behind feature flag

- **Description:**
  Swap `Publisher`'s `wal: Option<wal::Wal>` field for
  `wal: Option<WalImpl>` where `WalImpl` selects between v0.1
  and v0.2 based on the `wal_v2` feature.  Default behaviour
  unchanged.
- **Priority:** P1
- **Acceptance criteria:**
  - AC1: With `wal_v2` off (default), no change to v0.1 behaviour
    or perf.
  - AC2: With `wal_v2` on, `cargo test --features wal_v2` runs the
    full existing suite (incl. `tests/wal_publisher.rs`,
    `tests/wal_subscriber.rs`, `tests/wal_acceptance.rs`) against
    the v2 backend, all pass.
  - AC3: Bench (`cargo bench --features wal_v2 --bench
    publish_with_wal`) shows `wal=Batched` overhead ≤ +10% vs
    baseline.
- **Inputs:** v0.1 `Publisher`, W2-4.
- **Outputs:**
  - `src/publisher.rs` modified.
  - `src/wal/mod.rs` re-exports.
- **Dependencies:** W2-4.
- **Constraints:**
  - Zero-cost when `wal_v2` is off.  No new branches on the no-WAL
    hot path.
- **Steps:**
  1. Trait or `enum WalImpl { V1(v1::Wal), V2(v2::Wal) }`.
  2. Publisher's `wal: Option<WalImpl>` field.
  3. Update `Wal::open` calls in `Publisher::create`.
  4. Run existing tests + bench under `--features wal_v2`.
- **Error handling:** Same `Error::Wal` propagation.
- **Validation:**
  - Automated: `cargo test --features wal_v2`; `cargo bench
    --features wal_v2 --bench publish_with_wal`.
  - Manual: pubsub round-trip with v2 enabled.
- **Idempotency:** Same as v0.1.
- **Status:** Todo

---

### Task W2-6: Subscriber integration

- **Description:**
  Subscriber's WAL replay path uses the v2 reader when v2 is
  enabled.  WalReader / WalReplayer types stay compatible.
- **Priority:** P1
- **Acceptance criteria:**
  - AC1: `subscribe_from(cursor)` over a v2 WAL replays every
    record up to live tail and transitions to ring reads.
  - AC2: `tests/wal_subscriber.rs` passes under `--features
    wal_v2`.
- **Inputs:** v0.1 `Subscriber`, W2-4.
- **Outputs:**
  - `src/subscriber.rs` modified.
  - `src/wal/v2/reader.rs` (read-only directory scanner).
- **Dependencies:** W2-4.
- **Constraints:**
  - Must follow rotations during replay (SKIP_TO_END handling).
- **Steps:**
  1. v2 WalReader (mirrors v0.1 WalReader).
  2. Wire Subscriber to pick v2 when feature is on.
  3. Tests.
- **Validation:** `cargo test --features wal_v2 --test wal_subscriber`.
- **Idempotency:** Yes.
- **Status:** Todo

---

### Task W2-7: Per-platform durability primitives

- **Description:**
  Implement `flush_async()` / `flush_sync()` per OS so the
  Batched flusher and the Each policy have a portable surface.
- **Priority:** P1
- **Acceptance criteria:**
  - AC1: Linux: `msync(MS_ASYNC)` and `msync(MS_SYNC) +
    fdatasync(fd)`.
  - AC2: macOS: `msync(MS_ASYNC)` and
    `msync(MS_SYNC) + fcntl(fd, F_FULLFSYNC)`.
  - AC3: Windows: `FlushViewOfFile` and `FlushViewOfFile +
    FlushFileBuffers`.
  - AC4: Cross-platform test asserts that after `flush_sync()`,
    a fresh process opening the file sees every record (uses
    `tempfile::tempdir` + `Command::new(current_exe()).arg("--
    subprocess-reader")`).
- **Inputs:** RFC §3.5.
- **Outputs:**
  - `src/wal/v2/durability.rs` with `cfg` per platform.
- **Dependencies:** W2-1 (need a writer to flush).
- **Constraints:**
  - Inline `flush_sync` cost is OS-bound (3-5 ms on APFS) — this
    is the wal=Each baseline, not optimisable.
- **Steps:**
  1. `flush_async(&MmapMut, &File) -> io::Result<()>` per platform.
  2. `flush_sync(...)` per platform.
  3. Cross-platform subprocess test.
- **Validation:** `cargo test --features wal_v2 wal::v2::durability`
  on Linux, macOS, Windows CI matrices.
- **Idempotency:** Both are idempotent.
- **Status:** Todo

---

### Task W2-8: Acceptance + perf + default flip

- **Description:**
  Re-run W1-f's acceptance suite under `--features wal_v2`; add
  a v2-specific bench targeting the +10% gate; if green, flip
  `WalConfig::default().enabled` to true in v0.2.0 and promote
  the v2 path out of feature-flag in v0.2.1.
- **Priority:** P1
- **Acceptance criteria:**
  - AC1: `tests/wal_acceptance.rs` passes under v2.
  - AC2: `cargo bench --features wal_v2 --bench publish_with_wal`
    shows `wal=Batched` ≤ +10% vs no-WAL.
  - AC3: `docs/rfc-wal-v2-lockfree.md` gets a "Shipped" header +
    Results section.
  - AC4: `CHANGELOG.md` [Unreleased] entry for v0.2.0; flip
    `WalConfig::default().enabled` to true.
  - AC5: Open a follow-up task tracking v2 promotion out of
    feature flag (v0.2.1).
- **Inputs:** Everything from W2-0 through W2-7.
- **Outputs:**
  - `tests/wal_v2_acceptance.rs` (or extends `wal_acceptance.rs`).
  - `benches/publish_with_wal.rs` updated.
  - `CHANGELOG.md`, RFC, plan updated.
- **Dependencies:** W2-1 through W2-7.
- **Constraints:**
  - If the +10% gate doesn't land, do NOT flip the default; ship
    v2 as opt-in instead and open follow-ups.
- **Steps:**
  1. Run acceptance under v2.  2. Run bench.  3. Decide flip.
  4. Update docs.
- **Validation:**
  - Automated: full suite under `--features wal_v2`.
  - Manual: cross-platform CI run + 24h burn-in under publish load.
- **Idempotency:** Yes.
- **Status:** Todo
- **Ambiguity:**
  - If only one platform hits +10% (e.g. Linux yes, macOS no due
    to APFS msync cost), do we flip per-platform?  Lean: no — pick
    the worst-case OS as the gate.

---

## Cross-cutting notes

### Feature flag discipline

- `wal_v2` is **off** by default for the whole v0.2.x cycle.
- v0.2.0 ships both implementations; users opt in with the feature
  + `WalConfig` toggle.
- v0.2.1 (after at least one release of burn-in) promotes v2 out
  of feature-flag and deletes v0.1's WAL implementation.

### Compatibility matrix

| Writer | Reader | Result                              |
|--------|--------|-------------------------------------|
| v1     | v1     | Works (existing)                    |
| v1     | v2     | Works (v2 reader handles v1 format) |
| v2     | v1     | Works (bit-compatible)              |
| v2     | v2     | Works (preferred)                   |

### Rollback story

If v2 surfaces a correctness regression in production:

- v0.2.0: users disable the `wal_v2` feature; v0.1 path is still
  there.
- v0.2.1: revert the v2 promotion commit; re-ship v0.1 path.

### Test infra

W2-5 / W2-6 / W2-8 re-run the existing `tests/wal_*.rs` suite
under the new backend.  No new test types invented for v2; the
acceptance criteria from W1-f are the contract.
