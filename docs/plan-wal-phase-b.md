# WAL Phase B — implementation plan

Decomposition of `docs/rfc-wal-phase-b.md` into reviewable tasks
using the universal task template from `~/.claude/CLAUDE.md`.

Read order: RFC for the design + trade-offs, this doc for the
execution slicing.  The RFC's §14 "Implementation staging" is the
two-line version of what's below; this doc expands each row into
acceptance criteria, dependencies, validation steps, and error
handling so a contributor can pick up any single task without
re-reading the whole RFC.

## Decomposition shape

Seven tasks, mostly linear:

```
W1-0 (scaffold) ──► W1-a (writer) ──► W1-b (reader) ──► W1-c (Wal)
                                                          │
                                                          ▼
                                                W1-d (Publisher)
                                                          │
                                                          ▼
                                                W1-e (Subscriber)
                                                          │
                                                          ▼
                                                W1-f (acceptance)
```

W1-f runs the RFC §15 acceptance criteria + the perf-budget bench;
it's the gate before flipping `wal = "batched"` from opt-in to the
default in CHANGELOG / docs.

Each task is sized for one or two reviewable commits.

---

### Task W1-0: Module scaffolding + config surface + Cargo deps

- **Description:**
  Create the `mmbus::wal` module skeleton, add the `WalConfig` /
  `FsyncPolicy` types from RFC §11, plumb a `wal: WalConfig` field
  through `BusConfig` (default `WalConfig::disabled()`), and add the
  CRC32C dep.  No behavioural change — `Publisher` ignores `wal`
  until W1-d.
- **Priority:** P1
- **Acceptance criteria:**
  - AC1: `cargo test` is green; the new module compiles and is
    exposed as `mmbus::wal::{WalConfig, FsyncPolicy}`.
  - AC2: A `BusConfig { wal: WalConfig { enabled: true, ... }, .. }`
    round-trips through `serde` (writing this for forward-compat
    with future Python kwarg support).
  - AC3: TCP-only build size doesn't regress more than 50 KB
    (the CRC dep is small).
- **Inputs:** `docs/rfc-wal-phase-b.md` §11; existing `src/config.rs`.
- **Outputs:**
  - `src/wal/mod.rs` (module scaffold + re-exports).
  - `src/wal/config.rs` (`WalConfig`, `FsyncPolicy`, defaults from
    RFC §11).
  - `Cargo.toml` adds `crc32c = "0.6"` (or `crc32fast` if perf
    benches show it's faster on our target ISA).
  - `src/config.rs` gains `pub wal: WalConfig` with
    `#[serde(default)]`.
- **Dependencies:** None — this is the entry stage.
- **Constraints:**
  - `WalConfig::default()` must be `enabled = false` so existing
    callers see no behavioural change.
  - No new public API surface from `mmbus::*` beyond the two
    config types this stage adds.
- **Steps:**
  1. Add `mod wal;` to `src/lib.rs`.
  2. Create `src/wal/mod.rs` re-exporting `config::*`.
  3. Implement `WalConfig` + `FsyncPolicy` per RFC §11 with serde
     derives + `#[derive(Default)]` for the "disabled" shape.
  4. Add `pub wal: WalConfig` to `BusConfig` with serde default.
  5. Add `crc32c = "0.6"` to `Cargo.toml`.
  6. Unit test: `WalConfig::default().enabled == false` +
     `serde_json` round-trip of a fully-populated config.
- **Error handling:**
  - Expected failure modes: none on the hot path (this is
    type-level work).
  - Recovery / rollback: revert the commit; no on-disk state.
- **Validation:**
  - Automated: `cargo test --lib wal::config`;
    `cargo test --all-features` stays green.
  - Manual: `cargo doc --no-deps` shows the new `mmbus::wal`
    module page.
- **Idempotency:** Yes — type additions only.
- **Status:** Shipped
- **Ambiguity:**
  - crc32c vs crc32fast crate — decide in the validation step
    based on a micro-bench at default segment size (target:
    < 100 ns / 4 KiB record); commit the result in the task
    notes.
- **Observability:** None this stage.

---

### Task W1-a: `wal::SegmentWriter` — append + CRC + segment header

- **Description:**
  A single-file writer that lays down the 32 B segment header at
  open time, then appends length-prefixed CRC-tagged records.  Owns
  a `BufWriter<File>` so per-publish cost is one `write_all` + no
  syscalls when fsync_policy is `none` or `batched`.
- **Priority:** P1
- **Acceptance criteria:**
  - AC1: `SegmentWriter::create(path, first_cursor)` writes the
    header per RFC §4 (magic, version=1, reserved=0, first_cursor,
    ts_unix_nanos).
  - AC2: `append(cursor, ts, &payload)` writes the record (header
    fields + payload + crc32c over the body) and increments
    `pending_cursor`.
  - AC3: `fsync()` flushes the BufWriter then `fdatasync()` the
    file; updates `durable_cursor = pending_cursor`.
  - AC4: 8 unit tests cover: header round-trip, empty payload
    round-trip, large payload (just under MAX), CRC matches on
    read-back, two appends then read both, append-then-fsync
    advances durable_cursor.
- **Inputs:** RFC §4 (file format), §5 (fsync), §10 (recovery —
  understanding the read side helps shape the write side).
- **Outputs:**
  - `src/wal/segment_writer.rs`.
  - `src/wal/record.rs` (shared record layout constants + the
    `Record` struct serialise / deserialise helpers).
- **Dependencies:** W1-0 done.
- **Constraints:**
  - Append must do exactly one `write_all` call from a buffer
    we own (avoid scatter/gather for v1; one syscall per
    fsync_policy = `each` publish).
  - All multi-byte fields little-endian.
  - `MAX_RECORD_LEN = 16 MiB` to bound the decoder allocation
    risk (mirror the bridge frame `MAX_FRAME_LEN`).
- **Steps:**
  1. Define record layout constants (HEADER_LEN, SEGMENT_HEADER_LEN,
     MAGIC, VERSION, MAX_RECORD_LEN).
  2. `Record::encode_into(&self, &mut Vec<u8>)` — laid out
     identical to RFC §4.
  3. `SegmentWriter::create(path, first_cursor) -> Self` — opens
     with `OpenOptions::create_new().write(true)`, writes header.
  4. `SegmentWriter::append(cursor, ts, payload) -> Result<()>`.
  5. `SegmentWriter::fsync(&mut self) -> Result<u64>` returning
     new durable_cursor.
  6. `SegmentWriter::close(self) -> Result<()>` (fsyncs + drops).
  7. Tests.
- **Error handling:**
  - Expected failure modes: ENOSPC, EIO during append/fsync;
    OpenOptions create_new fails if path exists.
  - Recovery / rollback: append errors surface to caller; the
    BufWriter contents are dropped on error.  Caller (W1-d
    Publisher) decides whether to fail the publish or retry.
- **Validation:**
  - Automated: `cargo test --lib wal::segment_writer`.
  - Manual: `hexdump -C` on the written segment shows the magic
    + version + records at expected offsets.
- **Idempotency:** Partial — `create()` fails if the path exists
  (intentional, avoids clobbering); `append` is naturally
  appending and not retry-safe (a duplicate append duplicates
  the record).
- **Status:** Shipped
- **Ambiguity:**
  - Whether to use `pwrite` directly for the append step
    (skips BufWriter's Vec).  Defer to W1-c bench; the BufWriter
    is simpler and likely fast enough.
- **Observability:**
  - Counter (internal, no Prometheus yet): `wal_append_bytes`,
    `wal_fsync_calls`.  Exposed via `WalStats` in W1-c.

---

### Task W1-b: `wal::SegmentReader` + recovery scan

- **Description:**
  Open + iterate a segment, validate every record's CRC, and on
  the first CRC mismatch (or short read) truncate the segment at
  that offset so subsequent appends start clean.  Used both by
  the in-memory index builder (W1-c) and by `Publisher::open` on
  process restart (W1-d).
- **Priority:** P1
- **Acceptance criteria:**
  - AC1: `SegmentReader::open(path) -> Self` validates the header
    (magic, version) and reads `first_cursor`.
  - AC2: `iter()` yields `(cursor, ts, payload)` for every valid
    record in order; returns None on EOF.
  - AC3: `recover_truncate(path)` runs the full scan and
    `ftruncate`s at the first bad record, returning the post-
    truncate length (new EOF).  Logs WARN once with the truncate
    offset.
  - AC4: 6 tests: open + read clean segment; iter past EOF
    returns None; torn final record gets truncated; short
    payload (less than `payload_len` claims) gets truncated;
    CRC mismatch in middle gets truncated (everything after is
    lost — invariant); header version mismatch errors loudly.
- **Inputs:** RFC §4 + §10; W1-a's encoded segment shape.
- **Outputs:**
  - `src/wal/segment_reader.rs`.
- **Dependencies:** W1-a done.
- **Constraints:**
  - Reader uses a `BufReader<File>`; no `mmap` (keeps the WAL
    page-cache pressure under our control).
  - `recover_truncate` is the only operation that mutates the
    file — `open` + `iter` are read-only.
- **Steps:**
  1. `SegmentHeader::parse(&[u8; 32])` validator.
  2. `SegmentReader::open(path) -> Result<Self>`.
  3. `Records<'a>` iterator over decoded records.
  4. `recover_truncate(path) -> Result<u64>`.
  5. Tests including the corruption injections (write a clean
     segment via W1-a, then `seek` + overwrite to torn).
- **Error handling:**
  - Expected failure modes: header magic mismatch (return loud
    Err — not a recovery situation); read short of expected
    record body (recover_truncate); CRC mismatch (truncate).
  - Recovery / rollback: `recover_truncate` IS the rollback
    primitive; it brings the file to a known-good state.
- **Validation:**
  - Automated: `cargo test --lib wal::segment_reader`.
  - Manual: write a segment, corrupt last record bytes,
    `recover_truncate`, verify final size + iter completes
    without yielding the corrupted record.
- **Idempotency:** Yes — `recover_truncate` is idempotent (a
  re-run on an already-clean file is a no-op).
- **Status:** Shipped
- **Ambiguity:** None blocking.
- **Observability:**
  - WARN log on every truncate event with `bytes_dropped`,
    `offset_at_first_bad_record`.

---

### Task W1-c: `wal::Wal` aggregator — rotation + retention + flusher

- **Description:**
  The user-facing `Wal` struct that wraps a directory of segments.
  Handles rotation when active segment reaches `segment_size_max`,
  retention when total bytes exceed `retention_bytes`, the
  in-memory `BTreeMap<first_cursor → segment_path>` index, and
  the background flusher thread for `FsyncPolicy::Batched`.
- **Priority:** P1
- **Acceptance criteria:**
  - AC1: `Wal::open(dir, cfg)` scans `dir/wal/*.seg`, runs
    `recover_truncate` on each, builds the in-memory index,
    returns a writable handle.  Cold-start cost < 100 ms at
    default retention (1 GiB).
  - AC2: `append(cursor, payload)` writes to the active segment;
    rotates to a new segment when the active hits
    `segment_size_max`; rotates eagerly on `bump_generation()`.
  - AC3: `read_from(cursor)` returns an iterator that walks
    segments in order from the segment containing `cursor` to
    the active segment.  Returns `Err(WalError::CursorTooOld)`
    when `cursor < oldest_cursor`.
  - AC4: Background flusher thread under `FsyncPolicy::Batched`
    advances `durable_cursor` every `fsync_interval` or
    `fsync_batch_bytes`, whichever first.
  - AC5: Retention thread deletes the oldest segment when total
    bytes exceed `retention_bytes`; never deletes the active
    segment.
  - AC6: 10 unit tests covering: open empty dir, rotate at size
    cap, generation bump rotates, retention deletes oldest,
    `read_from` walks segments in order, `CursorTooOld` for
    pre-retention reads, batched flusher latency, fsync-each
    durability, in-memory index reflects deletions.
- **Inputs:** RFC §4 (layout), §6 (rotation), §7 (retention),
  §8 (index), §11 (config), §12 (observability).
- **Outputs:**
  - `src/wal/wal.rs`.
  - `src/wal/stats.rs` (WalStats from RFC §12).
- **Dependencies:** W1-a + W1-b done.
- **Constraints:**
  - Single writer (mmbus is SPMC) — no inter-process locking;
    the existing `producer.lock` already enforces single-process.
  - Background threads must surface failures via a flag the
    publisher checks on each `append`; a dead flusher with
    `fsync_policy = batched` should poison the WAL rather than
    silently regress to no-durability.
- **Steps:**
  1. `Wal::open(dir, cfg)`.
  2. Index builder + recovery loop.
  3. `Wal::append(cursor, payload)` with rotation.
  4. `Wal::read_from(cursor)` segment iterator.
  5. Background flusher (spawn under `Batched`; no-op under
     `None`/`Each`).
  6. Background retention rotator.
  7. `Wal::stats() -> WalStats`.
  8. `Wal::bump_generation()` for the publisher-restart path.
  9. Tests.
- **Error handling:**
  - Expected failure modes: ENOSPC during rotation (publish
    surfaces `Error::WalFull`); flusher thread death (publish
    surfaces `Error::WalPoisoned`); retention delete fails
    (logged, retried on next tick).
  - Recovery / rollback: caller's `Publisher` decides; for an
    in-memory test harness, `Wal::close` cleanly drops + joins
    threads.
- **Validation:**
  - Automated: `cargo test --lib wal::wal -- --test-threads=1`
    (the rotation tests touch shared dirs; serialise to keep
    them isolated).
  - Manual: drive `Wal::append` in a loop for 100k records;
    verify `ls -la wal/` shows the expected segment rotation;
    verify oldest segment gets deleted at retention threshold.
- **Idempotency:** `Wal::open` is idempotent on a clean dir;
  on a dirty dir it runs `recover_truncate` (also idempotent).
  `append` is naturally appending — re-running publishes records
  twice.
- **Status:** Shipped
- **Ambiguity:**
  - `bump_generation` semantics: rotate immediately, OR rotate
    on next append?  RFC §6 says "publisher detects fresh
    generation" — immediate rotation simplifies the
    Publisher::create_or_reuse handoff.  Document the choice.
- **Observability:**
  - `WalStats { pending_cursor, durable_cursor, oldest_cursor,
    active_segment_bytes, total_wal_bytes, segments }`.
  - Counters for `wal_appends_total`, `wal_fsyncs_total`,
    `wal_rotations_total`, `wal_retention_deletes_total`,
    `wal_torn_records_total` (per RFC §12).

---

### Task W1-d: Publisher integration — hot-path append + recovery handoff

- **Description:**
  Plumb `Wal` through `Publisher::create_or_reuse`.  On every
  `publish`, append the record to the WAL *before* the ring
  write, stamped with the same cursor.  On `create_or_reuse`
  with an existing WAL, find the highest intact cursor and
  start the ring tail at `max(wal_last + 1, ring.tail())`.
- **Priority:** P1
- **Acceptance criteria:**
  - AC1: With `cfg.wal.enabled = true`, `Publisher::publish`
    writes the WAL record before the ring publish; on WAL append
    error, publish returns the error and does NOT advance the
    ring.
  - AC2: A publish that returns `Ok` from a `wal = "each"` bus
    is observable to a `subscribe_from(0)` subscriber after a
    SIGKILL + restart of the publisher.
  - AC3: A `wal = "batched"` bus may lose the last few records
    on crash, but the durable_cursor + ring tail stay consistent
    (subscribers replaying from before durable_cursor see every
    record; nothing past durable_cursor).
  - AC4: With `wal.enabled = false`, the publish hot path is
    byte-identical to today (no extra branch, no extra
    allocation).
  - AC5: 5 tests in `tests/wal_publisher.rs`: roundtrip with
    each fsync_policy, generation handoff after restart with
    intact WAL, generation handoff after restart with torn-
    tail WAL.
- **Inputs:** RFC §9.1 (write ordering), §9.2 (generation
  handling); existing `src/publisher.rs`.
- **Outputs:**
  - `src/publisher.rs` modified.
  - `tests/wal_publisher.rs`.
- **Dependencies:** W1-c done.
- **Constraints:**
  - The `wal = false` path must remain a zero-cost abstraction.
    A `match cfg.wal.enabled { false => ring.publish, true => ... }`
    is fine; an unconditional WAL call is not.
  - WAL append failure leaves the ring untouched (no partial
    state).
- **Steps:**
  1. Add `wal: Option<Wal>` to `Publisher`.
  2. In `Publisher::create_or_reuse`, when `cfg.wal.enabled`,
     open the WAL and reconcile cursors per §9.2.
  3. Rewrite `publish()` to call `wal.append` first, then
     `ring.publish`.
  4. `Publisher::stats()` extended with `WalStats` when enabled.
  5. Tests including a forked-process variant for the SIGKILL
     recovery cases.
- **Error handling:**
  - Expected failure modes: WAL append fails (return
    `Error::Wal`); WAL recovery on open finds segments newer
    than the ring tail (RFC §9.2 — start ring at WAL max + 1).
  - Recovery / rollback: WAL append happens before ring publish,
    so a failed append leaves no inconsistency.  A failed ring
    publish after a successful WAL append leaves a "phantom"
    WAL record observable only via replay; documented as
    at-least-once.
- **Validation:**
  - Automated: `cargo test --test wal_publisher`.
  - Manual: `cargo bench --bench publish_with_wal` — compare to
    `publish_no_wal` baseline; target < 10% regression at
    default settings.
- **Idempotency:** `create_or_reuse` is idempotent (per existing
  `producer.lock` + generation bump semantics; WAL recovery is
  also idempotent).  `publish` is not (each call appends).
- **Status:** Shipped
- **Ambiguity:**
  - Subscriber-visible `WalStats` exposure — surface in
    `TopicStats` so existing observability picks it up
    automatically?  Lean: yes; smaller API churn than a separate
    `wal_stats()` call.  Document.
- **Observability:**
  - `TopicStats.wal` populated when WAL enabled.
  - Existing publish-success/failure counters extended with a
    `wal_failed_appends` increment when the WAL is the reason
    for a publish failure.

---

### Task W1-e: Subscriber integration — `subscribe_from` over WAL → ring handoff

- **Description:**
  When `subscribe_from(cursor)` requests a position older than
  `ring.oldest_cursor()`, spin up a WAL replayer that feeds
  records to the subscriber.  When the replayer catches up to
  `ring.oldest_cursor()`, atomically transition to a normal SPMC
  ring cursor (per RFC §9.3's catch-up loop).
- **Priority:** P1
- **Acceptance criteria:**
  - AC1: `subscribe_from(cursor)` where
    `cursor >= wal.oldest_cursor() && cursor < ring.oldest_cursor()`
    succeeds and the subscriber receives every record from
    `cursor` to the current tail in order with no duplicates.
  - AC2: `subscribe_from(cursor)` where `cursor < wal.oldest_cursor()`
    returns `Error::CursorTooOld { requested, oldest:
    wal.oldest_cursor() }`.
  - AC3: A subscriber that catches up during heavy publish load
    transitions cleanly (no missed messages between WAL last and
    ring first).
  - AC4: 6 tests in `tests/wal_subscriber.rs`: subscribe_from
    pre-ring cursor with each fsync_policy, subscribe at cursor
    exactly at boundary, subscribe at cursor too old, catch-up
    under sustained-publish load, subscriber drop during replay
    cleans up the replayer thread, multiple concurrent replayers.
- **Inputs:** RFC §9.3 (handoff loop); existing
  `src/subscriber.rs` + `src/subscription.rs`.
- **Outputs:**
  - `src/subscriber.rs` modified.
  - `src/wal/replayer.rs` (new — owns the per-subscriber WAL
    iterator thread).
  - `tests/wal_subscriber.rs`.
- **Dependencies:** W1-c done (and W1-d for end-to-end tests).
- **Constraints:**
  - The replayer must not hold the publisher's WAL handle
    open across rotations; it `Wal::open`s its own read-only
    view at subscribe time.
  - The transition from WAL → ring must claim a real ring
    cursor at the precise position right after the last WAL
    record; the seqlock guarantees correctness if a publisher
    overwrote the intervening slot, but the cursor math must be
    exact.
- **Steps:**
  1. `WalReplayer::start(wal_view, start_cursor) -> mpsc::Receiver<Vec<u8>>`.
  2. Refactor `Subscriber::connect_with(StartPos::Explicit(c))`
     to branch on `c < ring.oldest_cursor()`.
  3. WAL→ring handoff loop per RFC §9.3.
  4. Tests.
- **Error handling:**
  - Expected failure modes: WAL deleted under us
    (`CursorTooOld` on next read; bubble up); replayer thread
    panics (subscriber receives EOF cleanly).
  - Recovery / rollback: subscriber's `Drop` joins the replayer.
- **Validation:**
  - Automated: `cargo test --test wal_subscriber`.
  - Manual: spin up `examples/np_pipeline.py` with WAL enabled,
    kill + restart the subscriber, verify it picks up where it
    left off.
- **Idempotency:** `subscribe_from(cursor)` is idempotent — two
  subscribers requesting the same cursor get independent
  replayers + matching streams.
- **Status:** Shipped
- **Ambiguity:**
  - Replayer rate-limit: should we throttle so a replayer can't
    starve live subscribers?  Lean: no v1 throttle; the WAL
    read is from disk + already slower than the live ring.
- **Observability:**
  - INFO log on replayer start with `start_cursor` +
    `ring_oldest_cursor`.
  - Counter `wal_replays_started_total`.

---

### Task W1-f: Acceptance criteria + perf bench

- **Description:**
  Run the RFC §15 acceptance criteria as an automated harness;
  add the perf bench that gates "< 10% regression vs no-WAL".
  Document the result; flip `wal = "batched"` from opt-in to
  the default in CHANGELOG + README once the budget holds.
- **Priority:** P1
- **Acceptance criteria:**
  - AC1: New `tests/wal_acceptance.rs` integration test runs
    every RFC §15 scenario for all three fsync policies and
    passes.
  - AC2: New `benches/publish_with_wal.rs` Criterion bench
    produces a number; compared to the existing
    `benches/ring.rs` 32 B publish, regression is < 10%.
  - AC3: `docs/rfc-wal-phase-b.md` gets a "Shipped" status
    header + a "Results" section with the bench numbers.
  - AC4: `CHANGELOG.md` Phase B entry under `[Unreleased]`
    with the perf result + the default-policy decision.
- **Inputs:** Everything from W1-a through W1-e.
- **Outputs:**
  - `tests/wal_acceptance.rs`.
  - `benches/publish_with_wal.rs`.
  - `docs/rfc-wal-phase-b.md` updated.
  - `CHANGELOG.md` updated.
- **Dependencies:** W1-a through W1-e done.
- **Constraints:**
  - Acceptance tests must be deterministic — no time-dependent
    assertions; use `Duration::from_millis(5)` minimums for
    batched flusher and verify the post-flush durable_cursor
    by polling not sleeping.
- **Steps:**
  1. Port each RFC §15 scenario into a `#[test]` function.
  2. Write the publish bench (1k publishes, 32 B payload, mean
     ns/op).
  3. Capture numbers; update RFC + CHANGELOG.
  4. If regression > 10%, surface in the commit message + open
     a follow-up task for the optimisation pass before
     defaulting `batched`.
- **Error handling:**
  - Expected failure modes: a single acceptance test fails →
    block the merge of W1-f; do not flip the default.
  - Recovery / rollback: the WAL itself stays opt-in until W1-f
    is green; reverting W1-f leaves W1-a..W1-e usable.
- **Validation:**
  - Automated: `cargo test --release --test wal_acceptance`;
    `cargo bench --bench publish_with_wal`.
  - Manual: cross-check the published bench numbers against
    the RFC §15 < 10% gate.
- **Idempotency:** Yes — tests + benches re-run cleanly.
- **Status:** Shipped
- **Ambiguity:**
  - Default fsync_policy at flip time — if `batched` shows >
    10% regression, do we ship with `none` as default and a
    documented opt-in to `batched`?  Probably yes; record the
    decision in the commit.
- **Observability:** Documented in commit / RFC update; no new
  metrics specific to acceptance.

---

## Cross-cutting notes

### Error type surface

A new `mmbus::Error::Wal(WalError)` variant.  `WalError`:

```rust
pub enum WalError {
    Io(std::io::Error),
    BadMagic { found: u64 },
    UnsupportedVersion(u32),
    TornRecord { offset: u64 },
    CursorTooOld { requested: u64, oldest: u64 },
    Poisoned, // flusher thread died
}
```

Python wrapper extends the existing `CursorTooOldError`
(reusing the same exception class; only the "oldest" semantics
change between Phase A and Phase B) and adds a `WalError` for
operator surfaces.

### Test infra

Each W1-x stage stays independently green.  W1-d and W1-e are
the only stages that touch existing `Publisher` / `Subscriber`
code; previous stages add new code only.  This keeps the
regression risk localised.

### Rollback story

If W1-d's perf regression is unacceptable, revert just W1-d;
W1-a..W1-c remain useful as a debug log (subscribers don't
care).  If a deeper correctness issue surfaces, the
`wal.enabled = false` default keeps every existing user
unaffected.
