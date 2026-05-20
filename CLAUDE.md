# CLAUDE.md — mmbus

Project-specific extensions to `~/.claude/CLAUDE.md` (the universal
development harness).  The universal rules apply; this file calls
out invariants and conventions that are non-obvious from the code
and would be cheap to break in a refactor.

---

## Load-bearing invariants

These are correctness, not style.  Breaking any of them is a wire-
format break or a crash-safety break.

1. **Single publisher per topic.**  mmbus is SPMC by design.  The
   per-topic `producer.lock` (`flock` on Unix, `LockFileEx` on
   Windows) enforces this across processes; in-process a `HashSet`
   covers the BSD/macOS "flock is per-process not per-fd" gap.
   Never relax this without a wire-format bump.

2. **Wire format version stamping.**  The ring header carries
   `MAGIC` (`mmbus\0\0\0` + 4-byte version) + `version: u32`.
   Current version is `5` (v5 added the per-cursor `needs_wakeup`
   flag table after the cursor table — see the wakeup-coalescing
   eventcount in `Subscriber::wait_readable`/`arm_wakeup` +
   `Publisher::broadcast_wakeup`).  ANY layout change to the ring or
   the per-slot framing requires a version bump + a reader fall-back
   (subscribers refuse mismatched mmaps with `InvalidData`, not
   undefined behaviour).  Same rule applies to WAL segments:
   `SEGMENT_MAGIC` + `SEGMENT_VERSION = 1`.

   The handshake also carries the subscriber's `cursor_idx` to the
   publisher (Linux: in the `SCM_RIGHTS` iovec; macOS: a 4-byte
   prefix on the signal socket; Windows: in the semaphore-handshake
   struct) so `broadcast_wakeup` can address each subscriber's flag.
   A change to either the wire format or this handshake must keep the
   two sides in lockstep.

3. **No `ftruncate(0)` on a live ring.**  `RingBuffer::create_or_reuse`
   bumps the in-header `generation` counter instead.  A truncate
   would SIGBUS every subscriber holding the mmap.  This is the
   single most important crash-safety property; the regression test
   is `tests/crash_recovery.rs::restart_invalidates_existing_subscriber`.

4. **WAL append happens BEFORE the ring publish.**  Per
   `docs/rfc-wal-phase-b.md` §9.1: a failed WAL append returns
   `Error::Wal` and the ring stays untouched.  Reordering this
   creates a window where the ring has a record that the WAL
   doesn't — defeating crash recovery.  Test:
   `tests/wal_publisher.rs::each_policy_appends_and_fsyncs_before_ring_publish`.

5. **Seqlock bracket pattern around slot writes.**  The publisher
   stores `tail | SEQ_WRITING_BIT` BEFORE the payload write and
   the clean `tail` AFTER.  Subscribers retry on observing the
   WRITING bit.  Without the bracket, a `DropOldest` subscriber
   reads torn payloads.  Validated by
   `fuzz/fuzz_targets/ring_concurrent.rs`.

6. **Cursors are globally monotonic across publisher restarts when
   the WAL is enabled.**  `Publisher::create` aligns
   `ring.tail = wal.pending_cursor()` on open.  Without this,
   `subscribe_from(N)` across a restart returns the wrong records.

7. **Wakeup-coalescing eventcount: SeqCst fences on both sides.**
   The publisher fires a wakeup only when a subscriber's
   `needs_wakeup` flag is set.  A subscriber about to sleep does
   `set_wakeflag` → `fence(SeqCst)` → re-check `cursor < tail`
   (`Subscriber::wait_readable`/`arm_wakeup`); the publisher does
   tail-store (Release in `write_slot`) → `fence(SeqCst)` →
   `take_wakeflag` (`broadcast_wakeup`).  The paired SeqCst fences are
   what prevent a missed wakeup (sleeper stranded): in the total
   order, either the subscriber observes the new tail (doesn't sleep)
   or the publisher observes the flag (wakes it).  Dropping a fence,
   or weakening either flag op below SeqCst, reintroduces the race.
   The publisher also wakes a client whose cursor went `UNCLAIMED`
   (clean disconnect) so dead peers are still reaped.  Tests:
   `tests/wakeup_coalescing.rs`.

## Hot-path discipline

These rules guard the per-publish CPU budget.

- **No-WAL path is byte-identical to v0.1.0.**  Any new logic added
  to `Publisher::publish` must be inside an `if let Some(wal)` (or
  equivalent feature-gated block).  The `is_full` pre-check is the
  only example we already track.  Re-bench `cargo bench --bench
  publish_with_wal -- baseline_no_wal` on changes that touch the
  publisher hot path.
- **No allocations on the WAL append hot path.**  The historical
  regression was a per-publish `payload.to_vec()` in `Record`
  encoding.  `encode_record_into(&mut Vec, cursor, ts, &[u8])` is
  the allocation-free entry point.
- **No `SystemTime::now()` on the publish path.**  Use the cached
  `(wall_base_nanos, Instant)` snapshot from `Publisher::create`
  and compute `wall_base_nanos + mono_base.elapsed().as_nanos()`.
- **Bench every PR that touches the WAL or the ring.**  Include
  before/after `cargo bench --bench {ring,publish_with_wal}`
  numbers in the commit message.  The +10% gate on `wal=Batched`
  is the v0.2.0 release criterion; protect it.

## Testing patterns

- **Acceptance scenarios.**  Cross-cutting durability + replay
  contracts live in `tests/wal_acceptance.rs` and mirror the
  RFC's §15 scenarios.  When you change WAL semantics, update
  these — they are the contract.
- **Fuzz harnesses.**  `fuzz/fuzz_targets/ring_concurrent.rs` is
  the authority on the seqlock invariants.  Run for at least
  100k iterations before merging any change to `write_slot` /
  `try_receive` / the WRITING-bit dance.
- **Stress tests are opt-in.**  `cargo test --release --test
  stress -- --ignored` exercises fan-out + restart cycles.  Run
  in CI on tagged releases, not on every PR (they're slow).
- **No mocked I/O in integration tests.**  All `tests/*.rs` use
  real `tempfile::tempdir` + real mmap + real Unix sockets.
  Mocking the data path defeats the point of testing the data
  path.
- **Python smoke test (`python/smoke_test.py`)** runs in the
  Linux Dockerfile.  It's a sequential `__main__` script, not
  pytest — keep it that way so it can run in any CI without
  Python test infra.

## Observability conventions

- **Logs.**  No structured logging in the Rust core yet (open
  Phase-5 roadmap item).  Where stderr is used today (e.g.
  `recover_truncate` truncate WARN), keep the format `mmbus::<mod>:
  <message>` so a grep-based extractor can find them later when we
  wire `tracing`.
- **Metrics.**  Same — none today.  `WalStats` + `RingStats` +
  `TopicStats` are the canonical snapshot shape; any future
  Prometheus exporter reads from those, not from internal atomics
  directly.
- **Runbooks.**  Each operational alert needs a `docs/runbooks/`
  entry.  See `docs/runbooks/wal-disk-pressure.md` for the
  template.

## Workflow

- Use `/implement-task` for any task with 2+ files or any change
  to a public API.
- Use `/review` (the `code-reviewer` agent) before tagging a
  release.
- Use the `/security-review` agent before changes to the bridge
  (`crates/mmbus-bridge`), the producer lock, or the WAL.
- Commit messages: lead with `feat(<scope>):` / `fix(<scope>):` /
  `perf(<scope>):` / `docs(<scope>):` / `chore(<scope>):`.
  Include the W-prefixed task code (e.g. `W1-d`, `B4b-3`) when
  applicable so the commit links to the plan doc.

## Release readiness

See `docs/release-checklist.md`.  The two gates that bite are
(a) the OWNER placeholders (cleared as of 2026-05-18) and
(b) the wire-format version stamping — every release that
touches `ring.rs` or `wal/record.rs` must bump the relevant
version constant.
