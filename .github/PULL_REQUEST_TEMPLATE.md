<!--
Fill in the sections that apply.  Mark N/A + reason for the rest.
Reference: docs/templates/spec-template.md and CLAUDE.md.
-->

## Summary

<!-- 1–3 sentences: what changed and why. -->

## Type

- [ ] feat — new behaviour
- [ ] fix — bug fix
- [ ] perf — performance only, no behaviour change
- [ ] refactor — internal only, no behaviour change
- [ ] docs / chore / test / ci / build

## Scope

<!-- Module(s) touched. e.g. wal/segment_writer, publisher, ring. -->

## Acceptance criteria

<!-- Bullet list. Each one should map to a test or a manual check. -->

-
-

## Test plan

```bash
# Commands you ran; copy-paste the relevant test result line(s).
cargo test
cargo clippy --all-targets -- -D warnings
```

If the change touches a hot path (`ring.rs`, `publisher.rs`,
`wal/`), include bench numbers:

```
cargo bench --bench publish_with_wal -- baseline_no_wal
# before: 178 ns/iter
# after : 180 ns/iter (within noise)
```

---

## Code Review Lanes

Mark each lane: ✅ checked / ⚠️ partial / N/A + reason.

### Lane A — Correctness

- [ ] Types and contracts match the spec
- [ ] Error paths handled (null, empty, clock skew, dup delivery)
- [ ] Idempotency on mutations and workers
- [ ] Corner cases (see universal checklist in `~/.claude/CLAUDE.md`)
- [ ] Regression test added for any bug fixed

### Lane B — Security & Privacy

- [ ] No secrets in code/logs/responses
- [ ] No new PII fields without explicit opt-in
- [ ] Tenant / process isolation preserved (single-publisher
      invariant for mmbus)
- [ ] No injection vectors (path traversal, SQL, shell, HTML)

### Lane C — Performance & Reliability

- [ ] Hot path stays async/non-blocking; bounded queues
- [ ] No unbounded memory growth
- [ ] Retries capped + back-off documented
- [ ] DB / network calls have timeouts (N/A for in-process)
- [ ] Bench numbers attached if hot path was touched

### Lane D — DX & UX

- [ ] Errors are actionable (what failed + recovery)
- [ ] Loading / empty / error / data states handled (N/A for
      library code without UI)
- [ ] Docs updated for changed public surfaces (rustdoc,
      Python docstrings, README, CHANGELOG)

### Lane E — Observability & Ops

- [ ] Actionable logs added for new behaviour paths
- [ ] Metrics / counters updated where behaviour changed
- [ ] Migrations: safe deploy order, rollback path documented
      (N/A for libraries)
- [ ] Feature flags + env vars: defaults in README / `.env.example`
- [ ] Runbook added / updated for any new alert

---

## Wire format

- [ ] No wire-format change
- [ ] Wire format changed → version constant bumped + reader
      fall-back tested (see `CLAUDE.md` §"Load-bearing invariants")

## Rollback plan

<!-- One line: how do we revert if this breaks production? -->
