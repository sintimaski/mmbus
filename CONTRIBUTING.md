# Contributing to mmbus

This project follows the universal development harness in
`~/.claude/CLAUDE.md`, with the mmbus-specific overrides in
`CLAUDE.md` at the repo root.  TL;DR for contributors:

## Workflow

1. **Spec before code** for non-trivial changes.  Use the
   templates in `docs/templates/`:
   - `spec-template.md` — small features and behaviour changes.
   - `task-template.md` — multi-step plans (see the WAL Phase B
     and WAL v2 plans in `docs/plan-*.md` for full examples).
   - `runbook-template.md` — every operational alert needs one.
2. **Small diffs.**  One vertical slice per PR: types → logic →
   tests → docs.  No drive-by refactors.
3. **Tests first-class.**  Every change ships with at least one
   automated test that would have caught the regression.
4. **No mocked I/O on the data path.**  Real mmap, real sockets,
   real `tempfile::tempdir`.
5. **Bench what you touch.**  If your PR modifies `src/ring.rs`,
   `src/publisher.rs`, or anything under `src/wal/`, include
   before/after numbers from `cargo bench --bench {ring,
   publish_with_wal}` in the PR description.

## Local checks before pushing

```bash
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo clippy --all-targets --features wal_v2 -- -D warnings
cargo test
cargo test --features wal_v2
# Optional but recommended on bigger changes:
cargo bench --bench ring
cargo bench --bench publish_with_wal
```

The same gates run in CI (`.github/workflows/ci.yml`).  A red
local run will be red in CI; save yourself the round-trip.

## PR template

`.github/PULL_REQUEST_TEMPLATE.md` embeds the Code Review Lanes
(A: correctness, B: security, C: performance/reliability, D:
DX/UX, E: observability) from the universal harness.  Fill in
the lanes that apply; mark N/A + reason for the rest.

## Commit message style

```
<type>(<scope>): <short imperative>

<body — what changed and why, not how>

<task code if applicable, e.g. W1-d>

Co-Authored-By: <if pairing>
```

Types: `feat`, `fix`, `perf`, `docs`, `chore`, `refactor`,
`test`, `ci`, `build`.  Scope is usually a module name (`wal`,
`ring`, `publisher`, `bridge`, `python`).

## Areas with extra review rigor

- **`src/ring.rs`** — wire format changes need a version bump
  (currently v4) + reader fallback.  Pair with the
  `code-reviewer` agent.
- **`src/wal/`** — the WAL append happens before the ring
  publish; do not reorder.  Pair with the `security-reviewer`
  agent for any change to recovery / CRC logic.
- **`src/producer_lock.rs`** — the SPMC invariant is enforced
  here.  Any change needs cross-platform validation
  (Linux + macOS + Windows in CI).
- **`crates/mmbus-bridge/`** — network surface.  Pair with the
  `security-reviewer` agent for PSK / cert-pin / QUIC changes.

## Reporting bugs

`.github/ISSUE_TEMPLATE/bug.md` walks you through the minimum
info we need.  For security issues, see `SECURITY.md` — please
do NOT open a public issue for those.
