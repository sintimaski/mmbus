# Release readiness checklist

Run through every box before tagging a release.  Mirrors the
"Release Readiness" section of the universal harness in
`~/.claude/CLAUDE.md`.

## Automated gates (all must pass)

- [ ] `cargo test` (default features) — every crate green
- [ ] `cargo test --features wal_v2` — wal_v2 path green
- [ ] `cargo clippy --all-targets -- -D warnings`
- [ ] `cargo clippy --all-targets --features wal_v2 -- -D warnings`
- [ ] `cargo build --release` succeeds on Linux + macOS (+ Windows
      if any platform-conditional code changed)
- [ ] `cargo audit` shows no unfixed advisories (or each is
      explicitly waived with a comment in `audit.toml`)
- [ ] `cargo bench --bench ring` — no regression > 10% vs the
      last release baseline
- [ ] `cargo bench --bench publish_with_wal` — no regression
      on `baseline_no_wal`; WAL policies within their
      documented overhead bands
- [ ] Python smoke test (`python/smoke_test.py`) runs in
      the Linux Dockerfile

## Manual gates

- [ ] Happy path works end-to-end (`examples/pub.py` +
      `examples/sub.py` round-trip on Linux + macOS)
- [ ] `BusConfig::wal = WalConfig::disabled()` path is
      byte-identical to v0.1.0 (no perf regression)
- [ ] Auth boundary preserved (single-publisher invariant
      enforced by `producer.lock`)
- [ ] Wire format version constants checked:
  - [ ] `ring::MAGIC` / `ring::VERSION` (currently 4)
  - [ ] `wal::record::SEGMENT_MAGIC` /
        `wal::record::SEGMENT_VERSION` (currently 1)
- [ ] CHANGELOG `[Unreleased]` section reviewed; move to a
      versioned section if shipping
- [ ] Docs updated: README quickstart still works, rustdoc
      builds clean, any new public API has a doc-comment

## Release process

1. Bump `Cargo.toml` `version = ` if needed (semver: PATCH
   for bug fixes, MINOR for opt-in features, MAJOR only on
   wire-format break).
2. Move `[Unreleased]` content to a new versioned section in
   `CHANGELOG.md`.  Update the compare-link footer.
3. Commit (`chore(release): vX.Y.Z`).
4. Tag: `git tag vX.Y.Z -m "..."`.
5. Push: `git push && git push --tags` → `wheels.yml` builds
   + publishes to PyPI on the tag push.
6. After CI succeeds: `gh release create vX.Y.Z --generate-notes`
   (the auto-generated notes are a starting point; paste in the
   CHANGELOG section as the body).

## Risk register

For each class, mark **pass** / **needs mitigation** / **blocked**
with evidence + owner if mitigation required.

- [ ] Data exposure (no PII in logs, no secrets in responses)
- [ ] Auth boundary (single-publisher per topic preserved)
- [ ] Hot path perf (no-WAL path, WAL=Batched gate)
- [ ] Reliability containment (bounded queues, no memory
      growth under load)
- [ ] Scope drift (features beyond stated goal)
- [ ] Data lifecycle (WAL retention, ring overflow semantics)
- [ ] Supply chain (`cargo audit` + dep updates)

Release proceeds when all automated gates pass + no **blocked**
risks remain.
