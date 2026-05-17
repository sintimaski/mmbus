# Security policy

## Supported versions

mmbus is pre-1.0.  Only the latest minor release receives security
fixes; older releases get hard-deprecated.

| Version | Supported |
|---------|-----------|
| 0.1.x   | ✅        |
| < 0.1   | ❌        |

## Threat model

mmbus is **single-machine, same-user IPC by default**.  Its security
boundary is the local filesystem permissions on `BusConfig.base_dir`
(default `/tmp/mmbus/`).  Anyone who can read/write that directory can
publish to and subscribe from any bus inside it.

Out-of-scope (today):

- **Multi-tenant isolation**: there is no per-bus authentication.
  Don't use mmbus as a security boundary between users on the same
  host.  Use per-user `base_dir` paths if multiple uids share a host.
- **Untrusted publishers**: a malicious publisher with write access to
  `base_dir` can serve arbitrary bytes — subscribers must validate
  payload contents.  This is true of any pub/sub bus.
- **Filesystem-level attacks on `base_dir`**: tmpfs vs. ext4 vs. NFS
  semantics differ for `flock` and `mmap`; we test on local tmpfs/HFS
  only.  Don't put `base_dir` on a network filesystem.

In-scope:

- Memory safety in the Rust core (`#[forbid(unsafe_op_in_unsafe_fn)]`-
  worthy review of `RingBuffer`'s `unsafe` blocks; seqlock correctness
  under concurrent overwrite — exercised by `tests/stress.rs` +
  `fuzz/`).
- Correct lock semantics: `Bus::clean_topic` refuses to wipe a live
  topic; producer-lock acquisition is exclusive across processes
  (`flock`) AND in-process (HashSet) — the in-process check matters
  on BSD/macOS where `flock` is per-process not per-fd.
- Crash safety: a publisher crash never SIGBUSes a subscriber's mmap
  (generation-counter restart, no `ftruncate`).

## Reporting a vulnerability

We don't have a security mailing list yet.  Until we do, please open
a GitHub security advisory:

  https://github.com/OWNER/mmbus/security/advisories/new

(Once the repo is public.  In the interim, contact the maintainer
listed in `Cargo.toml`.)

Please **do not** open public GitHub issues for security problems.

### What we ask of you

- Provide a reproduction (`cargo test` case or minimal example).
- Tell us the version (`cargo pkgid` or `pip show mmbus`).
- Give us a reasonable window (30 days) to ship a fix before
  public disclosure.

### What we'll do

- Acknowledge within 7 days.
- Triage + fix on a `0.x.y` patch release; backport to the latest
  minor if applicable (see "Supported versions").
- Credit you in the release notes unless you ask us not to.
