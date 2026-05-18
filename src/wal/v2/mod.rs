//! Lock-free mmap-backed WAL — experimental v0.2.0 implementation.
//!
//! Full design: `docs/rfc-wal-v2-lockfree.md`.  Stage decomposition:
//! `docs/plan-wal-v2-lockfree.md`.
//!
//! This module is empty in W2-0 — the writer / reader / aggregator
//! land in W2-1 through W2-4.  The whole module is gated behind the
//! `wal_v2` Cargo feature so v0.1.x stays the on-by-default code path
//! during the burn-in window.
