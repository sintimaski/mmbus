//! Lock-free mmap-backed WAL — experimental v0.2.0 implementation.
//!
//! Full design: `docs/rfc-wal-v2-lockfree.md`.  Stage decomposition:
//! `docs/plan-wal-v2-lockfree.md`.
//!
//! Gated behind the `wal_v2` Cargo feature so v0.1.x stays the
//! on-by-default code path during the burn-in window.  Reader +
//! aggregator + publisher/subscriber integration land in W2-2
//! through W2-6.

pub mod mmap_segment_writer;

pub use mmap_segment_writer::{
    align_record_len, AppendOutcome, MmapSegmentWriter, WriterError,
    SEGMENT_VERSION_V2, WRITING_BIT_U32,
};
