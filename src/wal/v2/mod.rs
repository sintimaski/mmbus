//! Lock-free mmap-backed WAL — experimental v0.2.0 implementation.
//!
//! Full design: `docs/rfc-wal-v2-lockfree.md`.  Stage decomposition:
//! `docs/plan-wal-v2-lockfree.md`.
//!
//! Gated behind the `wal_v2` Cargo feature so v0.1.x stays the
//! on-by-default code path during the burn-in window.  Reader +
//! aggregator + publisher/subscriber integration land in W2-2
//! through W2-6.

pub mod active;
pub mod mmap_segment_reader;
pub mod mmap_segment_writer;
pub mod rotation;

pub use active::{peek as peek_active_coord, ActiveCoord, ACTIVE_COORD_FILENAME, ACTIVE_COORD_LEN};
pub use mmap_segment_reader::{
    MmapSegmentReader, ReadOutcome, ReaderError, SKIP_TO_END_SENTINEL,
};
pub use mmap_segment_writer::{
    align_record_len, AppendOutcome, MmapSegmentWriter, WriterError,
    SEGMENT_VERSION_V2, WRITING_BIT_U32,
};
pub use rotation::{open_segment_reader, rotate, segment_path, RotateError};
