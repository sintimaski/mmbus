//! Write-ahead log for durable replay (Phase B of `rfc-wal-replay.md`).
//!
//! Lets late-joining or crash-restarted subscribers replay messages
//! older than the in-ring history (Phase A, shipped in v0.1.0).  Opt-in
//! per-bus via [`BusConfig::wal`](crate::config::BusConfig).  The
//! whole module compiles + is callable today but the publisher and
//! subscriber wiring lands in stages W1-d and W1-e of
//! `docs/plan-wal-phase-b.md`; until then a non-disabled
//! `WalConfig` is a no-op at the bus level.
//!
//! Full design: `docs/rfc-wal-phase-b.md`.

mod config;
pub mod record;
pub mod segment_writer;

pub use config::{FsyncPolicy, WalConfig};
pub use record::{Record, SegmentHeader, SegmentHeaderError, MAX_PAYLOAD_LEN, SEGMENT_HEADER_LEN};
pub use segment_writer::{SegmentWriter, WriterError};
