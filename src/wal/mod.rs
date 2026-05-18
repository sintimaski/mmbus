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
pub mod reader;
pub mod record;
pub mod segment_reader;
pub mod segment_writer;
pub mod stats;
#[cfg(feature = "wal_v2")]
pub mod v2;
#[allow(clippy::module_inception)]
pub mod wal;

pub use config::{FsyncPolicy, WalConfig};
pub use record::{Record, SegmentHeader, SegmentHeaderError, MAX_PAYLOAD_LEN, SEGMENT_HEADER_LEN};
pub use segment_reader::{recover_truncate, ReaderError, RecoveryReport, SegmentReader};
pub use segment_writer::{SegmentWriter, WriterError};
pub use stats::WalStats;

// The publisher / subscriber import `crate::wal::{Wal, WalError,
// WalReader, WalReplayer}` — re-export the right backend based on
// the `wal_v2` feature so the integration is transparent.  v0.1's
// types stay reachable at `crate::wal::wal::*` and `crate::wal::reader::*`
// for code that wants them explicitly (e.g. v0.1's own unit tests).
#[cfg(not(feature = "wal_v2"))]
pub use reader::WalReader;
#[cfg(not(feature = "wal_v2"))]
pub use wal::{Wal, WalError, WalReplayer};

#[cfg(feature = "wal_v2")]
pub use v2::reader::WalReader;
#[cfg(feature = "wal_v2")]
pub use v2::{Wal, WalError, WalReplayer};
