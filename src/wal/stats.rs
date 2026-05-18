//! Observability snapshot returned by [`crate::wal::Wal::stats`].
//!
//! Surfaced through `TopicStats.wal` (planned for W1-d) so existing
//! mmbus monitoring picks it up automatically.

/// Read-only snapshot of WAL state.  Computed by walking the in-memory
/// index + querying the active segment; safe to call frequently
/// (one-or-two atomic loads + a `BTreeMap` head/tail walk).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalStats {
    /// Highest cursor appended (may not yet be durable under
    /// `FsyncPolicy::Batched`).
    pub pending_cursor: u64,
    /// Highest cursor fsynced.  Lags `pending_cursor` until the
    /// flusher ticks (Batched) or always equals it (Each / None).
    pub durable_cursor: u64,
    /// First cursor still on disk — anything older has been retained-
    /// out.  Used by the subscriber's `CursorTooOld` check.
    pub oldest_cursor: u64,
    /// Bytes in the currently-being-written segment.  Drives rotation
    /// when it crosses `WalConfig::segment_size_max`.
    pub active_segment_bytes: u64,
    /// Total bytes across every segment file on disk.  Drives
    /// retention when it exceeds `WalConfig::retention_bytes`.
    pub total_wal_bytes: u64,
    /// Number of segment files on disk (>= 1 once any record has
    /// been written; 0 before the first append).
    pub segments: usize,
    /// Monotonic count of successful `append()` calls since this
    /// WAL handle was opened.  Useful as a rate counter via
    /// `rate(appends_total[1m])` in Prometheus.
    pub appends_total: u64,
    /// Monotonic count of payload bytes appended since open
    /// (excludes per-record framing).
    pub append_bytes_total: u64,
    /// Monotonic count of completed `flush_sync` calls.  Each
    /// policy increments inline per publish; Batched increments
    /// per flusher tick (typically every `fsync_interval`).
    pub flushes_total: u64,
}
