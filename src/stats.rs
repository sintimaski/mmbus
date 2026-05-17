use crate::ring::RingStats;
use crate::wal::WalStats;

/// Snapshot of a topic's ring-buffer and socket state.
#[derive(Debug, Clone)]
pub struct TopicStats {
    /// Ring stats (tail position, active subscriber cursors, per-cursor lags).
    pub ring: RingStats,

    /// Number of subscriber sockets currently accepted by the publisher.
    /// May lag slightly behind `ring.active_subscribers` (cursor is claimed
    /// before the socket handshake completes).
    pub connected_sockets: usize,

    /// WAL state when `BusConfig::wal.enabled = true`; otherwise `None`.
    pub wal: Option<WalStats>,
}
