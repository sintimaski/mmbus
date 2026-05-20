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

    /// Monotonic count of successful publishes since this Publisher
    /// was created.  Use `rate(published_total[1m])` for a per-
    /// second publish rate in Prometheus.
    pub published_total: u64,

    /// Monotonic count of publishes rejected with `Error::Full`
    /// (only fires under `BackpressurePolicy::Error`; DropOldest
    /// never rejects).
    pub full_rejected_total: u64,

    /// Monotonic count of subscribers dropped by the publisher
    /// because their wakeup call failed (typically: peer closed
    /// the connection / process died).
    pub subscribers_dropped_total: u64,

    /// Monotonic count of wakeup syscalls actually fired since this
    /// Publisher was created.  With wakeup coalescing this is far
    /// below `published_total` under bursts (a subscriber that is
    /// keeping up or actively draining is not re-woken per message).
    /// `wakeups_sent_total / published_total` is the coalescing ratio.
    pub wakeups_sent_total: u64,
}
