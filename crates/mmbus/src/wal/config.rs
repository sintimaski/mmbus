//! WAL configuration types (`WalConfig`, `FsyncPolicy`) and their
//! defaults.  See `docs/rfc-wal-phase-b.md` §11.

use std::time::Duration;

/// Per-record durability policy.  Selectable per-bus via
/// [`WalConfig::fsync_policy`].
///
/// | Variant   | Per-publish cost          | Durability on crash             |
/// |-----------|---------------------------|----------------------------------|
/// | `None`    | append to OS page cache   | last few seconds may be lost     |
/// | `Batched` | append + signal flusher   | last `fsync_interval` may be lost (default) |
/// | `Each`    | append + fsync(2)         | nothing lost since the most recent successful publish |
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// No fsync; rely on the OS to flush page cache on its own
    /// cadence.  Test/dev only.
    None,
    /// Background flusher thread fsyncs every
    /// [`WalConfig::fsync_interval`] *or* whenever pending bytes
    /// exceed [`WalConfig::fsync_batch_bytes`], whichever first.
    /// Subscribers are clamped to the durable cursor.
    #[default]
    Batched,
    /// `fsync(2)` inline on every publish.  Slowest but the
    /// strictest guarantee.
    Each,
}

/// WAL configuration carried by [`BusConfig`](crate::config::BusConfig).
/// `WalConfig::default()` is the **disabled** shape — existing callers
/// who don't opt in see no behavioural change.
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Enable the WAL for this bus.  Default `false`; opt-in only.
    pub enabled: bool,

    /// Per-record fsync policy.  Default [`FsyncPolicy::Batched`].
    pub fsync_policy: FsyncPolicy,

    /// Flusher tick interval for [`FsyncPolicy::Batched`].  Default 5 ms.
    pub fsync_interval: Duration,

    /// Pending-bytes high-water mark that triggers an immediate
    /// fsync regardless of the interval.  Default 1 MiB.
    pub fsync_batch_bytes: usize,

    /// Per-segment size cap that triggers rotation.  Default 64 MiB.
    pub segment_size_max: usize,

    /// Total on-disk retention cap; oldest segments are deleted to
    /// stay under this.  Default 1 GiB.
    pub retention_bytes: u64,
}

impl WalConfig {
    /// The "WAL not in use" config — opts a bus out of the v0.2.1+
    /// on-by-default WAL.  Useful for tests and bare-ring perf-
    /// sensitive deployments that don't need durability.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Self::default()
        }
    }

    /// Convenience: a fully-on default-policy WAL (== `Default`).
    /// Kept for source-compat with v0.1.x callers — equivalent to
    /// just `WalConfig::default()` since the v0.2.1 default flip.
    pub fn batched() -> Self {
        Self::default()
    }
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            // Flipped in v0.2.1: WAL on-by-default with the
            // `Batched` policy.  The post-perf-push overhead is
            // ~+22% over the no-WAL ring (4.6 → 3.6 Melem/s on
            // 32 B payloads), traded for free durable replay +
            // crash-safe at-least-once delivery.  Users who want
            // the bare ring opt out with `WalConfig::disabled()`.
            //
            // See docs/rfc-wal-v2-lockfree.md §11 for the
            // per-source overhead breakdown.
            enabled: true,
            fsync_policy: FsyncPolicy::Batched,
            fsync_interval: Duration::from_millis(5),
            fsync_batch_bytes: 1024 * 1024,
            segment_size_max: 64 * 1024 * 1024,
            retention_bytes: 1024 * 1024 * 1024,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_enabled() {
        // v0.2.1 flipped this default; see WalConfig::default()'s
        // doc-comment for the rationale + perf number.
        let cfg = WalConfig::default();
        assert!(cfg.enabled, "v0.2.1+ defaults to WAL on (Batched)");
        assert_eq!(cfg.fsync_policy, FsyncPolicy::Batched);
    }

    #[test]
    fn disabled_constructor_returns_disabled() {
        let d = WalConfig::disabled();
        assert!(!d.enabled, "WalConfig::disabled() must produce !enabled");
    }

    #[test]
    fn batched_constructor_enables_with_defaults() {
        let cfg = WalConfig::batched();
        assert!(cfg.enabled);
        assert_eq!(cfg.fsync_policy, FsyncPolicy::Batched);
    }

    #[test]
    fn default_constants_match_rfc_section_11() {
        let cfg = WalConfig::default();
        assert_eq!(cfg.fsync_interval, Duration::from_millis(5));
        assert_eq!(cfg.fsync_batch_bytes, 1024 * 1024);
        assert_eq!(cfg.segment_size_max, 64 * 1024 * 1024);
        assert_eq!(cfg.retention_bytes, 1024 * 1024 * 1024);
    }
}
