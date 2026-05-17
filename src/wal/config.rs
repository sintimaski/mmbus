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
    /// The "WAL not in use" config — `WalConfig::default()` also
    /// returns this.  Exists as a named constructor so opt-in/opt-out
    /// reads cleanly at call sites.
    pub fn disabled() -> Self {
        Self::default()
    }

    /// Convenience: a fully-on default-policy WAL.  Equivalent to
    /// `WalConfig { enabled: true, ..Default::default() }`.
    pub fn batched() -> Self {
        Self { enabled: true, ..Self::default() }
    }
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            enabled: false,
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
    fn default_is_disabled() {
        let cfg = WalConfig::default();
        assert!(!cfg.enabled, "default WAL must be off — opt-in only");
        assert_eq!(cfg.fsync_policy, FsyncPolicy::Batched);
    }

    #[test]
    fn disabled_alias_matches_default() {
        let a = WalConfig::default();
        let b = WalConfig::disabled();
        assert_eq!(a.enabled, b.enabled);
        assert_eq!(a.fsync_policy, b.fsync_policy);
        assert_eq!(a.fsync_interval, b.fsync_interval);
        assert_eq!(a.segment_size_max, b.segment_size_max);
        assert_eq!(a.retention_bytes, b.retention_bytes);
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
