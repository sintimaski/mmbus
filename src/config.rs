use std::path::PathBuf;

/// What the publisher does when the ring buffer is full.
#[derive(Clone, Debug, Default)]
pub enum BackpressurePolicy {
    /// Return `Err(Error::Full)` so the caller decides what to do.
    #[default]
    Error,

    /// Silently drop the oldest unread slot for the slowest subscriber and
    /// keep writing. The subscriber detects the skip on its next read.
    DropOldest,
}

/// Configuration for a [`crate::Bus`] or standalone
/// [`crate::Publisher`]/[`crate::Subscriber`].
#[derive(Clone, Debug)]
pub struct BusConfig {
    /// Max payload bytes per message (default: 64 KiB).
    pub slot_size: u32,

    /// Ring buffer slot count (default: 256).
    pub capacity: u32,

    /// Root directory for bus files (default: `/tmp/mmbus`).
    pub base_dir: PathBuf,

    /// Maximum simultaneous subscribers per topic (default: 16).
    pub max_subscribers: u32,

    /// What to do when the ring is full (default: `BackpressurePolicy::Error`).
    pub backpressure: BackpressurePolicy,
}

impl Default for BusConfig {
    fn default() -> Self {
        Self {
            slot_size: 64 * 1024,
            capacity: 256,
            base_dir: default_base_dir(),
            max_subscribers: 16,
            backpressure: BackpressurePolicy::Error,
        }
    }
}

/// Default on-disk root for bus files.  Per-platform because there is no
/// single cross-OS scratch location:
///   * Unix: `/tmp/mmbus`.
///   * Windows: `%LOCALAPPDATA%\mmbus`, falling back to `%TEMP%\mmbus`
///     and finally `C:\mmbus` if neither env var is set.
#[cfg(unix)]
fn default_base_dir() -> PathBuf {
    PathBuf::from("/tmp/mmbus")
}

#[cfg(windows)]
fn default_base_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("LOCALAPPDATA") {
        return PathBuf::from(dir).join("mmbus");
    }
    if let Ok(dir) = std::env::var("TEMP") {
        return PathBuf::from(dir).join("mmbus");
    }
    PathBuf::from(r"C:\mmbus")
}
