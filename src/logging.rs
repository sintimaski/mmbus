//! Optional stderr log subscriber (`logging` feature).
//!
//! mmbus emits structured [`tracing`] events at lifecycle points (publisher
//! created, subscriber connected/dropped, WAL rotation/retention, publisher
//! restart).  Those events are silent unless a subscriber is installed.
//! Rust callers usually wire their own (`tracing_subscriber::fmt::init()`);
//! [`init_logging`] is a convenience for them and the only path available to
//! Python users (a Python process can't install a Rust subscriber itself),
//! where it is re-exported as `mmbus.init_logging()`.

use std::sync::atomic::{AtomicBool, Ordering};

/// Install a global stderr subscriber for mmbus's `tracing` events.
///
/// Filtering precedence: the `RUST_LOG` environment variable if set (e.g.
/// `RUST_LOG=mmbus=debug`, `RUST_LOG=mmbus::wal=trace`), otherwise the
/// `level` argument (`"info"`, `"debug"`, `"mmbus=trace"`, …), otherwise
/// `"info"`.
///
/// Idempotent: the first call installs the subscriber and returns `true`;
/// any later call (or a call when another global subscriber is already
/// installed) is a no-op and returns `false`.  ANSI colour is disabled so
/// the output stays clean when redirected to a file.
pub fn init_logging(level: Option<&str>) -> bool {
    // Guard against double-install: only the first caller attempts it.
    static ATTEMPTED: AtomicBool = AtomicBool::new(false);
    if ATTEMPTED.swap(true, Ordering::AcqRel) {
        return false;
    }

    use tracing_subscriber::filter::EnvFilter;
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(level.unwrap_or("info")))
        .unwrap_or_else(|_| EnvFilter::new("info"));

    // `try_init` returns Err if some other global subscriber is already set
    // (e.g. the host application installed its own) — treat that as "not
    // installed by us" rather than panicking.
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .try_init()
        .is_ok()
}
