//! mmbus — zero-copy pub/sub over mmap + Unix-socket / eventfd wakeup.
//!
//! See [`Bus`] for the recommended high-level entry point.  The lower-level
//! [`Publisher`] / [`Subscriber`] types are exposed for callers that need
//! direct control over a single topic.

pub mod ring;
pub mod wal;

#[cfg(feature = "prometheus")]
pub mod prometheus;

mod bus;
mod config;
mod error;
mod producer_lock;
mod publisher;
mod stats;
mod subscriber;
mod subscription;
mod waker;

pub use bus::Bus;
pub use config::{BackpressurePolicy, BusConfig};
pub use error::{Error, Result};
pub use publisher::Publisher;
pub use ring::{RingBuffer, RingStats};
pub use stats::TopicStats;
pub use subscriber::{StartPos, Subscriber};
pub use subscription::Subscription;
pub use wal::{FsyncPolicy, WalConfig};

#[cfg(feature = "python")]
mod python;
