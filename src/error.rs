use std::io;
use thiserror::Error;

/// All errors returned by the mmbus public API.
#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Ring buffer is full and the backpressure policy is `Error`.
    #[error("ring buffer full")]
    Full,

    /// The payload exceeds the configured per-slot size.
    #[error("message too large: {size} bytes, max is {max}")]
    TooLarge { size: usize, max: usize },

    /// `subscribe_timeout` / `wait_for_subscribers` exceeded the deadline.
    #[error("connection timeout waiting for '{0}'")]
    Timeout(String),

    /// Subscriber count hit the per-topic cursor-table limit.
    #[error("too many subscribers: limit is {0}")]
    TooManySubscribers(u32),

    /// Another process or in-process `Publisher` already owns this topic.
    #[error("a publisher is already running for topic '{0}'")]
    AlreadyPublishing(String),

    /// `subscribe_from` requested a cursor older than the oldest in-ring slot.
    /// The caller should retry from `tail`, fail, or use a WAL (future work).
    #[error("cursor {requested} is older than the oldest in-ring slot ({oldest})")]
    CursorTooOld { requested: u64, oldest: u64 },
}

pub type Result<T> = std::result::Result<T, Error>;
