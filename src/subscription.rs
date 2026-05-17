use crate::error::{Error, Result};
use crate::subscriber::Subscriber;
use std::io;
use std::os::unix::io::RawFd;
use std::time::Duration;

/// A live subscription to a topic. Returned by [`crate::Bus::subscribe`].
/// Implements `Iterator<Item = Result<Vec<u8>>>` for ergonomic loops.
pub struct Subscription {
    sub: Subscriber,
}

impl Subscription {
    pub(crate) fn new(sub: Subscriber) -> Self {
        Self { sub }
    }

    /// Block until the next message arrives.
    pub fn recv(&mut self) -> Result<Vec<u8>> {
        self.sub.receive()
    }

    /// Block with a timeout. Returns `Ok(None)` if no message arrives in time.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<Option<Vec<u8>>> {
        self.sub.receive_timeout(timeout)
    }

    /// Non-blocking poll. Returns `None` immediately if no message is ready.
    pub fn try_recv(&mut self) -> Option<Vec<u8>> {
        self.sub.try_receive()
    }

    /// How many messages this subscriber is behind the producer.
    pub fn lag(&self) -> u64 {
        self.sub.lag()
    }

    /// Current read cursor position.
    pub fn cursor(&self) -> u64 {
        self.sub.cursor()
    }

    /// The underlying wakeup file descriptor.
    ///
    /// On Linux this is the subscriber's `eventfd(2)`; on macOS it is the Unix
    /// domain socket. Either way the fd becomes readable when a new message
    /// is available, so callers can pass it to
    /// `asyncio.get_event_loop().add_reader()` or any `epoll`/`kqueue`-based
    /// poller for truly non-blocking receive.
    pub fn fileno(&self) -> RawFd {
        self.sub.fileno()
    }

    /// The handshake socket fd. On Linux this differs from [`Self::fileno`] and
    /// signals **publisher disconnect** via `POLLHUP`. Register both with
    /// the event loop so disconnect is detected even while idle. On macOS
    /// it equals [`Self::fileno`].
    pub fn socket_fileno(&self) -> RawFd {
        self.sub.socket_fileno()
    }

    /// Non-blocking event-loop callback helper: drain at most one wakeup
    /// signal and attempt one ring read.  Returns:
    ///
    /// * `Ok(Some(msg))` — a message was received.
    /// * `Ok(None)`      — no wakeup pending (spurious wake or already drained).
    /// * `Err(_)`        — publisher disconnected.
    pub fn poll_recv(&mut self) -> Result<Option<Vec<u8>>> {
        self.sub.poll_recv()
    }
}

impl Iterator for Subscription {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.sub.receive() {
            Ok(msg) => Some(Ok(msg)),
            Err(Error::Io(e))
                if e.kind() == io::ErrorKind::UnexpectedEof
                    || e.kind() == io::ErrorKind::ConnectionReset =>
            {
                None // publisher disconnected
            }
            Err(e) => Some(Err(e)),
        }
    }
}
