use crate::error::{Error, Result};
use crate::subscriber::Subscriber;
use std::io;
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::io::RawFd;

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

    /// Buffer-reusing variant of [`Self::recv`].  `out` is `clear()`'d
    /// then refilled with the next message's payload.  Saves one
    /// allocation per receive in tight loops.
    pub fn recv_into(&mut self, out: &mut Vec<u8>) -> Result<()> {
        self.sub.receive_into(out)
    }

    /// Block with a timeout. Returns `Ok(None)` if no message arrives in time.
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<Option<Vec<u8>>> {
        self.sub.receive_timeout(timeout)
    }

    /// Buffer-reusing variant of [`Self::recv_timeout`].  Returns
    /// `Ok(true)` and fills `out` on success; `Ok(false)` on timeout.
    pub fn recv_timeout_into(
        &mut self,
        timeout: Duration,
        out: &mut Vec<u8>,
    ) -> Result<bool> {
        self.sub.receive_timeout_into(timeout, out)
    }

    /// Non-blocking poll. Returns `None` immediately if no message is ready.
    pub fn try_recv(&mut self) -> Option<Vec<u8>> {
        self.sub.try_receive()
    }

    /// Buffer-reusing variant of [`Self::try_recv`].  Returns `true`
    /// and fills `out` if a message was available; returns `false`
    /// immediately otherwise.
    pub fn try_recv_into(&mut self, out: &mut Vec<u8>) -> bool {
        self.sub.try_receive_into(out)
    }

    /// Slice-target try_recv: write directly into `out`, return the
    /// number of bytes written.  See
    /// [`crate::subscriber::Subscriber::try_receive_into_slice`] for
    /// semantics + the `usize::MAX` oversize sentinel.
    pub fn try_recv_into_slice(&mut self, out: &mut [u8]) -> Option<usize> {
        self.sub.try_receive_into_slice(out)
    }

    /// WAL-aware, single-message slice receive.  See
    /// [`crate::subscriber::Subscriber::try_receive_one_into_slice`].
    pub fn try_recv_one_into_slice(&mut self, out: &mut [u8]) -> Result<Option<usize>> {
        self.sub.try_receive_one_into_slice(out)
    }

    /// Block for one wakeup without touching a payload buffer.  See
    /// [`crate::subscriber::Subscriber::wait_readable`].
    pub fn wait_readable(&mut self, timeout_ms: i32) -> Result<bool> {
        self.sub.wait_readable(timeout_ms)
    }

    /// Arm the wakeup flag for an external (`add_reader`) waiter.  Returns
    /// `true` if data is already available (read now, don't await).  See
    /// [`crate::subscriber::Subscriber::arm_wakeup`].
    pub fn arm_wakeup(&mut self) -> bool {
        self.sub.arm_wakeup()
    }

    /// Drain one pending wakeup unit (asyncio `add_reader` path).  See
    /// [`crate::subscriber::Subscriber::drain_wakeup`].
    pub fn drain_wakeup(&mut self) -> Result<bool> {
        self.sub.drain_wakeup()
    }

    /// Largest payload a single message can carry (ring slot size).
    pub fn slot_size(&self) -> u32 {
        self.sub.slot_size()
    }

    /// How many messages this subscriber is behind the producer.
    pub fn lag(&self) -> u64 {
        self.sub.lag()
    }

    /// Current read cursor position.
    pub fn cursor(&self) -> u64 {
        self.sub.cursor()
    }

    /// The underlying wakeup primitive.
    ///
    /// On Unix this is a `RawFd` (eventfd on Linux, handshake socket on
    /// macOS) — pass it to `asyncio.get_event_loop().add_reader()` or
    /// any `epoll`/`kqueue`-based poller for non-blocking receive.
    /// On Windows this is the semaphore HANDLE value as an `isize` —
    /// asyncio on Windows uses IOCP, not file descriptors, so this
    /// number is mostly diagnostic; a Windows-native async path is a
    /// planned follow-up.
    #[cfg(unix)]
    pub fn fileno(&self) -> RawFd {
        self.sub.fileno()
    }
    #[cfg(windows)]
    pub fn fileno(&self) -> isize {
        self.sub.fileno()
    }

    /// The handshake fd/handle.  On Linux this differs from
    /// [`Self::fileno`] (the eventfd) and signals **publisher
    /// disconnect** via `POLLHUP`; register both with the event loop so
    /// disconnect is detected even while idle.  On macOS it equals
    /// [`Self::fileno`].  On Windows this is the named-pipe HANDLE.
    #[cfg(unix)]
    pub fn socket_fileno(&self) -> RawFd {
        self.sub.socket_fileno()
    }
    #[cfg(windows)]
    pub fn socket_fileno(&self) -> isize {
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
