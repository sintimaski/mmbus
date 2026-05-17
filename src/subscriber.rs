use crate::config::BusConfig;
use crate::error::{Error, Result};
use crate::ring::RingBuffer;
use std::io;
use std::os::unix::io::{AsRawFd, RawFd};
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

#[cfg(target_os = "linux")]
use std::os::fd::OwnedFd;

#[cfg(not(target_os = "linux"))]
use std::io::Read;

/// Low-level consumer handle. Prefer [`crate::Bus::subscribe`] for most use-cases.
pub struct Subscriber {
    ring: RingBuffer,
    sock: UnixStream,

    /// On Linux: the subscriber-owned read-end of the eventfd.
    #[cfg(target_os = "linux")]
    efd: OwnedFd,

    cursor: u64,
    cursor_idx: usize,

    /// Publisher generation at connect time.  If the publisher crashes and a
    /// new one bumps the generation, the next wakeup observes the mismatch
    /// and `receive` returns `UnexpectedEof` so the iterator terminates
    /// instead of reading from a logically-reset ring.
    generation: u64,
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        self.ring.release_cursor(self.cursor_idx);
    }
}

impl Subscriber {
    /// Connect to a named bus, retrying until `timeout` expires.
    ///
    /// The cursor is claimed in the ring *before* completing the socket
    /// handshake, so that by the time `Publisher::wait_for_subscribers` returns,
    /// the cursor is already visible to the producer's backpressure check.
    ///
    /// On Linux an `eventfd(2)` is created before connecting and its write-end
    /// is passed to the publisher via `SCM_RIGHTS` over the handshake socket.
    pub fn connect(name: &str, cfg: &BusConfig, timeout: Duration) -> Result<Self> {
        let dir = cfg.base_dir.join(name);
        let ring_path = dir.join("ring.mmap");
        let sock_path = dir.join("signal.sock");
        let deadline = Instant::now() + timeout;

        let ring = loop {
            match RingBuffer::open(&ring_path) {
                Ok(r) => break r,
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(_) => return Err(Error::Timeout(name.to_owned())),
            }
        };

        // Claim cursor before socket connect so the producer sees our position
        // as soon as it accepts the connection.
        let cursor = ring.current_tail();
        let cursor_idx = ring
            .claim_cursor(cursor)
            .ok_or(Error::TooManySubscribers(ring.max_subscribers))?;

        // On Linux: create the eventfd now so we can pass it during the
        // socket handshake.
        #[cfg(target_os = "linux")]
        let efd = match crate::waker::linux::create_eventfd() {
            Ok(fd) => fd,
            Err(e) => {
                ring.release_cursor(cursor_idx);
                return Err(Error::Io(e));
            }
        };

        let sock = loop {
            match UnixStream::connect(&sock_path) {
                Ok(s) => break s,
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(_) => {
                    ring.release_cursor(cursor_idx);
                    return Err(Error::Timeout(name.to_owned()));
                }
            }
        };

        // Pass the eventfd write-end to the publisher so it can wake us.
        #[cfg(target_os = "linux")]
        if let Err(e) = crate::waker::linux::send_fd(&sock, efd.as_raw_fd()) {
            ring.release_cursor(cursor_idx);
            return Err(Error::Io(e));
        }

        // Re-synchronise after the handshake completes.  If the publisher
        // restarted between our initial `current_tail` read above and the
        // socket connect, the tail was reset to 0 — but our cursor still
        // points at the *old* tail and would block forever waiting for
        // messages that never arrive.  Reading tail + generation now
        // captures the post-handshake state.
        let cursor = ring.current_tail();
        ring.set_cursor(cursor_idx, cursor);
        let generation = ring.generation();

        Ok(Self {
            ring,
            sock,
            #[cfg(target_os = "linux")]
            efd,
            cursor,
            cursor_idx,
            generation,
        })
    }

    /// Block until the next message arrives.
    pub fn receive(&mut self) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        loop {
            self.wait_wakeup(-1)?;
            if let Some(new_cursor) =
                self.ring.try_receive(self.cursor_idx, self.cursor, &mut out)
            {
                self.cursor = new_cursor;
                return Ok(out);
            }
            // Wakeup consumed but ring slot not yet visible (e.g. DropOldest
            // race). Loop for the next wakeup.
        }
    }

    /// Block with a timeout. Returns `Ok(None)` if the timeout elapses.
    pub fn receive_timeout(&mut self, timeout: Duration) -> Result<Option<Vec<u8>>> {
        let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as i32;
        let mut out = Vec::new();
        loop {
            match self.wait_wakeup(timeout_ms) {
                Ok(()) => {}
                Err(e)
                    if e.kind() == io::ErrorKind::WouldBlock
                        || e.kind() == io::ErrorKind::TimedOut =>
                {
                    return Ok(None);
                }
                Err(e) => return Err(e.into()),
            }
            if let Some(new_cursor) =
                self.ring.try_receive(self.cursor_idx, self.cursor, &mut out)
            {
                self.cursor = new_cursor;
                return Ok(Some(out));
            }
        }
    }

    /// Non-blocking poll. Returns `None` if no message is ready.
    pub fn try_receive(&mut self) -> Option<Vec<u8>> {
        let mut out = Vec::new();
        if let Some(new_cursor) = self.ring.try_receive(self.cursor_idx, self.cursor, &mut out) {
            self.cursor = new_cursor;
            Some(out)
        } else {
            None
        }
    }

    /// How many messages are currently ahead of this subscriber's read position.
    pub fn lag(&self) -> u64 {
        self.ring.current_tail().saturating_sub(self.cursor)
    }

    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// The underlying wakeup fd: eventfd on Linux, Unix socket on macOS.
    /// Becomes readable when at least one message is available.
    pub fn fileno(&self) -> RawFd {
        #[cfg(target_os = "linux")]
        {
            self.efd.as_raw_fd()
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.sock.as_raw_fd()
        }
    }

    /// The handshake socket fd. On Linux this differs from [`Self::fileno`] (the
    /// eventfd) and signals **disconnect** via `POLLHUP` — register it with
    /// the event loop alongside [`Self::fileno`] so publisher death is detected
    /// even while idle. On macOS it equals [`Self::fileno`].
    pub fn socket_fileno(&self) -> RawFd {
        self.sock.as_raw_fd()
    }

    /// Non-blocking: drain at most one wakeup signal and attempt one ring
    /// read.  Designed for event-loop callbacks (`asyncio.add_reader`).
    ///
    /// * `Ok(Some(msg))` — a message was received.
    /// * `Ok(None)`      — no wakeup was pending (spurious wake or already drained).
    /// * `Err(_)`        — publisher disconnected or I/O error.
    pub fn poll_recv(&mut self) -> Result<Option<Vec<u8>>> {
        if !self.try_drain_wakeup()? {
            return Ok(None);
        }
        if self.ring.generation() != self.generation {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "publisher restarted (generation changed)",
            )));
        }
        Ok(self.try_receive())
    }

    /// Drain exactly one wakeup unit without blocking.  Returns `Ok(true)`
    /// if a wakeup was consumed, `Ok(false)` if none was pending.
    fn try_drain_wakeup(&mut self) -> io::Result<bool> {
        #[cfg(target_os = "linux")]
        {
            // eventfd is EFD_NONBLOCK | EFD_SEMAPHORE — read decrements by 1.
            match crate::waker::linux::eventfd_drain(self.efd.as_raw_fd()) {
                Ok(_) => Ok(true),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(false),
                Err(e) => Err(e),
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            // Use MSG_DONTWAIT so we don't have to toggle O_NONBLOCK on a
            // socket that the blocking `receive()` path also uses.
            let mut byte = 0u8;
            let ret = unsafe {
                libc::recv(
                    self.sock.as_raw_fd(),
                    &mut byte as *mut u8 as *mut libc::c_void,
                    1,
                    libc::MSG_DONTWAIT,
                )
            };
            if ret == 1 {
                Ok(true)
            } else if ret == 0 {
                // EOF: peer closed.
                Err(io::Error::new(io::ErrorKind::UnexpectedEof, "publisher closed"))
            } else {
                let e = io::Error::last_os_error();
                if e.kind() == io::ErrorKind::WouldBlock {
                    Ok(false)
                } else {
                    Err(e)
                }
            }
        }
    }

    // ── Internal wakeup helpers ───────────────────────────────────────────────

    /// Wait for one wakeup signal. `timeout_ms = -1` blocks indefinitely.
    /// On Linux: `poll(2)` on eventfd + socket (disconnect detection).
    /// On macOS: `read_exact(1)` on the socket (with optional read timeout).
    fn wait_wakeup(&mut self, timeout_ms: i32) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            crate::waker::linux::poll_wakeup(
                self.efd.as_raw_fd(),
                self.sock.as_raw_fd(),
                timeout_ms,
            )?;
            // Drain the eventfd counter (EFD_SEMAPHORE: returns 1, decrements
            // by 1) so the next poll blocks if no further wakeups are pending.
            crate::waker::linux::eventfd_drain(self.efd.as_raw_fd())?;
        }
        #[cfg(not(target_os = "linux"))]
        {
            if timeout_ms < 0 {
                let mut b = [0u8; 1];
                self.sock.read_exact(&mut b)?;
            } else {
                let t = Duration::from_millis(timeout_ms as u64);
                self.sock.set_read_timeout(Some(t))?;
                let mut b = [0u8; 1];
                let r = self.sock.read_exact(&mut b);
                let _ = self.sock.set_read_timeout(None);
                r?;
            }
        }
        // Publisher-restart check: a fresh publisher reused our mmap and
        // bumped the generation.  Report EOF so the iterator terminates
        // cleanly instead of reading from the logically-reset ring.
        if self.ring.generation() != self.generation {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "publisher restarted (generation changed)",
            ));
        }
        Ok(())
    }
}
