use crate::config::BusConfig;
use crate::error::{Error, Result};
use crate::ring::RingBuffer;
use crate::wal::{WalReader, WalReplayer};
use std::io;
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::io::{AsRawFd, RawFd};
#[cfg(unix)]
use std::os::unix::net::UnixStream;

#[cfg(target_os = "linux")]
use std::os::fd::OwnedFd;

#[cfg(target_os = "macos")]
use std::io::Read;

#[cfg(windows)]
use std::os::windows::io::{AsRawHandle, OwnedHandle};

/// Low-level consumer handle. Prefer [`crate::Bus::subscribe`] for most use-cases.
pub struct Subscriber {
    ring: RingBuffer,

    /// Unix handshake socket — also carries the byte-wakeup on macOS.
    #[cfg(unix)]
    sock: UnixStream,

    /// Linux: subscriber-owned read-end of the eventfd.
    #[cfg(target_os = "linux")]
    efd: OwnedFd,

    /// Windows: handshake pipe (kept open so peer-disconnect is observable
    /// via `WaitForMultipleObjects` returning the pipe slot).
    #[cfg(windows)]
    pipe: OwnedHandle,

    /// Windows: our half of the semaphore.  We pass a handle value to
    /// the publisher during the handshake; the publisher then
    /// `DuplicateHandle`'s its own copy into its process.  We retain
    /// this one for `WaitForMultipleObjects`.
    #[cfg(windows)]
    sem: OwnedHandle,

    cursor: u64,
    cursor_idx: usize,

    /// Publisher generation at connect time.  If the publisher crashes and a
    /// new one bumps the generation, the next wakeup observes the mismatch
    /// and `receive` returns `UnexpectedEof` so the iterator terminates
    /// instead of reading from a logically-reset ring.
    generation: u64,

    /// WAL replayer feeding records from the on-disk log when the
    /// subscriber connected with a cursor behind the ring's oldest
    /// in-ring slot.  `receive()` pulls from this first; on exhaustion
    /// the field is dropped and reads continue from the live ring.
    wal_replay: Option<WalReplayer>,
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        self.ring.release_cursor(self.cursor_idx);
    }
}

/// Where the subscriber's cursor starts.  Used by `Subscriber::connect_with`.
#[derive(Clone, Copy, Debug)]
pub enum StartPos {
    /// Start at the current tail — receive only messages published from now on.
    Now,
    /// Start `n_messages_back` behind the current tail (capped at ring capacity).
    /// Best-effort replay of recent in-ring history.
    HistoryBack(u64),
    /// Start at an explicit cursor value.  Returns
    /// [`crate::Error::CursorTooOld`] if the cursor is older than the
    /// oldest in-ring slot at connect time.
    Explicit(u64),
}

impl Subscriber {
    /// Connect to a named bus, retrying until `timeout` expires.  Receives
    /// only messages published from the connect moment forward.
    ///
    /// Shorthand for [`Subscriber::connect_with`] with [`StartPos::Now`].
    pub fn connect(name: &str, cfg: &BusConfig, timeout: Duration) -> Result<Self> {
        Self::connect_with(name, cfg, timeout, StartPos::Now)
    }

    /// Connect with a custom start position.
    ///
    /// The cursor is claimed in the ring *before* completing the handshake,
    /// so that by the time `Publisher::wait_for_subscribers` returns,
    /// the cursor is already visible to the producer's backpressure check.
    pub fn connect_with(
        name: &str,
        cfg: &BusConfig,
        timeout: Duration,
        start: StartPos,
    ) -> Result<Self> {
        let dir = cfg.base_dir.join(name);
        let ring_path = dir.join("ring.mmap");
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

        // Claim cursor before transport handshake so the producer sees our
        // position as soon as it accepts the connection.
        let cursor = ring.current_tail();
        let cursor_idx = ring
            .claim_cursor(cursor)
            .ok_or(Error::TooManySubscribers(ring.max_subscribers))?;

        // ── Linux: eventfd + Unix socket + SCM_RIGHTS handshake ──────────────
        #[cfg(target_os = "linux")]
        let (sock, efd) = {
            let sock_path = dir.join("signal.sock");
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
            if let Err(e) = crate::waker::linux::send_fd(&sock, efd.as_raw_fd(), cursor_idx as u32)
            {
                ring.release_cursor(cursor_idx);
                return Err(Error::Io(e));
            }
            (sock, efd)
        };

        // ── macOS: Unix socket only; the publisher byte-wakes per message ────
        #[cfg(target_os = "macos")]
        let sock = {
            let sock_path = dir.join("signal.sock");
            let mut s = loop {
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
            // Handshake: send our cursor_idx (4 bytes LE) so the publisher
            // can address our wakeup flag.  Subsequent traffic on this
            // socket is the 1-byte per-message wakeup.
            use std::io::Write;
            if let Err(e) = s.write_all(&(cursor_idx as u32).to_le_bytes()) {
                ring.release_cursor(cursor_idx);
                return Err(Error::Io(e));
            }
            s
        };

        // ── Windows: named pipe + semaphore handshake ────────────────────────
        #[cfg(windows)]
        let (pipe, sem) = {
            let pipe_name = crate::waker::windows::pipe_name(name);
            let sem = match crate::waker::windows::create_semaphore() {
                Ok(h) => h,
                Err(e) => {
                    ring.release_cursor(cursor_idx);
                    return Err(Error::Io(e));
                }
            };
            let pipe = loop {
                match crate::waker::windows::connect_pipe(&pipe_name) {
                    Ok(h) => break h,
                    Err(_) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => {
                        ring.release_cursor(cursor_idx);
                        return Err(Error::Timeout(name.to_owned()));
                    }
                }
            };
            // Send our (pid, sem, cursor_idx) so the publisher can
            // DuplicateHandle and address our wakeup flag.
            if let Err(e) = crate::waker::windows::send_handshake(
                pipe.as_raw_handle() as crate::waker::windows::RawWinHandle,
                sem.as_raw_handle() as crate::waker::windows::RawWinHandle,
                cursor_idx as u32,
            ) {
                ring.release_cursor(cursor_idx);
                return Err(Error::Io(e));
            }
            (pipe, sem)
        };

        // Re-synchronise after the handshake completes.  If the publisher
        // restarted between our initial `current_tail` read above and the
        // handshake, the tail was reset to 0 — but our cursor still
        // points at the *old* tail and would block forever waiting for
        // messages that never arrive.
        let tail = ring.current_tail();
        let (cursor, wal_replay) = match start {
            StartPos::Now => (tail, None),
            StartPos::HistoryBack(n) => (tail.saturating_sub(n), None),
            StartPos::Explicit(c) => {
                let ring_oldest = tail.saturating_sub(ring.capacity as u64);
                if c >= ring_oldest {
                    (c, None)
                } else {
                    // Behind the ring — try the WAL.  We don't claim a
                    // cursor at `c` because that would block the
                    // publisher's backpressure on a position the ring
                    // can no longer reach; we leave cursor_idx pointing
                    // at the live tail and read the missing prefix from
                    // the WAL.  An on-disk WAL with no segments (or
                    // missing entirely) is treated the same as no WAL
                    // at all — surface CursorTooOld with the ring's
                    // oldest visible cursor.
                    let wal_reader = WalReader::open(&dir)
                        .ok()
                        .filter(|wr| wr.oldest_cursor().is_some());
                    match wal_reader {
                        Some(wr) => match wr.read_from(c) {
                            Ok(replayer) => (c, Some(replayer)),
                            Err(crate::wal::WalError::CursorTooOld { requested, oldest }) => {
                                ring.release_cursor(cursor_idx);
                                return Err(Error::CursorTooOld { requested, oldest });
                            }
                            Err(e) => {
                                ring.release_cursor(cursor_idx);
                                return Err(Error::Wal(e));
                            }
                        },
                        None => {
                            ring.release_cursor(cursor_idx);
                            return Err(Error::CursorTooOld {
                                requested: c,
                                oldest: ring_oldest,
                            });
                        }
                    }
                }
            }
        };
        // During WAL replay the live cursor is parked at the current
        // tail so the publisher's backpressure check ignores us; we
        // re-sync to the real cursor when replay catches up.
        let live_cursor = if wal_replay.is_some() { tail } else { cursor };
        ring.set_cursor(cursor_idx, live_cursor);
        let generation = ring.generation();

        tracing::info!(
            target: "mmbus::subscriber",
            topic = name,
            cursor,
            cursor_idx,
            generation,
            replaying_via_wal = wal_replay.is_some(),
            "subscriber connected",
        );
        Ok(Self {
            ring,
            #[cfg(unix)]
            sock,
            #[cfg(target_os = "linux")]
            efd,
            #[cfg(windows)]
            pipe,
            #[cfg(windows)]
            sem,
            cursor,
            cursor_idx,
            generation,
            wal_replay,
        })
    }

    /// Block until the next message arrives.
    pub fn receive(&mut self) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        self.receive_into(&mut out)?;
        Ok(out)
    }

    /// Block until the next message arrives, reusing the caller's
    /// buffer.  The buffer is `clear()`'d on entry; on success it
    /// holds the new payload.  Saves one allocation per receive vs
    /// [`Self::receive`] — used by the Python binding (which keeps a
    /// reusable scratch buffer on the wrapper) and by any Rust caller
    /// reading in a tight loop.
    pub fn receive_into(&mut self, out: &mut Vec<u8>) -> Result<()> {
        out.clear();
        if let Some(payload) = self.next_wal_record()? {
            out.extend_from_slice(&payload);
            return Ok(());
        }
        loop {
            if let Some(new_cursor) = self.ring.try_receive(self.cursor_idx, self.cursor, out) {
                self.cursor = new_cursor;
                return Ok(());
            }
            // Ring empty: arm the wakeup flag, re-check, then sleep.
            // `wait_readable` performs the eventcount handshake so the
            // publisher knows to wake us (it coalesces wakeups otherwise).
            self.wait_readable(-1)?;
        }
    }

    /// Block with a timeout. Returns `Ok(None)` if the timeout elapses.
    pub fn receive_timeout(&mut self, timeout: Duration) -> Result<Option<Vec<u8>>> {
        let mut out = Vec::new();
        match self.receive_timeout_into(timeout, &mut out)? {
            true => Ok(Some(out)),
            false => Ok(None),
        }
    }

    /// Buffer-reusing variant of [`Self::receive_timeout`].  Returns
    /// `Ok(true)` if a message was received and written to `out`,
    /// `Ok(false)` on timeout.  `out` is `clear()`'d on entry.
    pub fn receive_timeout_into(&mut self, timeout: Duration, out: &mut Vec<u8>) -> Result<bool> {
        out.clear();
        if let Some(payload) = self.next_wal_record()? {
            out.extend_from_slice(&payload);
            return Ok(true);
        }
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(new_cursor) = self.ring.try_receive(self.cursor_idx, self.cursor, out) {
                self.cursor = new_cursor;
                return Ok(true);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(false);
            }
            let ms = remaining.as_millis().min(i32::MAX as u128) as i32;
            // eventcount handshake (see `wait_readable`); Ok(false) = timed out.
            if !self.wait_readable(ms)? {
                return Ok(false);
            }
        }
    }

    /// Non-blocking poll. Returns `None` if no message is ready.
    pub fn try_receive(&mut self) -> Option<Vec<u8>> {
        let mut out = Vec::new();
        if self.try_receive_into(&mut out) {
            Some(out)
        } else {
            None
        }
    }

    /// Buffer-reusing variant of [`Self::try_receive`].  Returns
    /// `true` and fills `out` if a message was available; returns
    /// `false` immediately if not.  `out` is `clear()`'d on entry.
    pub fn try_receive_into(&mut self, out: &mut Vec<u8>) -> bool {
        out.clear();
        if let Ok(Some(payload)) = self.next_wal_record() {
            out.extend_from_slice(&payload);
            return true;
        }
        if let Some(new_cursor) = self.ring.try_receive(self.cursor_idx, self.cursor, out) {
            self.cursor = new_cursor;
            true
        } else {
            false
        }
    }

    /// Slice-target try_receive: write the next record directly into
    /// `out` (no intermediate `Vec`) and return the number of bytes
    /// written.  `None` means no message available; `Some(0)` is
    /// impossible (zero-length payloads still consume the slot's
    /// framing, but the API reports the actual payload bytes — empty
    /// payloads return `Some(0)`).  Returns `Some(usize::MAX)` if the
    /// payload is larger than `out`; the cursor advances so the
    /// caller's loop doesn't stick on the oversize record.
    ///
    /// Used by the Python `recv_into_buffer` path so numpy / bytearray
    /// users skip the `Vec<u8>` + `PyBytes` allocations entirely.
    /// Does NOT consult the WAL replayer — slice reads are for
    /// live-ring fast paths only; WAL replay continues to use
    /// `receive_into` / `try_receive_into`.
    pub fn try_receive_into_slice(&mut self, out: &mut [u8]) -> Option<usize> {
        match self
            .ring
            .try_receive_into_slice(self.cursor_idx, self.cursor, out)
        {
            Some((new_cursor, bytes_written)) => {
                self.cursor = new_cursor;
                Some(bytes_written)
            }
            None => None,
        }
    }

    /// WAL-aware, single-message variant of [`Self::try_receive_into_slice`].
    /// Consults the WAL replayer first (catch-up), then the live ring, and
    /// writes the next record's payload directly into `out`.
    ///
    /// Returns `Ok(Some(n))` with `n` bytes written, `Ok(None)` if nothing
    /// is available, or `Err(Error::TooLarge)` if the payload is larger than
    /// `out`.  On a live-ring oversize the record IS consumed (the seqlock
    /// path cannot peek without advancing) — size `out` to [`Self::slot_size`]
    /// so this never fires.  WAL-replay oversize reports the exact payload
    /// size; ring oversize reports `slot_size` (the maximum a message can be).
    pub fn try_receive_one_into_slice(&mut self, out: &mut [u8]) -> Result<Option<usize>> {
        if let Some(payload) = self.next_wal_record()? {
            if payload.len() > out.len() {
                return Err(Error::TooLarge {
                    size: payload.len(),
                    max: out.len(),
                });
            }
            out[..payload.len()].copy_from_slice(&payload);
            return Ok(Some(payload.len()));
        }
        match self.try_receive_into_slice(out) {
            Some(n) if n == usize::MAX => Err(Error::TooLarge {
                size: self.ring.slot_payload_size as usize,
                max: out.len(),
            }),
            Some(n) => Ok(Some(n)),
            None => Ok(None),
        }
    }

    /// Eventcount wait: the caller has just observed the ring empty.
    /// Announce intent to sleep (set the wakeup flag so the publisher will
    /// wake us — it coalesces wakeups to subscribers that don't), re-check
    /// the ring to close the publish-between-check-and-sleep race, then
    /// block.  `timeout_ms = -1` blocks indefinitely.
    ///
    /// Returns `Ok(true)` if the caller should retry its read (the re-check
    /// found data, or we were woken); `Ok(false)` on timeout.
    ///
    /// Also the "wait" half of the slice-target receive loop: the Python
    /// binding holds a borrowed buffer pointer across this call (GIL
    /// released) and reads into it only afterwards (GIL held), so the wait
    /// must not access that buffer — it doesn't.
    pub fn wait_readable(&mut self, timeout_ms: i32) -> Result<bool> {
        self.ring.set_wakeflag(self.cursor_idx);
        // StoreLoad barrier: order the flag store before the tail load
        // below, mirroring the publisher's fence between its tail store and
        // its flag swap.  Together they guarantee no missed wakeup — either
        // we observe the new tail here, or the publisher observes our flag
        // and wakes us.
        std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
        if self.cursor < self.ring.current_tail() {
            self.ring.clear_wakeflag(self.cursor_idx);
            return Ok(true);
        }
        let r = self.wait_wakeup(timeout_ms);
        self.ring.clear_wakeflag(self.cursor_idx);
        match r {
            Ok(()) => Ok(true),
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut =>
            {
                Ok(false)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Arm the wakeup flag for an event-loop (`add_reader`) waiter that
    /// sleeps outside this process (asyncio).  Sets the flag + the same
    /// SeqCst barrier as [`Self::wait_readable`], then re-checks the ring.
    ///
    /// Returns `true` if data is already available (caller should read
    /// immediately, not await); `false` if the flag is armed and the caller
    /// should await fd readability — the publisher's next publish will fire
    /// the wakeup and clear the flag.
    pub fn arm_wakeup(&mut self) -> bool {
        self.ring.set_wakeflag(self.cursor_idx);
        std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
        if self.cursor < self.ring.current_tail() {
            self.ring.clear_wakeflag(self.cursor_idx);
            true
        } else {
            false
        }
    }

    /// The ring's fixed per-slot payload capacity — the largest payload a
    /// single message can carry.  Size a `recv_into` buffer to this to
    /// guarantee no message is ever rejected as too large.
    pub fn slot_size(&self) -> u32 {
        self.ring.slot_payload_size
    }

    /// Pull the next record from the WAL replayer (if active) and
    /// advance `self.cursor`.  When the replayer is exhausted, drop
    /// it and re-sync the live ring cursor to our true position so
    /// subsequent ring reads pick up where the WAL left off.
    ///
    /// Returns `Ok(None)` when there is no WAL replay in flight OR
    /// the replayer has just been drained — in either case the caller
    /// falls through to the live-ring read path.
    fn next_wal_record(&mut self) -> Result<Option<Vec<u8>>> {
        let Some(replayer) = self.wal_replay.as_mut() else {
            return Ok(None);
        };
        match replayer.next() {
            Some(Ok(record)) => {
                self.cursor = record.cursor + 1;
                Ok(Some(record.payload))
            }
            Some(Err(e)) => {
                self.wal_replay = None;
                Err(Error::Wal(e))
            }
            None => {
                // Replayer drained — promote to live-ring reads.
                // Re-sync our claimed cursor slot so the publisher
                // sees our real position from here on.
                self.wal_replay = None;
                self.ring.set_cursor(self.cursor_idx, self.cursor);
                Ok(None)
            }
        }
    }

    /// How many messages are currently ahead of this subscriber's read position.
    pub fn lag(&self) -> u64 {
        self.ring.current_tail().saturating_sub(self.cursor)
    }

    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// The underlying wakeup primitive.  On Unix this is a `RawFd`
    /// (eventfd on Linux, the handshake socket on macOS) that asyncio's
    /// `loop.add_reader` can register.  On Windows this is the semaphore
    /// HANDLE value — asyncio on Windows uses IOCP, not file descriptors,
    /// so this number is mostly useful for diagnostics; the async path
    /// on Windows is planned as a follow-up.
    #[cfg(unix)]
    pub fn fileno(&self) -> RawFd {
        #[cfg(target_os = "linux")]
        {
            self.efd.as_raw_fd()
        }
        #[cfg(target_os = "macos")]
        {
            self.sock.as_raw_fd()
        }
    }
    #[cfg(windows)]
    pub fn fileno(&self) -> isize {
        self.sem.as_raw_handle() as isize
    }

    /// The handshake fd/handle. On Linux this differs from
    /// [`Self::fileno`] (the eventfd) and signals **disconnect** via
    /// `POLLHUP` — register it with the event loop alongside
    /// [`Self::fileno`] so publisher death is detected even while idle.
    /// On macOS it equals [`Self::fileno`].  On Windows this is the
    /// named-pipe handle value (also distinct from `fileno()`).
    #[cfg(unix)]
    pub fn socket_fileno(&self) -> RawFd {
        self.sock.as_raw_fd()
    }
    #[cfg(windows)]
    pub fn socket_fileno(&self) -> isize {
        self.pipe.as_raw_handle() as isize
    }

    /// Non-blocking: drain at most one wakeup signal and attempt one ring
    /// read.  Designed for event-loop callbacks (`asyncio.add_reader`).
    pub fn poll_recv(&mut self) -> Result<Option<Vec<u8>>> {
        // WAL replay records are available without any kernel wakeup —
        // serve them first so a subscriber catching up from disk isn't
        // gated on producer-side wakeups it never receives.
        if let Some(payload) = self.next_wal_record()? {
            return Ok(Some(payload));
        }
        if !self.try_drain_wakeup()? {
            return Ok(None);
        }
        let current_gen = self.ring.generation();
        if current_gen != self.generation {
            tracing::warn!(
                target: "mmbus::subscriber",
                connected_generation = self.generation,
                current_generation = current_gen,
                cursor_idx = self.cursor_idx,
                "publisher restarted (generation changed); ending subscription",
            );
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "publisher restarted (generation changed)",
            )));
        }
        Ok(self.try_receive())
    }

    /// Public, `Result`-typed wrapper over the internal `try_drain_wakeup`
    /// for the asyncio `add_reader` path: clear one pending wakeup unit so the
    /// (level-triggered) fd stops signalling, then read the ring separately
    /// via `try_receive`.  Returns `Ok(true)` if a unit was drained,
    /// `Ok(false)` if none was pending; `Err` on publisher disconnect.
    pub fn drain_wakeup(&mut self) -> Result<bool> {
        self.try_drain_wakeup().map_err(Into::into)
    }

    /// Drain exactly one wakeup unit without blocking.
    fn try_drain_wakeup(&mut self) -> io::Result<bool> {
        #[cfg(target_os = "linux")]
        {
            match crate::waker::linux::eventfd_drain(self.efd.as_raw_fd()) {
                Ok(_) => Ok(true),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(false),
                Err(e) => Err(e),
            }
        }
        #[cfg(target_os = "macos")]
        {
            let mut byte = 0u8;
            // SAFETY: self.sock is an owned UnixStream (fd open); &mut byte
            // points to one stack byte; libc::recv writes at most 1 byte
            // (we tell it length 1).  MSG_DONTWAIT makes the call return
            // immediately if the socket has no data.
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
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "publisher closed",
                ))
            } else {
                let e = io::Error::last_os_error();
                if e.kind() == io::ErrorKind::WouldBlock {
                    Ok(false)
                } else {
                    Err(e)
                }
            }
        }
        #[cfg(windows)]
        {
            crate::waker::windows::semaphore_drain(
                self.sem.as_raw_handle() as crate::waker::windows::RawWinHandle
            )
        }
    }

    // ── Internal wakeup helpers ───────────────────────────────────────────────

    /// Wait for one wakeup signal. `timeout_ms = -1` blocks indefinitely.
    fn wait_wakeup(&mut self, timeout_ms: i32) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            crate::waker::linux::poll_wakeup(
                self.efd.as_raw_fd(),
                self.sock.as_raw_fd(),
                timeout_ms,
            )?;
            crate::waker::linux::eventfd_drain(self.efd.as_raw_fd())?;
        }
        #[cfg(target_os = "macos")]
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
        #[cfg(windows)]
        {
            crate::waker::windows::wait_wakeup(
                self.sem.as_raw_handle() as crate::waker::windows::RawWinHandle,
                self.pipe.as_raw_handle() as crate::waker::windows::RawWinHandle,
                timeout_ms,
            )?;
        }
        // Publisher-restart check: a fresh publisher reused our mmap and
        // bumped the generation.
        if self.ring.generation() != self.generation {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "publisher restarted (generation changed)",
            ));
        }
        Ok(())
    }
}
