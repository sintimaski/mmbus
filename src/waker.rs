/// Linux-only eventfd wakeup helpers + SCM_RIGHTS fd-passing.
///
/// On Linux each subscriber creates an `eventfd(2)` and passes the write-end
/// to the publisher via SCM_RIGHTS over the handshake Unix socket.  The
/// publisher writes `1` to the eventfd on every message; the subscriber uses
/// `poll(2)` on *both* the eventfd and the handshake socket so that publisher
/// death (POLLHUP on the socket) is detected even when no messages are in
/// flight.
///
/// On macOS the module is empty — the Unix socket byte-per-message scheme is
/// used directly in `bus.rs`.
#[cfg(target_os = "linux")]
pub(crate) mod linux {
    use std::io;
    use std::os::fd::{FromRawFd, OwnedFd};
    use std::os::unix::io::{AsRawFd, RawFd};
    use std::os::unix::net::UnixStream;

    // ── eventfd primitives ────────────────────────────────────────────────────

    /// Create a non-blocking, close-on-exec eventfd in semaphore mode.
    ///
    /// `EFD_SEMAPHORE` makes each `read()` return 1 and decrement the counter
    /// by 1 — matching the macOS "one byte per message" socket semantics so
    /// `N` publishes produce `N` distinct wakeups (without it the counter is
    /// coalesced and the receiver loses messages already in the ring).
    pub fn create_eventfd() -> io::Result<OwnedFd> {
        // SAFETY: libc::eventfd is always safe to call with valid flag
        // constants; returns a new fd or -1.
        let fd = unsafe {
            libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC | libc::EFD_SEMAPHORE)
        };
        if fd < 0 {
            Err(io::Error::last_os_error())
        } else {
            // SAFETY: fd is freshly created by eventfd above and not
            // duplicated; we have exclusive ownership.
            Ok(unsafe { OwnedFd::from_raw_fd(fd) })
        }
    }

    /// Increment the eventfd counter by 1.  Returns false on EBADF / EPIPE
    /// (peer has closed its copy — subscriber disconnected).
    pub fn eventfd_wake(fd: RawFd) -> bool {
        let val: u64 = 1;
        // SAFETY: &val points to 8 bytes on the stack; we tell libc::write
        // the exact size; fd is the caller's responsibility to keep open
        // for the duration of the call.
        unsafe { libc::write(fd, &val as *const u64 as *const libc::c_void, 8) == 8 }
    }

    /// Consume the current eventfd counter value (resets to 0).
    /// Fails with `WouldBlock` if counter is 0 (fd is `EFD_NONBLOCK`).
    pub fn eventfd_drain(fd: RawFd) -> io::Result<u64> {
        let mut val: u64 = 0;
        // SAFETY: &mut val points to 8 bytes on the stack; libc::read writes
        // exactly that many bytes for an eventfd (or returns -1).
        let ret = unsafe { libc::read(fd, &mut val as *mut u64 as *mut libc::c_void, 8) };
        if ret == 8 {
            Ok(val)
        } else {
            Err(io::Error::last_os_error())
        }
    }

    // ── poll helper ───────────────────────────────────────────────────────────

    /// Block until `efd` is readable (≥1 wakeup pending) **or** `sock` closes.
    ///
    /// * `timeout_ms = -1`  → wait forever
    /// * `timeout_ms ≥ 0`   → return `Err(WouldBlock)` after that many ms
    ///
    /// Returns `Err(UnexpectedEof)` when the publisher has disconnected (socket
    /// POLLHUP/POLLERR), so callers can propagate it as `StopIteration`.
    pub fn poll_wakeup(efd: RawFd, sock: RawFd, timeout_ms: i32) -> io::Result<()> {
        let mut fds = [
            libc::pollfd { fd: efd, events: libc::POLLIN, revents: 0 },
            libc::pollfd { fd: sock, events: libc::POLLHUP | libc::POLLERR, revents: 0 },
        ];
        loop {
            // SAFETY: fds is a 2-element stack array; we pass its base
            // pointer + length 2.  libc::poll writes to revents fields
            // in-place.  efd / sock fds are the caller's responsibility.
            let n = unsafe { libc::poll(fds.as_mut_ptr(), 2, timeout_ms) };
            if n < 0 {
                let e = io::Error::last_os_error();
                if e.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(e);
            }
            if n == 0 {
                return Err(io::Error::new(io::ErrorKind::WouldBlock, "poll timeout"));
            }
            if fds[1].revents & (libc::POLLHUP | libc::POLLERR) != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "publisher disconnected",
                ));
            }
            return Ok(());
        }
    }

    // ── SCM_RIGHTS fd-passing ─────────────────────────────────────────────────

    /// Send `fd` to `sock` as SCM_RIGHTS ancillary data with a 1-byte payload.
    pub fn send_fd(sock: &UnixStream, fd: RawFd) -> io::Result<()> {
        let dummy: u8 = 1;
        let iov =
            libc::iovec { iov_base: &dummy as *const u8 as *mut libc::c_void, iov_len: 1 };
        let fd_size = std::mem::size_of::<libc::c_int>() as u32;
        // SAFETY: CMSG_SPACE is a const-fn-equivalent macro that just
        // computes a size; it has no side effects.
        let cmsg_space = unsafe { libc::CMSG_SPACE(fd_size) } as usize;
        let mut buf = vec![0u8; cmsg_space];
        let msg = libc::msghdr {
            msg_name: std::ptr::null_mut(),
            msg_namelen: 0,
            msg_iov: &iov as *const libc::iovec as *mut libc::iovec,
            msg_iovlen: 1,
            msg_control: buf.as_mut_ptr() as *mut libc::c_void,
            msg_controllen: cmsg_space,
            msg_flags: 0,
        };
        // SAFETY: cmsg_buf has the size CMSG_SPACE(fd_size) computed by
        // libc; CMSG_FIRSTHDR returns the first cmsghdr in that buffer,
        // which fits because we allocated for one fd-passing cmsg.
        // CMSG_DATA points into the same buffer after the cmsghdr
        // prefix; we write one c_int there. iov references `dummy` on
        // the stack which lives until `sendmsg` returns.
        unsafe {
            let cmsg = libc::CMSG_FIRSTHDR(&msg) as *mut libc::cmsghdr;
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_RIGHTS;
            (*cmsg).cmsg_len = libc::CMSG_LEN(fd_size) as libc::size_t;
            std::ptr::write(libc::CMSG_DATA(cmsg) as *mut libc::c_int, fd);
            if libc::sendmsg(sock.as_raw_fd(), &msg, libc::MSG_NOSIGNAL) < 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(())
    }

    /// Receive an fd sent via SCM_RIGHTS from `sock`.  The received fd is a
    /// fresh file-descriptor number in the caller's process (kernel dups it).
    pub fn recv_fd(sock: &UnixStream) -> io::Result<OwnedFd> {
        let mut dummy: u8 = 0;
        let mut iov =
            libc::iovec { iov_base: &mut dummy as *mut u8 as *mut libc::c_void, iov_len: 1 };
        let fd_size = std::mem::size_of::<libc::c_int>() as u32;
        // SAFETY: pure size computation, no side effects.
        let cmsg_space = unsafe { libc::CMSG_SPACE(fd_size) } as usize;
        let mut buf = vec![0u8; cmsg_space];
        let mut msg = libc::msghdr {
            msg_name: std::ptr::null_mut(),
            msg_namelen: 0,
            msg_iov: &mut iov as *mut libc::iovec,
            msg_iovlen: 1,
            msg_control: buf.as_mut_ptr() as *mut libc::c_void,
            msg_controllen: cmsg_space,
            msg_flags: 0,
        };
        // SAFETY: msg's control buffer is `cmsg_space` bytes (sized for
        // exactly one fd-passing cmsg); iov references the stack-local
        // `dummy` valid for the recvmsg duration.  After recvmsg returns,
        // CMSG_FIRSTHDR points into our control buffer; CMSG_DATA points
        // to the c_int payload within it.  Returned fd is freshly dup'd
        // by the kernel into our process so OwnedFd::from_raw_fd takes
        // exclusive ownership cleanly.
        unsafe {
            if libc::recvmsg(sock.as_raw_fd(), &mut msg, 0) < 0 {
                return Err(io::Error::last_os_error());
            }
            let cmsg = libc::CMSG_FIRSTHDR(&msg);
            if cmsg.is_null()
                || (*cmsg).cmsg_level != libc::SOL_SOCKET
                || (*cmsg).cmsg_type != libc::SCM_RIGHTS
            {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "expected SCM_RIGHTS"));
            }
            let raw: libc::c_int = std::ptr::read(libc::CMSG_DATA(cmsg) as *const libc::c_int);
            Ok(OwnedFd::from_raw_fd(raw))
        }
    }
}
