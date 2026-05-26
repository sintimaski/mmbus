/// Platform-specific wakeup primitives.
///
/// * **Linux**: `eventfd(2)` + `SCM_RIGHTS` fd-passing over the handshake
///   socket.  Subscribers `poll(2)` both the eventfd and the socket so
///   publisher death (POLLHUP) is detected even while idle.
/// * **Windows**: `CreateSemaphore` + named pipes + `DuplicateHandle`.
///   Subscribers `WaitForMultipleObjects` on (semaphore, pipe) so peer
///   process death (`ERROR_BROKEN_PIPE` on the pipe) is detected even
///   while idle.
/// * **macOS / other Unix**: no helpers here — the byte-per-message Unix
///   socket scheme is used directly in `publisher.rs` / `subscriber.rs`.
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
            libc::eventfd(
                0,
                libc::EFD_NONBLOCK | libc::EFD_CLOEXEC | libc::EFD_SEMAPHORE,
            )
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
            libc::pollfd {
                fd: efd,
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: sock,
                events: libc::POLLHUP | libc::POLLERR,
                revents: 0,
            },
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
    /// Send the subscriber's eventfd (via SCM_RIGHTS) plus its 4-byte
    /// `cursor_idx` (in the regular iovec payload) to the publisher.  The
    /// publisher uses the index to address this subscriber's wakeup flag.
    pub fn send_fd(sock: &UnixStream, fd: RawFd, cursor_idx: u32) -> io::Result<()> {
        let idx_bytes = cursor_idx.to_le_bytes();
        let iov = libc::iovec {
            iov_base: idx_bytes.as_ptr() as *mut libc::c_void,
            iov_len: idx_bytes.len(),
        };
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
            // CMSG_FIRSTHDR returns `*mut cmsghdr` on Linux but
            // `*const cmsghdr` on macOS — the cast is necessary on
            // one and a no-op on the other.
            #[allow(clippy::unnecessary_cast)]
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

    /// Receive an fd sent via SCM_RIGHTS from `sock`, plus the subscriber's
    /// 4-byte `cursor_idx` from the regular payload.  The received fd is a
    /// fresh file-descriptor number in the caller's process (kernel dups it).
    pub fn recv_fd(sock: &UnixStream) -> io::Result<(OwnedFd, u32)> {
        let mut idx_bytes = [0u8; 4];
        let mut iov = libc::iovec {
            iov_base: idx_bytes.as_mut_ptr() as *mut libc::c_void,
            iov_len: idx_bytes.len(),
        };
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
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "expected SCM_RIGHTS",
                ));
            }
            let raw: libc::c_int = std::ptr::read(libc::CMSG_DATA(cmsg) as *const libc::c_int);
            Ok((OwnedFd::from_raw_fd(raw), u32::from_le_bytes(idx_bytes)))
        }
    }
}

#[cfg(target_os = "windows")]
pub(crate) mod windows {
    //! Windows wakeup primitives — `CreateSemaphore` mirrors Linux's
    //! `eventfd(EFD_SEMAPHORE)`: each `ReleaseSemaphore(h, 1)` increments
    //! the count by 1; each successful wait decrements by 1.  Multiple
    //! `Release`s queue up the same way multiple `eventfd_write(1)`s do.
    //!
    //! The handshake transport is a named pipe (`\\.\pipe\mmbus-<bus>-
    //! <topic>-signal`).  After accept, the publisher and subscriber
    //! exchange a fixed 12-byte message:
    //!
    //!   * `u32 pid_le`  — subscriber's `GetCurrentProcessId`
    //!   * `u64 handle_le` — subscriber's semaphore handle value
    //!
    //! The publisher then `OpenProcess(PROCESS_DUP_HANDLE)` on the
    //! subscriber and `DuplicateHandle` to pull the semaphore into its
    //! own handle table.  No reply needed.
    use std::ffi::CString;
    use std::io;
    use std::os::windows::io::{FromRawHandle, OwnedHandle, RawHandle};

    use windows_sys::Win32::Foundation::{
        CloseHandle, DuplicateHandle, DUPLICATE_SAME_ACCESS, ERROR_PIPE_CONNECTED, FALSE, HANDLE,
        INVALID_HANDLE_VALUE, WAIT_OBJECT_0, WAIT_TIMEOUT,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        CreateFileA, ReadFile, WriteFile, FILE_GENERIC_READ, FILE_GENERIC_WRITE, OPEN_EXISTING,
        PIPE_ACCESS_DUPLEX,
    };
    use windows_sys::Win32::System::Pipes::{
        ConnectNamedPipe, CreateNamedPipeA, PIPE_READMODE_BYTE, PIPE_REJECT_REMOTE_CLIENTS,
        PIPE_TYPE_BYTE, PIPE_UNLIMITED_INSTANCES, PIPE_WAIT,
    };
    use windows_sys::Win32::System::Threading::{
        CreateSemaphoreA, GetCurrentProcess, GetCurrentProcessId, OpenProcess, ReleaseSemaphore,
        WaitForMultipleObjects, WaitForSingleObject, INFINITE, PROCESS_DUP_HANDLE,
    };

    // ── Semaphore primitives ──────────────────────────────────────────────────

    /// Create a counting semaphore initialised to 0.  Max count
    /// `i32::MAX` matches the practical ceiling of `eventfd_write` (a
    /// subscriber that lets ~2 G unread wakeups accumulate has bigger
    /// problems than counter saturation).
    pub fn create_semaphore() -> io::Result<OwnedHandle> {
        // SAFETY: name=NULL → unnamed semaphore (private to the process
        // until duplicated); initial_count=0 so first wait blocks;
        // max_count=i32::MAX bounded above the realistic queue depth;
        // attributes=NULL = default security descriptor.
        let h = unsafe { CreateSemaphoreA(std::ptr::null(), 0, i32::MAX, std::ptr::null()) };
        if h.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: CreateSemaphoreA returned a fresh handle owned by this
        // process; FromRawHandle takes exclusive ownership and CloseHandle
        // will run on Drop.
        Ok(unsafe { OwnedHandle::from_raw_handle(h as RawHandle) })
    }

    /// Increment the semaphore count by 1 (analog of `eventfd_wake`).
    /// Returns `false` if the call fails (e.g. peer's handle was closed).
    pub fn semaphore_wake(h: HANDLE) -> bool {
        // SAFETY: caller holds the HANDLE open for the duration of the
        // call; ReleaseSemaphore reads it and writes through the second
        // arg (NULL = don't return previous count).
        let ok = unsafe { ReleaseSemaphore(h, 1, std::ptr::null_mut()) };
        ok != 0
    }

    /// Non-blocking drain of one semaphore unit.  Returns `Ok(true)` if a
    /// unit was drained, `Ok(false)` if the semaphore was empty.
    pub fn semaphore_drain(h: HANDLE) -> io::Result<bool> {
        // SAFETY: caller-owned HANDLE; WaitForSingleObject with timeout=0
        // returns WAIT_OBJECT_0 (decremented) or WAIT_TIMEOUT (empty).
        match unsafe { WaitForSingleObject(h, 0) } {
            WAIT_OBJECT_0 => Ok(true),
            WAIT_TIMEOUT => Ok(false),
            _ => Err(io::Error::last_os_error()),
        }
    }

    /// Block until the semaphore has at least one unit **or** the pipe
    /// signals (the latter typically meaning peer disconnect — a read
    /// from a closed pipe returns 0 bytes / ERROR_BROKEN_PIPE).
    ///
    /// * `timeout_ms < 0` → INFINITE.
    /// * Returns `Err(WouldBlock)` on timeout.
    /// * Returns `Err(UnexpectedEof)` when the pipe is the wakeup source
    ///   (peer has disconnected).
    pub fn wait_wakeup(sem: HANDLE, pipe: HANDLE, timeout_ms: i32) -> io::Result<()> {
        let handles = [sem, pipe];
        let timeout = if timeout_ms < 0 {
            INFINITE
        } else {
            timeout_ms as u32
        };
        // SAFETY: handles points to a 2-element stack array of HANDLEs that
        // are valid for the duration of the call (owned by the caller).
        let r = unsafe { WaitForMultipleObjects(2, handles.as_ptr(), FALSE, timeout) };
        match r {
            WAIT_TIMEOUT => Err(io::Error::new(io::ErrorKind::WouldBlock, "wait timeout")),
            w if w == WAIT_OBJECT_0 => Ok(()), // semaphore signaled
            w if w == WAIT_OBJECT_0 + 1 => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "publisher disconnected (pipe broken)",
            )),
            _ => Err(io::Error::last_os_error()),
        }
    }

    // ── Named pipe primitives (handshake transport) ───────────────────────────

    /// Path for the handshake pipe.  Local-machine namespace (`\\.\pipe\`);
    /// the user-SID prefix lives in the bus name so two users on the
    /// same machine don't collide.
    pub fn pipe_name(bus_dir_name: &str) -> String {
        format!(r"\\.\pipe\mmbus-{bus_dir_name}-signal")
    }

    /// Create the first instance of a named pipe (publisher side).
    /// Subsequent accepts must call `create_pipe_instance` again — each
    /// `CreateNamedPipeA` returns a single instance.
    pub fn create_pipe_instance(name: &str) -> io::Result<OwnedHandle> {
        let c_name =
            CString::new(name).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        // SAFETY: c_name lives for the call; remaining args are
        // primitive constants.  PIPE_REJECT_REMOTE_CLIENTS keeps the
        // pipe local-only; PIPE_UNLIMITED_INSTANCES lets us call this
        // function once per accept.  4096-byte buffers are ample for
        // the 12-byte handshake.  Timeout=0 means use the default
        // (50 ms; only matters for WaitNamedPipe).
        let h = unsafe {
            CreateNamedPipeA(
                c_name.as_ptr() as *const u8,
                PIPE_ACCESS_DUPLEX,
                PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS,
                PIPE_UNLIMITED_INSTANCES,
                4096,
                4096,
                0,
                std::ptr::null(),
            )
        };
        if h == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: CreateNamedPipeA returned a fresh handle owned by this
        // process.
        Ok(unsafe { OwnedHandle::from_raw_handle(h as RawHandle) })
    }

    /// Block until a client connects to this pipe instance.  Returns
    /// `Ok(())` once the connection is established.  `ERROR_PIPE_CONNECTED`
    /// (already connected before ConnectNamedPipe ran) is treated as success.
    pub fn accept_pipe(h: HANDLE) -> io::Result<()> {
        // SAFETY: caller-owned HANDLE; overlapped=NULL → synchronous wait.
        let ok = unsafe { ConnectNamedPipe(h, std::ptr::null_mut()) };
        if ok != 0 {
            return Ok(());
        }
        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(ERROR_PIPE_CONNECTED as i32) {
            return Ok(());
        }
        Err(err)
    }

    /// Subscriber-side: connect to a publisher's named pipe instance.
    pub fn connect_pipe(name: &str) -> io::Result<OwnedHandle> {
        let c_name =
            CString::new(name).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        // SAFETY: c_name lives for the call.  Open for both read + write
        // (we exchange a fixed handshake message + later detect peer
        // closure via ReadFile returning 0 bytes).
        let h = unsafe {
            CreateFileA(
                c_name.as_ptr() as *const u8,
                FILE_GENERIC_READ | FILE_GENERIC_WRITE,
                0,
                std::ptr::null(),
                OPEN_EXISTING,
                0,
                std::ptr::null_mut(),
            )
        };
        if h == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: CreateFileA returned a fresh handle owned by this process.
        Ok(unsafe { OwnedHandle::from_raw_handle(h as RawHandle) })
    }

    // ── Handshake message (12 bytes: u32 pid + u64 handle value) ──────────────

    /// Subscriber-side: tell the publisher which process we are, the value
    /// of our semaphore handle (so the publisher can dup it), and our
    /// `cursor_idx` (so the publisher can address our wakeup flag).
    pub fn send_handshake(pipe: HANDLE, sem: HANDLE, cursor_idx: u32) -> io::Result<()> {
        // SAFETY: pipe is caller-owned and open for write; GetCurrentProcessId
        // is a pure read of the current TEB.  We send a 16-byte buffer of
        // (pid_le u32, handle_value_le u64, cursor_idx_le u32) — handle
        // values are pointer-sized on Windows but kernel handle numbers are
        // always within the first 32 bits in practice; we encode as u64 for
        // cross-architecture safety.
        let pid = unsafe { GetCurrentProcessId() };
        let mut buf = [0u8; 16];
        buf[0..4].copy_from_slice(&pid.to_le_bytes());
        buf[4..12].copy_from_slice(&(sem as usize as u64).to_le_bytes());
        buf[12..16].copy_from_slice(&cursor_idx.to_le_bytes());
        write_all(pipe, &buf)
    }

    /// Publisher-side: read the subscriber's handshake and dup the
    /// subscriber's semaphore into our handle table.  Returns an owned
    /// handle to the duplicated semaphore (drop = `CloseHandle`) plus the
    /// subscriber's `cursor_idx`.
    pub fn recv_handshake_and_dup(pipe: HANDLE) -> io::Result<(OwnedHandle, u32)> {
        let mut buf = [0u8; 16];
        read_exact(pipe, &mut buf)?;
        let pid = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let sub_sem_val = u64::from_le_bytes(buf[4..12].try_into().unwrap());
        let cursor_idx = u32::from_le_bytes(buf[12..16].try_into().unwrap());

        // SAFETY: OpenProcess with PROCESS_DUP_HANDLE returns a handle we
        // own (must CloseHandle); pid is just an integer.  inherit_handle=
        // FALSE.
        let sub_proc = unsafe { OpenProcess(PROCESS_DUP_HANDLE, FALSE, pid) };
        if sub_proc.is_null() {
            return Err(io::Error::last_os_error());
        }

        let mut dup_handle: HANDLE = std::ptr::null_mut();
        // SAFETY: sub_proc was just OpenProcess'd; the source handle value
        // came over the pipe from the same subscriber that owns sub_proc;
        // GetCurrentProcess is a constant pseudo-handle; we get back a
        // real handle in our own process via dup_handle.  Options=0 +
        // access=0 + DUPLICATE_SAME_ACCESS asks the kernel to copy the
        // source handle's access mask verbatim.
        let dup_ok = unsafe {
            DuplicateHandle(
                sub_proc,
                sub_sem_val as HANDLE,
                GetCurrentProcess(),
                &mut dup_handle,
                0,
                FALSE,
                DUPLICATE_SAME_ACCESS,
            )
        };
        // We're done with the process handle no matter what.
        // SAFETY: sub_proc is owned by us (returned from OpenProcess); we
        // haven't stored it anywhere else.
        unsafe { CloseHandle(sub_proc) };
        if dup_ok == 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: dup_handle was just written by DuplicateHandle into our
        // own process; ownership transfers to OwnedHandle which will
        // CloseHandle on Drop.
        Ok((
            unsafe { OwnedHandle::from_raw_handle(dup_handle as RawHandle) },
            cursor_idx,
        ))
    }

    // ── Synchronous I/O helpers over a pipe HANDLE ────────────────────────────

    fn write_all(pipe: HANDLE, mut data: &[u8]) -> io::Result<()> {
        while !data.is_empty() {
            let mut written: u32 = 0;
            // SAFETY: pipe is caller-owned; data is a slice we hold for
            // the duration of the call; &mut written is a stack u32.
            let ok = unsafe {
                WriteFile(
                    pipe,
                    data.as_ptr(),
                    data.len() as u32,
                    &mut written,
                    std::ptr::null_mut(),
                )
            };
            if ok == 0 {
                return Err(io::Error::last_os_error());
            }
            if written == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "WriteFile wrote 0",
                ));
            }
            data = &data[written as usize..];
        }
        Ok(())
    }

    fn read_exact(pipe: HANDLE, mut buf: &mut [u8]) -> io::Result<()> {
        while !buf.is_empty() {
            let mut read: u32 = 0;
            // SAFETY: pipe is caller-owned; buf is a mut slice we hold
            // for the duration of the call; &mut read is a stack u32.
            let ok = unsafe {
                ReadFile(
                    pipe,
                    buf.as_mut_ptr(),
                    buf.len() as u32,
                    &mut read,
                    std::ptr::null_mut(),
                )
            };
            if ok == 0 {
                return Err(io::Error::last_os_error());
            }
            if read == 0 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "pipe closed"));
            }
            buf = &mut buf[read as usize..];
        }
        Ok(())
    }

    // Re-export the raw HANDLE type so callers in publisher.rs /
    // subscriber.rs can cast `as_raw_handle()` results without their
    // own `use windows_sys::...` block.
    pub use windows_sys::Win32::Foundation::HANDLE as RawWinHandle;
}
