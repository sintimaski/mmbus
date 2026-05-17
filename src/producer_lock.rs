//! Exclusive per-topic producer lock.
//!
//! Two layers are required:
//!
//! * **OS-level advisory lock** — `flock(2)` on Unix, `LockFileEx` on Windows.
//!   Both are per-process (BSD-style `flock` on macOS shares the lock record
//!   across all fds opened by the same process; `LockFileEx` on Windows
//!   likewise locks per-process).
//! * **Process-local `HashSet`** — same-process duplicates would otherwise
//!   bypass the per-process OS lock.

use crate::error::{Error, Result};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};

static IN_PROCESS_LOCKS: LazyLock<Mutex<HashSet<PathBuf>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

pub(crate) struct ProducerLock {
    path: PathBuf,
    _file: fs::File, // keeps the OS-level lock alive until dropped
}

impl Drop for ProducerLock {
    fn drop(&mut self) {
        let mut set = IN_PROCESS_LOCKS.lock().unwrap_or_else(|e| e.into_inner());
        set.remove(&self.path);
    }
}

/// Acquire the exclusive producer lock for `name` rooted at `dir`.
/// Returns `Error::AlreadyPublishing` if any holder (this process or another)
/// already owns the lock.
pub(crate) fn acquire_producer_lock(name: &str, dir: &Path) -> Result<ProducerLock> {
    let path = dir.join("producer.lock");

    // In-process check must come first (per-process semantics on both
    // macOS flock and Windows LockFileEx).
    {
        let mut set = IN_PROCESS_LOCKS.lock().unwrap_or_else(|e| e.into_inner());
        if !set.insert(path.clone()) {
            return Err(Error::AlreadyPublishing(name.to_owned()));
        }
    }

    // truncate(false): we never write meaningful content to the lock file;
    // explicitly preserve whatever's there in case a stale lock was left
    // behind by a crashed publisher (we only need the inode for the lock).
    let file = match fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&path)
    {
        Ok(f) => f,
        Err(e) => {
            IN_PROCESS_LOCKS.lock().unwrap_or_else(|e| e.into_inner()).remove(&path);
            return Err(Error::Io(e));
        }
    };

    if !try_lock_exclusive(&file) {
        IN_PROCESS_LOCKS.lock().unwrap_or_else(|e| e.into_inner()).remove(&path);
        return Err(Error::AlreadyPublishing(name.to_owned()));
    }

    Ok(ProducerLock { path, _file: file })
}

// ── Platform-specific lock primitives ────────────────────────────────────────

#[cfg(unix)]
fn try_lock_exclusive(file: &fs::File) -> bool {
    use std::os::unix::io::AsRawFd;
    // SAFETY: file is a freshly-opened std::fs::File whose fd is open for
    // the duration of the call; libc::flock either returns 0 (held) or
    // sets errno (we surface that as AlreadyPublishing).
    unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) == 0 }
}

#[cfg(windows)]
fn try_lock_exclusive(file: &fs::File) -> bool {
    use std::mem::MaybeUninit;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        LockFileEx, LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY,
    };
    use windows_sys::Win32::System::IO::OVERLAPPED;

    // LockFileEx wants a HANDLE + an OVERLAPPED struct (which it uses for the
    // file-offset of the lock region; we lock byte 0..u32::MAX).
    let mut overlapped: MaybeUninit<OVERLAPPED> = MaybeUninit::zeroed();
    // SAFETY: file is a freshly-opened std::fs::File whose handle is open
    // for the duration of the call.  overlapped is zero-initialised and
    // its OffsetHigh / Offset fields end up zero, which means "lock from
    // the start of file".  We pass length=u32::MAX so the lock covers the
    // whole file regardless of any future writes.  LOCKFILE_FAIL_IMMEDIATELY
    // gives the non-blocking try; on contention we return false rather
    // than blocking.
    let ok = unsafe {
        LockFileEx(
            file.as_raw_handle() as windows_sys::Win32::Foundation::HANDLE,
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
            0,           // reserved; must be 0
            u32::MAX,    // nNumberOfBytesToLockLow
            0,           // nNumberOfBytesToLockHigh
            overlapped.as_mut_ptr(),
        )
    };
    // LockFileEx returns BOOL (i32); nonzero = success.
    ok != 0
}
