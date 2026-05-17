//! Exclusive per-topic producer lock.
//!
//! Two layers are required:
//!
//! * **`flock(2)`** — cross-process advisory lock on `producer.lock`.
//! * **Process-local `HashSet`** — BSD/macOS `flock` semantics are *per-process*
//!   (all fds opened by the same process share one lock record), so a same-
//!   process duplicate would bypass `flock` alone.

use crate::error::{Error, Result};
use std::collections::HashSet;
use std::fs;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};

static IN_PROCESS_LOCKS: LazyLock<Mutex<HashSet<PathBuf>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

pub(crate) struct ProducerLock {
    path: PathBuf,
    _file: fs::File, // keeps the OS-level flock alive until dropped
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

    // In-process check must come first on macOS.
    {
        let mut set = IN_PROCESS_LOCKS.lock().unwrap_or_else(|e| e.into_inner());
        if !set.insert(path.clone()) {
            return Err(Error::AlreadyPublishing(name.to_owned()));
        }
    }

    let file = match fs::OpenOptions::new().create(true).write(true).open(&path) {
        Ok(f) => f,
        Err(e) => {
            IN_PROCESS_LOCKS.lock().unwrap_or_else(|e| e.into_inner()).remove(&path);
            return Err(Error::Io(e));
        }
    };

    // Cross-process exclusive advisory lock (non-blocking).
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        IN_PROCESS_LOCKS.lock().unwrap_or_else(|e| e.into_inner()).remove(&path);
        return Err(Error::AlreadyPublishing(name.to_owned()));
    }

    Ok(ProducerLock { path, _file: file })
}
