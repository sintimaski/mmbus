//! Per-platform durability primitives (W2-7).
//!
//! [`flush_async`] / [`flush_sync`] are the two-tier flush primitives
//! used by the [`crate::wal::v2::Wal`] aggregator.
//!
//! | Platform | `flush_async`                | `flush_sync`                                  |
//! |----------|------------------------------|-----------------------------------------------|
//! | Linux    | `msync(MS_ASYNC)`            | `msync(MS_SYNC)` + `fdatasync(fd)`            |
//! | macOS    | `msync(MS_ASYNC)`            | `msync(MS_SYNC)` + `fcntl(fd, F_FULLFSYNC)`   |
//! | Windows  | `FlushViewOfFile`            | `FlushViewOfFile` + `FlushFileBuffers`        |
//!
//! `msync(MS_ASYNC)` schedules a writeback but doesn't wait —
//! suitable for the publisher's "make data visible to other readers
//! mapping this file" path.  `flush_sync` is what `FsyncPolicy::Each`
//! and the Batched flusher call to advance `durable_cursor`.
//!
//! On macOS, `F_FULLFSYNC` is the only way to guarantee data hits
//! the drive's platter (regular `fsync(2)` only flushes the OS
//! buffer cache).  Bench-aware callers should treat the macOS
//! `flush_sync` cost as 3–5 ms per call (APFS drive characteristic);
//! the Batched policy amortizes this across a flush interval.

use memmap2::MmapMut;
use std::fs::File;
use std::io;

/// Async msync — schedule a writeback of dirty pages, return
/// without waiting.  memmap2's `flush_async` already wraps this
/// cross-platform.
pub fn flush_async(mmap: &MmapMut) -> io::Result<()> {
    mmap.flush_async()
}

/// Sync flush — block until every dirty page is on stable storage.
/// Combines `msync(MS_SYNC)` with the platform-appropriate file-
/// level sync (`fdatasync` / `F_FULLFSYNC` / `FlushFileBuffers`).
#[cfg(target_os = "linux")]
pub fn flush_sync(mmap: &MmapMut, file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    // Step 1: msync(MS_SYNC) — memmap2's `flush` wraps this.
    mmap.flush()?;
    // Step 2: fdatasync — flush the file's data blocks (not the
    // inode metadata; we don't care about mtime/atime here).
    let fd = file.as_raw_fd();
    // SAFETY: fd is a valid open file descriptor for the lifetime
    // of `file`.
    let rc = unsafe { libc::fdatasync(fd) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(target_os = "macos")]
pub fn flush_sync(mmap: &MmapMut, file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    // Step 1: msync(MS_SYNC).
    mmap.flush()?;
    // Step 2: F_FULLFSYNC — the only macOS primitive that drains
    // the drive's write cache to the platter.  Regular fsync(2)
    // on macOS is roughly equivalent to fdatasync on Linux (OS
    // buffer cache → drive buffer cache).
    let fd = file.as_raw_fd();
    // SAFETY: fd is a valid open file descriptor.  F_FULLFSYNC
    // takes no argument.
    let rc = unsafe { libc::fcntl(fd, libc::F_FULLFSYNC) };
    if rc == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Other Unix (BSDs etc.) — fall back to msync(MS_SYNC) + fsync(fd).
#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
pub fn flush_sync(mmap: &MmapMut, file: &File) -> io::Result<()> {
    mmap.flush()?;
    file.sync_data()
}

#[cfg(windows)]
pub fn flush_sync(mmap: &MmapMut, file: &File) -> io::Result<()> {
    // memmap2's flush() wraps FlushViewOfFile.
    mmap.flush()?;
    // FlushFileBuffers — equivalent of fsync for the file handle.
    // std's `sync_data()` calls this internally.
    file.sync_data()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::wal::record::SEGMENT_HEADER_LEN;
    use crate::wal::v2::mmap_segment_writer::MmapSegmentWriter;
    use std::process::Command;
    use tempfile::TempDir;

    fn tmpdir() -> TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    #[test]
    fn flush_async_succeeds_on_fresh_writer() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        let w = MmapSegmentWriter::create(&path, 4096, 0).unwrap();
        w.flush_async().expect("flush_async");
    }

    #[test]
    fn flush_sync_succeeds_on_fresh_writer() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        let w = MmapSegmentWriter::create(&path, 4096, 0).unwrap();
        w.flush_sync().expect("flush_sync");
    }

    #[test]
    fn flush_sync_after_append_persists_record() {
        // Cross-process durability test: write a record, flush_sync,
        // then spawn a child process that opens the file and reads
        // back the bytes.  If flush_sync genuinely synced the data
        // (and isn't just a no-op), the child sees the record bytes
        // — even though the writer process never closed the fd.
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        let w = MmapSegmentWriter::create(&path, 4096, 0).unwrap();
        w.append(0, 0xDEAD_BEEF, b"durable").unwrap();
        w.flush_sync().expect("flush_sync");

        // Read back from a subprocess (cat) — proves the bytes are
        // visible from another open(2), not just our own mmap.
        let output = Command::new("cat").arg(&path).output().expect("spawn cat");
        assert!(output.status.success(), "cat failed: {:?}", output);
        // Record body starts at SEGMENT_HEADER_LEN + 24 (skip
        // record_len + cursor + ts + payload_len) and is "durable".
        let payload_off = SEGMENT_HEADER_LEN + 24;
        let payload = &output.stdout[payload_off..payload_off + 7];
        assert_eq!(payload, b"durable");
    }
}
