//! `wal/active.dat` coordination file (W2-3).
//!
//! 16-byte mmap'd file that names the currently-active segment.
//! Single writer (the aggregator that owns the publisher), many
//! readers (every subscriber).  Atomic loads + stores on the two
//! `u64` slots are enough — no seqlock needed because each slot is
//! independently meaningful and readers care primarily about the
//! `first_cursor` slot.
//!
//! Layout (little-endian):
//!
//! | bytes | field                 | semantics                                      |
//! |-------|-----------------------|------------------------------------------------|
//! | 0..8  | `active_first_cursor` | identifies the current segment file by its     |
//! |       |                       | filename stem (`{first_cursor:020}.seg`)       |
//! | 8..16 | `generation`          | monotonic counter, bumped on each rotation     |
//! |       |                       | (diagnostics / "did anything change" probe)    |
//!
//! Update protocol: writer stores `first_cursor` (Release), then
//! bumps `generation` (Release).  Reader does an Acquire load on
//! `first_cursor` and switches segments if it differs from the
//! segment it currently has open.

use memmap2::MmapMut;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// Filename used inside the WAL directory.
pub const ACTIVE_COORD_FILENAME: &str = "active.dat";

/// Size of the coord file in bytes.  Pre-allocated at open.
pub const ACTIVE_COORD_LEN: usize = 16;

const FIRST_CURSOR_OFFSET: usize = 0;
const GENERATION_OFFSET: usize = 8;

/// Read/write handle on `wal/active.dat`.  Hold one per aggregator
/// (writer-side) and one per subscriber (read-only).  Cheap to keep
/// open — it's a single mapped page.
pub struct ActiveCoord {
    #[allow(dead_code)]
    path: PathBuf,
    mmap: MmapMut,
}

impl ActiveCoord {
    /// Open `<dir>/active.dat`, creating it (zero-initialised) if it
    /// doesn't exist.  Always returns a read+write mmap so the same
    /// type works for the writer and (read-only) subscribers — the
    /// API restricts subscribers to the `load_*` methods by
    /// convention; the bytes are 8-byte aligned so loads are atomic
    /// regardless of mode.
    pub fn open_or_create(dir: &Path) -> io::Result<Self> {
        let path = dir.join(ACTIVE_COORD_FILENAME);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        if file.metadata()?.len() < ACTIVE_COORD_LEN as u64 {
            file.set_len(ACTIVE_COORD_LEN as u64)?;
        }
        // SAFETY: file is sized to ACTIVE_COORD_LEN; we map exactly
        // those bytes and access them via AtomicU64 references whose
        // offsets are 8-aligned (mmap base is page-aligned).
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        Ok(Self { path, mmap })
    }

    /// Acquire-load the active first_cursor.  Subscribers call this
    /// on EndOfSegment to decide which segment file to open next.
    pub fn load_first_cursor(&self) -> u64 {
        self.first_cursor_atomic().load(Ordering::Acquire)
    }

    /// Acquire-load the rotation generation counter.
    pub fn load_generation(&self) -> u64 {
        self.generation_atomic().load(Ordering::Acquire)
    }

    /// Writer-only: publish a new active segment.  Stores the
    /// first_cursor with Release ordering, then bumps generation.
    /// Caller is responsible for having created the segment file
    /// before calling this.
    pub fn store_active(&self, first_cursor: u64) -> io::Result<()> {
        self.first_cursor_atomic()
            .store(first_cursor, Ordering::Release);
        self.generation_atomic().fetch_add(1, Ordering::Release);
        // msync so subscribers reading via independent mmaps see the
        // update without waiting for the kernel's writeback timer.
        self.mmap.flush_async()
    }

    fn first_cursor_atomic(&self) -> &AtomicU64 {
        // SAFETY: offset is 0, naturally 8-aligned; bytes 0..8 are
        // always mapped (we set_len to 16).
        unsafe { &*(self.mmap.as_ptr().add(FIRST_CURSOR_OFFSET) as *const AtomicU64) }
    }

    fn generation_atomic(&self) -> &AtomicU64 {
        // SAFETY: offset 8 is 8-aligned; bytes 8..16 always mapped.
        unsafe { &*(self.mmap.as_ptr().add(GENERATION_OFFSET) as *const AtomicU64) }
    }
}

/// Helper used by tests + the recovery path: read the coord file
/// without opening an mmap.  Returns `None` if the file doesn't
/// exist or is short.
pub fn peek(dir: &Path) -> io::Result<Option<(u64, u64)>> {
    let path = dir.join(ACTIVE_COORD_FILENAME);
    let mut file = match File::open(&path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut buf = [0u8; ACTIVE_COORD_LEN];
    use std::io::Read;
    let n = file.read(&mut buf)?;
    if n < ACTIVE_COORD_LEN {
        return Ok(None);
    }
    let first = u64::from_le_bytes(buf[0..8].try_into().unwrap());
    let gen = u64::from_le_bytes(buf[8..16].try_into().unwrap());
    Ok(Some((first, gen)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use tempfile::TempDir;

    fn tmpdir() -> TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    #[test]
    fn open_or_create_creates_zeroed_file() {
        let dir = tmpdir();
        let c = ActiveCoord::open_or_create(dir.path()).unwrap();
        assert_eq!(c.load_first_cursor(), 0);
        assert_eq!(c.load_generation(), 0);
        let len = std::fs::metadata(dir.path().join(ACTIVE_COORD_FILENAME))
            .unwrap()
            .len();
        assert_eq!(len, ACTIVE_COORD_LEN as u64);
    }

    #[test]
    fn open_or_create_is_idempotent() {
        let dir = tmpdir();
        {
            let c = ActiveCoord::open_or_create(dir.path()).unwrap();
            c.store_active(42).unwrap();
        }
        let c2 = ActiveCoord::open_or_create(dir.path()).unwrap();
        assert_eq!(c2.load_first_cursor(), 42);
        assert_eq!(c2.load_generation(), 1);
    }

    #[test]
    fn store_active_bumps_generation_and_is_visible_via_peek() {
        let dir = tmpdir();
        let c = ActiveCoord::open_or_create(dir.path()).unwrap();
        c.store_active(100).unwrap();
        c.store_active(200).unwrap();
        c.store_active(300).unwrap();
        assert_eq!(c.load_first_cursor(), 300);
        assert_eq!(c.load_generation(), 3);
        let (peeked_first, peeked_gen) = peek(dir.path()).unwrap().expect("file present");
        assert_eq!(peeked_first, 300);
        assert_eq!(peeked_gen, 3);
    }

    #[test]
    fn peek_returns_none_when_file_missing() {
        let dir = tmpdir();
        assert!(peek(dir.path()).unwrap().is_none());
    }

    #[test]
    fn concurrent_reader_sees_writer_updates() {
        // Writer thread bumps first_cursor 1..=100, reader thread
        // polls and asserts monotonic non-decreasing reads.
        let dir = tmpdir();
        let path = dir.path().to_path_buf();
        let writer = ActiveCoord::open_or_create(&path).unwrap();
        let reader = ActiveCoord::open_or_create(&path).unwrap();

        let writer_handle = thread::spawn(move || {
            for i in 1..=100u64 {
                writer.store_active(i).unwrap();
            }
        });

        let mut last = 0u64;
        let mut saw_final = false;
        for _ in 0..10_000 {
            let v = reader.load_first_cursor();
            assert!(v >= last, "regression: {v} < {last}");
            last = v;
            if v == 100 {
                saw_final = true;
                break;
            }
            std::hint::spin_loop();
        }
        writer_handle.join().unwrap();
        // Final state is 100; reader will see it either via spin or
        // via the trailing reload below.
        if !saw_final {
            assert_eq!(reader.load_first_cursor(), 100);
        }
    }
}
