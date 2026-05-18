//! Lock-free mmap-backed segment writer (W2-1).
//!
//! Hot-path publish is a single `tail.fetch_add` + a memcpy + two
//! `u32` atomic stores (bracketed seqlock).  No mutex, no syscall.
//! See `docs/rfc-wal-v2-lockfree.md` §3.2.
//!
//! Wire format choices that differ from v0.1:
//!
//! * **`SEGMENT_VERSION = 2`** — a v0.1 reader rejects v2 segments
//!   with `UnsupportedVersion` rather than misparsing them.  A v0.2
//!   reader (W2-2) handles both v1 and v2.
//! * **`tail: AtomicU64` at segment-header bytes 24..32** —
//!   overwrites v0.1's `created_unix_nanos` slot, which is
//!   informational only.  Naturally 8-byte aligned (segment base is
//!   page-aligned by mmap).
//! * **8-byte-aligned `record_len`** — pad each record so the next
//!   record's `record_len` field lands on a 4-byte boundary, a
//!   prerequisite for `AtomicU32` access.  v0.1's record_len held
//!   the exact unpadded size; v2's record_len is the PADDED total.
//!   The body still ends at `RECORD_FRAMING + payload.len()`; the
//!   remainder is zero-padding included in the CRC.
//! * **`WRITING_BIT_U32 = 1 << 31` in `record_len`** — high bit of
//!   the u32 length field doubles as the "writer in flight" flag.
//!   16 MiB max record (the `MAX_RECORD_LEN` constant) only uses
//!   bit 24, so bit 31 is free.
//!
//! Padding + version bump are the price of lock-free reads under
//! seqlock semantics; the read side (W2-2) returns the unpadded
//! payload by trusting the embedded `payload_len`.

use crate::wal::record::{
    MAX_PAYLOAD_LEN, MAX_RECORD_LEN, RECORD_FRAMING, SEGMENT_HEADER_LEN, SEGMENT_MAGIC,
};
use crate::wal::v2::durability;
use memmap2::MmapMut;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// Format version for v2 segments — distinct from v0.1's
/// `SEGMENT_VERSION = 1`.  A v0.1 reader rejects with
/// `UnsupportedVersion`.
pub const SEGMENT_VERSION_V2: u32 = 2;

/// High bit of the u32 `record_len` field — set while the publisher
/// is mid-write, cleared on commit.  The seqlock bracket primitive.
pub const WRITING_BIT_U32: u32 = 1 << 31;

/// Byte offset of the in-mmap `tail: AtomicU64` within the segment
/// header.  Naturally 8-byte aligned.
const TAIL_OFFSET: usize = 24;

/// Round `n` up to the next multiple of 8.  Used so consecutive
/// records both start at 4-byte-aligned offsets (we pick 8 for
/// extra cache-line headroom and to keep cursor / ts fields
/// naturally aligned within the body).
#[inline]
pub fn align_record_len(unpadded: usize) -> usize {
    (unpadded + 7) & !7
}

#[derive(Debug, thiserror::Error)]
pub enum WriterError {
    #[error("payload too large: {payload_len} > MAX_PAYLOAD_LEN ({MAX_PAYLOAD_LEN})")]
    PayloadTooLarge { payload_len: usize },

    #[error(
        "segment_size {segment_size} too small (need at least {min} to hold the header)"
    )]
    SegmentTooSmall { segment_size: usize, min: usize },

    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

/// Outcome of an `append` call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendOutcome {
    /// Record committed at `byte_offset`.  `next_cursor` is the
    /// cursor value the publisher should advance to.
    Ok { byte_offset: u64, next_cursor: u64 },
    /// The record would overrun `segment_size`.  The publisher's
    /// `fetch_add` was rolled back via a compare-and-swap, so the
    /// tail is unchanged and the next caller can attempt to write
    /// a SKIP_TO_END marker + rotate (W2-3).
    SegmentFull,
}

/// Writer for one v2 segment file.  Owns an `MmapMut` backed by a
/// pre-allocated file.  The owning `File` handle is kept around so
/// the per-platform [`durability`] primitives can call
/// `fdatasync` / `F_FULLFSYNC` / `FlushFileBuffers` on it after an
/// `msync`.
pub struct MmapSegmentWriter {
    path: PathBuf,
    file: File,
    mmap: MmapMut,
    segment_size: usize,
    first_cursor: u64,
}

impl MmapSegmentWriter {
    /// Pre-allocate `segment_size` bytes at `path`, mmap, write the
    /// v2 header, and initialise the in-mmap `tail` past the header.
    ///
    /// Fails with `io::ErrorKind::AlreadyExists` if `path` exists
    /// (segment filenames are deterministic; collision = logic bug).
    pub fn create(
        path: &Path,
        segment_size: usize,
        first_cursor: u64,
    ) -> Result<Self, WriterError> {
        if segment_size < SEGMENT_HEADER_LEN + RECORD_FRAMING {
            return Err(WriterError::SegmentTooSmall {
                segment_size,
                min: SEGMENT_HEADER_LEN + RECORD_FRAMING,
            });
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        file.set_len(segment_size as u64)?;
        // SAFETY: file was just created + sized; mmap reflects bytes
        // we own exclusively (single-publisher invariant enforced
        // upstream by producer.lock).
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };

        // Write the v2 segment header into bytes 0..24.  Bytes 24..32
        // (tail) are written via the atomic helper below so the store
        // happens-before any reader observes the file.
        mmap[0..8].copy_from_slice(&SEGMENT_MAGIC.to_le_bytes());
        mmap[8..12].copy_from_slice(&SEGMENT_VERSION_V2.to_le_bytes());
        // bytes 12..16 = reserved (zero-init from ftruncate).
        mmap[16..24].copy_from_slice(&first_cursor.to_le_bytes());
        // Initialise tail at byte offset SEGMENT_HEADER_LEN (= 32),
        // i.e. just past the header — the first record's offset.
        let writer = Self { path: path.to_owned(), file, mmap, segment_size, first_cursor };
        writer.tail_atomic().store(SEGMENT_HEADER_LEN as u64, Ordering::Release);
        Ok(writer)
    }

    /// Append one record, returning the byte offset on success or
    /// `SegmentFull` if it wouldn't fit.  `cursor` MUST equal the
    /// caller's expected next cursor; this writer does NOT enforce
    /// cursor monotonicity (the aggregator does — single publisher
    /// invariant makes it natural to track in one place).
    pub fn append(
        &self,
        cursor: u64,
        ts_unix_nanos: u64,
        payload: &[u8],
    ) -> Result<AppendOutcome, WriterError> {
        if payload.len() > MAX_PAYLOAD_LEN {
            return Err(WriterError::PayloadTooLarge { payload_len: payload.len() });
        }
        let unpadded = RECORD_FRAMING + payload.len();
        let aligned = align_record_len(unpadded);
        debug_assert!(aligned <= MAX_RECORD_LEN);

        // Step 1: reserve space via compare-and-swap on tail.  We use
        // CAS rather than fetch_add so an overrun rolls the tail back
        // — the next caller sees a clean state and can write a
        // SKIP_TO_END marker (W2-3) instead of inheriting a stuck
        // tail past the segment end.
        let segment_size = self.segment_size as u64;
        let tail = self.tail_atomic();
        let mut current = tail.load(Ordering::Acquire);
        let offset = loop {
            let next = current + aligned as u64;
            if next > segment_size {
                return Ok(AppendOutcome::SegmentFull);
            }
            match tail.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => break current,
                Err(actual) => current = actual,
            }
        };

        // Step 2: stake claim — record_len | WRITING_BIT — so any
        // reader catching up sees "writer in flight, retry".
        let slot_ptr = unsafe { self.mmap.as_ptr().add(offset as usize) as *mut u8 };
        let len_field = unsafe { &*(slot_ptr as *const AtomicU32) };
        len_field.store(aligned as u32 | WRITING_BIT_U32, Ordering::Release);

        // Step 3: body write.  Layout (matches v0.1's record bytes
        // exactly except for the trailing zero-pad up to `aligned`):
        //   [4..12]    cursor          u64 LE
        //   [12..20]   ts_unix_nanos   u64 LE
        //   [20..24]   payload_len     u32 LE
        //   [24..24+L] payload         bytes
        //   [24+L..aligned-4] zero pad (already zero from ftruncate)
        //   [aligned-4..aligned] crc32c
        //
        // SAFETY: slot_ptr..slot_ptr+aligned is within the mmap
        // (checked by the offset+aligned <= segment_size guard
        // above).  We hold exclusive write access to this slot
        // (publisher is single-threaded; the CAS above claims our
        // range).
        unsafe {
            std::ptr::write_unaligned(slot_ptr.add(4) as *mut u64, cursor.to_le());
            std::ptr::write_unaligned(slot_ptr.add(12) as *mut u64, ts_unix_nanos.to_le());
            std::ptr::write_unaligned(slot_ptr.add(20) as *mut u32, (payload.len() as u32).to_le());
            std::ptr::copy_nonoverlapping(payload.as_ptr(), slot_ptr.add(24), payload.len());
        }
        // CRC covers cursor..payload + any zero padding before the
        // CRC field, matching the v0.1 spec ("everything after
        // record_len excluding the crc itself").
        let crc_input_start = 4;
        let crc_input_end = aligned - 4;
        let crc_input = unsafe {
            std::slice::from_raw_parts(slot_ptr.add(crc_input_start), crc_input_end - crc_input_start)
        };
        let crc = crc32c::crc32c(crc_input);
        unsafe {
            std::ptr::write_unaligned(slot_ptr.add(aligned - 4) as *mut u32, crc.to_le());
        }

        // Step 4: commit — clear the WRITING bit by re-storing the
        // clean record_len.  Release-ordered so the body writes
        // happen-before the reader's Acquire-load of record_len.
        len_field.store(aligned as u32, Ordering::Release);

        Ok(AppendOutcome::Ok {
            byte_offset: offset,
            next_cursor: cursor + 1,
        })
    }

    /// Current tail byte offset.  Useful for tests + the W2-3
    /// rotation logic (decide where to write the SKIP_TO_END marker).
    pub fn current_tail(&self) -> u64 {
        self.tail_atomic().load(Ordering::Acquire)
    }

    /// Write the `SKIP_TO_END` sentinel at the current tail and
    /// advance tail past it, so any reader observing this segment
    /// transitions to `ReadOutcome::EndOfSegment` and chases the
    /// next segment.  Idempotent: if there isn't room for the 4-byte
    /// sentinel, returns `Ok(false)` — readers fall through to
    /// `EndOfSegment` via the `pos >= segment_size` guard anyway.
    ///
    /// Called by the W2-3 rotation path after an `append` returns
    /// [`AppendOutcome::SegmentFull`].
    pub fn write_skip_to_end(&self) -> Result<bool, WriterError> {
        use crate::wal::v2::mmap_segment_reader::SKIP_TO_END_SENTINEL;
        let segment_size = self.segment_size as u64;
        let tail = self.tail_atomic();
        let mut current = tail.load(Ordering::Acquire);
        let offset = loop {
            if current + 4 > segment_size {
                // No room for the 4-byte marker — but that's fine:
                // a reader whose pos lands at segment_size returns
                // EndOfSegment without needing a marker.
                return Ok(false);
            }
            match tail.compare_exchange_weak(
                current,
                current + 4,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break current,
                Err(actual) => current = actual,
            }
        };
        // SAFETY: offset+4 <= segment_size guarded above; offset is
        // 4+-byte aligned (every prior reservation was 8-aligned via
        // align_record_len).
        let slot = unsafe { self.mmap.as_ptr().add(offset as usize) as *const AtomicU32 };
        unsafe { (*slot).store(SKIP_TO_END_SENTINEL, Ordering::Release) };
        Ok(true)
    }

    pub fn first_cursor(&self) -> u64 {
        self.first_cursor
    }

    pub fn segment_size(&self) -> usize {
        self.segment_size
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Flush dirty mmap pages to the OS page cache via msync.  Does
    /// NOT fsync the file — that's [`Self::flush_sync`].  Useful
    /// for tests that want to make writes visible to a concurrent
    /// reader on a different fd (the same-mmap reads see them via
    /// the atomic stores already).
    pub fn flush_async(&self) -> io::Result<()> {
        durability::flush_async(&self.mmap)
    }

    /// Per-platform durable flush.  After this returns Ok, every
    /// committed record is on stable storage (within the OS's
    /// guarantees — `F_FULLFSYNC` on macOS, `fdatasync` on Linux,
    /// `FlushFileBuffers` on Windows).  Used by
    /// [`FsyncPolicy::Each`](crate::wal::FsyncPolicy::Each) inline
    /// and by the Batched flusher thread on each tick.
    pub fn flush_sync(&self) -> io::Result<()> {
        durability::flush_sync(&self.mmap, &self.file)
    }

    fn tail_atomic(&self) -> &AtomicU64 {
        // SAFETY: byte offset 24 is 8-byte aligned (segment base is
        // page-aligned via mmap), the header region is always mapped
        // (segment_size >= SEGMENT_HEADER_LEN checked in create),
        // and we cast through AtomicU64 so all reads/writes are
        // ordered atomic ops.
        unsafe { &*(self.mmap.as_ptr().add(TAIL_OFFSET) as *const AtomicU64) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::record::SegmentHeaderError;
    use tempfile::TempDir;

    fn tmpdir() -> TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    #[test]
    fn align_record_len_rounds_up_to_eight() {
        assert_eq!(align_record_len(0), 0);
        assert_eq!(align_record_len(1), 8);
        assert_eq!(align_record_len(7), 8);
        assert_eq!(align_record_len(8), 8);
        assert_eq!(align_record_len(28), 32); // RECORD_FRAMING
        assert_eq!(align_record_len(29), 32);
        assert_eq!(align_record_len(33), 40);
    }

    #[test]
    fn create_writes_v2_header_with_tail_at_record_start() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        let w = MmapSegmentWriter::create(&path, 4096, 42).expect("create");
        assert_eq!(w.first_cursor(), 42);
        assert_eq!(w.segment_size(), 4096);
        assert_eq!(w.current_tail(), SEGMENT_HEADER_LEN as u64);

        // Header bit-for-bit (except bytes 24..32 hold tail in v2):
        let bytes = std::fs::read(&path).unwrap();
        let magic = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        let first_cursor = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
        let tail = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
        assert_eq!(magic, SEGMENT_MAGIC);
        assert_eq!(version, SEGMENT_VERSION_V2);
        assert_eq!(first_cursor, 42);
        assert_eq!(tail, SEGMENT_HEADER_LEN as u64);
    }

    #[test]
    fn create_rejects_existing_path() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        let _w1 = MmapSegmentWriter::create(&path, 4096, 0).expect("first");
        match MmapSegmentWriter::create(&path, 4096, 0) {
            Err(WriterError::Io(e)) => {
                assert_eq!(e.kind(), io::ErrorKind::AlreadyExists);
            }
            Err(e) => panic!("expected Io(AlreadyExists), got Err({e:?})"),
            Ok(_) => panic!("expected Io(AlreadyExists), got Ok(writer)"),
        }
    }

    #[test]
    fn create_rejects_too_small_segment() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        let too_small = SEGMENT_HEADER_LEN + RECORD_FRAMING - 1;
        match MmapSegmentWriter::create(&path, too_small, 0) {
            Err(WriterError::SegmentTooSmall { .. }) => (),
            Err(e) => panic!("expected SegmentTooSmall, got Err({e:?})"),
            Ok(_) => panic!("expected SegmentTooSmall, got Ok(writer)"),
        }
    }

    #[test]
    fn append_advances_tail_by_aligned_size() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        let w = MmapSegmentWriter::create(&path, 4096, 0).unwrap();
        // 1-byte payload: unpadded = 29, aligned = 32.
        match w.append(0, 0, b"x").unwrap() {
            AppendOutcome::Ok { byte_offset, next_cursor } => {
                assert_eq!(byte_offset, SEGMENT_HEADER_LEN as u64);
                assert_eq!(next_cursor, 1);
            }
            other => panic!("expected Ok, got {other:?}"),
        }
        assert_eq!(w.current_tail(), SEGMENT_HEADER_LEN as u64 + 32);
    }

    #[test]
    fn append_segment_full_keeps_tail_unchanged() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        // Just enough room for the header + one 32-byte record.
        let segment_size = SEGMENT_HEADER_LEN + 32;
        let w = MmapSegmentWriter::create(&path, segment_size, 0).unwrap();
        // First append fits.
        assert!(matches!(w.append(0, 0, b"a").unwrap(), AppendOutcome::Ok { .. }));
        let after_first = w.current_tail();
        // Second append would overrun → SegmentFull, tail unchanged.
        assert_eq!(w.append(1, 0, b"b").unwrap(), AppendOutcome::SegmentFull);
        assert_eq!(w.current_tail(), after_first);
    }

    #[test]
    fn append_oversize_payload_returns_error() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        let w = MmapSegmentWriter::create(&path, MAX_RECORD_LEN, 0).unwrap();
        let too_big = vec![0u8; MAX_PAYLOAD_LEN + 1];
        match w.append(0, 0, &too_big) {
            Err(WriterError::PayloadTooLarge { payload_len }) => {
                assert_eq!(payload_len, MAX_PAYLOAD_LEN + 1);
            }
            other => panic!("expected PayloadTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn appended_record_has_writing_bit_clear_after_commit() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        let w = MmapSegmentWriter::create(&path, 4096, 0).unwrap();
        w.append(0, 0xDEAD_BEEF, b"hello").unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let record_len =
            u32::from_le_bytes(bytes[SEGMENT_HEADER_LEN..SEGMENT_HEADER_LEN + 4].try_into().unwrap());
        // unpadded = 28 + 5 = 33, aligned = 40.
        assert_eq!(record_len, 40, "record_len reflects aligned size");
        assert_eq!(record_len & WRITING_BIT_U32, 0, "WRITING_BIT must be clear on commit");
    }

    #[test]
    fn appended_record_body_round_trips_with_crc_match() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        let w = MmapSegmentWriter::create(&path, 4096, 100).unwrap();
        let ts: u64 = 0xCAFE_BABE_DEAD_BEEF;
        let payload = b"v2 round-trip";
        w.append(100, ts, payload).unwrap();
        drop(w);

        let bytes = std::fs::read(&path).unwrap();
        let off = SEGMENT_HEADER_LEN;
        let record_len = u32::from_le_bytes(bytes[off..off + 4].try_into().unwrap()) as usize;
        let cursor = u64::from_le_bytes(bytes[off + 4..off + 12].try_into().unwrap());
        let ts_read = u64::from_le_bytes(bytes[off + 12..off + 20].try_into().unwrap());
        let payload_len =
            u32::from_le_bytes(bytes[off + 20..off + 24].try_into().unwrap()) as usize;
        let payload_read = &bytes[off + 24..off + 24 + payload_len];
        let crc_stored =
            u32::from_le_bytes(bytes[off + record_len - 4..off + record_len].try_into().unwrap());

        assert_eq!(cursor, 100);
        assert_eq!(ts_read, ts);
        assert_eq!(payload_len, payload.len());
        assert_eq!(payload_read, payload);

        // CRC matches what the writer computed (over cursor..crc-start).
        let crc_input = &bytes[off + 4..off + record_len - 4];
        let crc_computed = crc32c::crc32c(crc_input);
        assert_eq!(crc_stored, crc_computed);
    }

    #[test]
    fn multiple_appends_chain_at_aligned_offsets() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        let w = MmapSegmentWriter::create(&path, 4096, 0).unwrap();
        // Three records of different payload sizes; check each starts
        // at an 8-byte-aligned offset.
        let payloads: &[&[u8]] = &[b"a", b"bcdefgh", b"foo bar baz"];
        let mut expected_offsets = Vec::new();
        let mut tail = SEGMENT_HEADER_LEN as u64;
        for (i, p) in payloads.iter().enumerate() {
            expected_offsets.push(tail);
            let outcome = w.append(i as u64, 0, p).unwrap();
            if let AppendOutcome::Ok { byte_offset, .. } = outcome {
                assert_eq!(byte_offset, tail);
                assert_eq!(byte_offset % 8, 0, "record offsets must be 8-aligned");
            } else {
                panic!("expected Ok, got {outcome:?}");
            }
            tail += align_record_len(RECORD_FRAMING + p.len()) as u64;
        }
        assert_eq!(w.current_tail(), tail);
    }

    #[test]
    fn v1_reader_rejects_v2_segment_with_unsupported_version() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        let _w = MmapSegmentWriter::create(&path, 4096, 0).unwrap();
        // Drop is not enough on its own — flush so the header is on
        // disk before SegmentHeader::parse reads it.
        // (SegmentHeader::parse reads bytes 0..32 from a &[u8].)
        let bytes = std::fs::read(&path).unwrap();
        match crate::wal::record::SegmentHeader::parse(&bytes[..SEGMENT_HEADER_LEN]) {
            Err(SegmentHeaderError::UnsupportedVersion { got }) => {
                assert_eq!(got, SEGMENT_VERSION_V2);
            }
            other => panic!("v1 reader should reject v2 segment with UnsupportedVersion, got {other:?}"),
        }
    }
}
