//! Lock-free mmap-backed segment reader (W2-2).
//!
//! Pairs with [`super::MmapSegmentWriter`].  Hot-path read is:
//! one atomic load of the in-mmap `tail`, one atomic load of the
//! record's `record_len` field (seqlock acquire), one memcpy.
//! No mutex, no syscall.  See `docs/rfc-wal-v2-lockfree.md` §3.3.
//!
//! The reader iterates records by following `record_len` (padded
//! 8-byte multiples).  Each iteration handles the three concurrent
//! states a record's leading u32 can be in:
//!
//! | record_len value           | state                                     | reader action          |
//! |----------------------------|-------------------------------------------|------------------------|
//! | `0`                        | publisher fetched the slot but hasn't     | `AwaitMore` (retry)    |
//! |                            | stored `record_len \| WRITING_BIT` yet    |                        |
//! | `n \| WRITING_BIT`         | publisher mid-write                       | `AwaitMore` (retry)    |
//! | `n` (clean, > 0)           | committed, body is consistent             | decode + validate CRC  |
//! | `u32::MAX` (`SKIP_TO_END`) | publisher wrote a SKIP marker (W2-3)      | `EndOfSegment`         |
//!
//! The CRC check at the end of body guards against partial writes
//! the publisher may have left mid-burst (process death) — the
//! `WRITING_BIT` covers the in-flight case, the CRC covers the
//! post-mortem case.

use crate::wal::record::{
    Record, SegmentHeaderError, MAX_RECORD_LEN, RECORD_FRAMING, SEGMENT_HEADER_LEN,
    SEGMENT_MAGIC,
};
use crate::wal::v2::mmap_segment_writer::{
    align_record_len, SEGMENT_VERSION_V2, WRITING_BIT_U32,
};
use memmap2::Mmap;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// Sentinel value the W2-3 rotation logic writes at a dying segment's
/// tail.  `u32::MAX` has `WRITING_BIT` set, but it's distinguishable
/// from a real in-flight record because `MAX_RECORD_LEN` is 16 MiB
/// (bit 24) and the sentinel is all-ones across bits 0..32.
pub const SKIP_TO_END_SENTINEL: u32 = u32::MAX;

#[derive(Debug, thiserror::Error)]
pub enum ReaderError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("segment header: {0}")]
    Header(#[from] SegmentHeaderError),

    /// Reader was opened on a v0.1 segment.  The v0.2 reader does
    /// NOT transparently read v1 segments — the caller is expected
    /// to route to the v0.1 reader path in that case (the v2 Wal
    /// aggregator at W2-4 handles this dispatch).
    #[error("v1 segment encountered by v2 reader (use the v0.1 SegmentReader instead)")]
    LegacyV1Segment,

    /// Decoded record_len exceeds MAX_RECORD_LEN — corruption.
    #[error("record_len {record_len} exceeds MAX_RECORD_LEN ({MAX_RECORD_LEN}) at offset {offset}")]
    OversizeRecord { record_len: usize, offset: usize },

    /// CRC over the record body doesn't match the trailing CRC field.
    #[error("CRC mismatch at offset {offset}: stored {stored:#010x}, computed {computed:#010x}")]
    CrcMismatch { offset: usize, stored: u32, computed: u32 },

    /// `record_len` < RECORD_FRAMING — structurally impossible from
    /// a correct writer, so this is treated as corruption.
    #[error("record_len {record_len} < RECORD_FRAMING ({RECORD_FRAMING}) at offset {offset}")]
    UndersizeRecord { record_len: usize, offset: usize },
}

/// Outcome of [`MmapSegmentReader::next_record`].
#[derive(Debug)]
pub enum ReadOutcome {
    /// A committed record was decoded.  Reader's position advanced.
    Record(Record),
    /// Reached the live tail (no record yet at this position), OR
    /// the current record has `WRITING_BIT` set (publisher mid-
    /// write).  Caller should wait + retry.  Reader's position is
    /// unchanged.
    AwaitMore,
    /// Saw the `SKIP_TO_END` sentinel.  Caller (W2-3) should re-read
    /// the active-segment coord file and switch to the next segment.
    EndOfSegment,
    /// Hard read error — CRC mismatch, oversize/undersize length, or
    /// I/O failure.  Reader's position advanced past the bad record
    /// so the caller can choose to skip + continue or treat as fatal.
    Err(ReaderError),
}

/// Read-only handle on a v2 segment file.
pub struct MmapSegmentReader {
    #[allow(dead_code)]
    path: PathBuf,
    mmap: Mmap,
    segment_size: usize,
    first_cursor: u64,
    pos: usize,
}

impl MmapSegmentReader {
    /// mmap `path` read-only; validate the v2 header.
    pub fn open(path: &Path) -> Result<Self, ReaderError> {
        let file = File::open(path)?;
        let file_len = file.metadata()?.len() as usize;
        if file_len < SEGMENT_HEADER_LEN {
            return Err(ReaderError::Header(SegmentHeaderError::Truncated(file_len)));
        }
        // SAFETY: file is open + the OS protects the mapped pages
        // from concurrent truncation; readers don't write the mmap.
        let mmap = unsafe { Mmap::map(&file)? };

        // Validate the header.  Bytes 0..8 = magic, 8..12 = version.
        let magic = u64::from_le_bytes(mmap[0..8].try_into().unwrap());
        if magic != SEGMENT_MAGIC {
            return Err(ReaderError::Header(SegmentHeaderError::BadMagic { got: magic }));
        }
        let version = u32::from_le_bytes(mmap[8..12].try_into().unwrap());
        if version == 1 {
            return Err(ReaderError::LegacyV1Segment);
        }
        if version != SEGMENT_VERSION_V2 {
            return Err(ReaderError::Header(SegmentHeaderError::UnsupportedVersion { got: version }));
        }
        let first_cursor = u64::from_le_bytes(mmap[16..24].try_into().unwrap());

        Ok(Self {
            path: path.to_owned(),
            mmap,
            segment_size: file_len,
            first_cursor,
            pos: SEGMENT_HEADER_LEN,
        })
    }

    pub fn first_cursor(&self) -> u64 {
        self.first_cursor
    }

    pub fn segment_size(&self) -> usize {
        self.segment_size
    }

    /// Atomic load of the in-mmap `tail` field — the byte offset
    /// past which no record has been committed yet.  Acquire-ordered
    /// so any record_len store the publisher has done before
    /// advancing tail is visible after this load returns.
    pub fn tail(&self) -> u64 {
        self.tail_atomic().load(Ordering::Acquire)
    }

    /// Read the next record at the reader's current position.
    /// Cheap (one atomic load + at most one body memcpy) — call in
    /// a loop from the caller's read pump.
    pub fn next_record(&mut self) -> ReadOutcome {
        if self.pos >= self.segment_size {
            return ReadOutcome::EndOfSegment;
        }
        // Past the live tail?  Publisher hasn't reserved this slot
        // yet → AwaitMore.  (We re-load tail every call so growth
        // since the last next_record is observable.)
        let tail = self.tail();
        if (self.pos as u64) >= tail {
            return ReadOutcome::AwaitMore;
        }

        // Load record_len with Acquire ordering — pairs with the
        // publisher's Release store of clean record_len.  All body
        // writes (cursor, ts, payload_len, payload, crc) happened-
        // before that store, so we see a consistent snapshot.
        let len_field = self.record_len_atomic(self.pos);
        let raw = len_field.load(Ordering::Acquire);

        if raw == 0 {
            // Publisher fetched the slot but hasn't even stored the
            // WRITING-bit marker yet — race window between tail's
            // CAS and the first store.  Caller retries.
            return ReadOutcome::AwaitMore;
        }
        if raw == SKIP_TO_END_SENTINEL {
            self.pos = self.segment_size;
            return ReadOutcome::EndOfSegment;
        }
        if raw & WRITING_BIT_U32 != 0 {
            // Publisher mid-write.  Caller retries; reader position
            // is unchanged.
            return ReadOutcome::AwaitMore;
        }

        let record_len = raw as usize;
        let offset = self.pos;
        if record_len > MAX_RECORD_LEN {
            // Corrupt record_len.  Advance past it so the caller
            // doesn't loop, surface error.
            self.pos = self.segment_size;
            return ReadOutcome::Err(ReaderError::OversizeRecord { record_len, offset });
        }
        if record_len < RECORD_FRAMING {
            self.pos = self.segment_size;
            return ReadOutcome::Err(ReaderError::UndersizeRecord { record_len, offset });
        }
        if offset + record_len > self.segment_size {
            // Record claims to extend past the segment end → corruption.
            self.pos = self.segment_size;
            return ReadOutcome::Err(ReaderError::OversizeRecord { record_len, offset });
        }

        // Decode body.  Layout (matches MmapSegmentWriter):
        //   [offset+4..+12]  cursor
        //   [offset+12..+20] ts
        //   [offset+20..+24] payload_len
        //   [offset+24..+24+payload_len] payload
        //   [offset+24+payload_len..offset+record_len-4] zero pad
        //   [offset+record_len-4..offset+record_len] crc
        let body = &self.mmap[offset..offset + record_len];
        let cursor = u64::from_le_bytes(body[4..12].try_into().unwrap());
        let ts = u64::from_le_bytes(body[12..20].try_into().unwrap());
        let payload_len = u32::from_le_bytes(body[20..24].try_into().unwrap()) as usize;
        if 24 + payload_len + 4 > record_len {
            // payload_len overshoots the record's bytes → corruption.
            self.pos += align_record_len(record_len);
            return ReadOutcome::Err(ReaderError::OversizeRecord { record_len, offset });
        }
        let stored_crc =
            u32::from_le_bytes(body[record_len - 4..record_len].try_into().unwrap());
        let crc_input = &body[4..record_len - 4];
        let computed_crc = crc32c::crc32c(crc_input);
        if stored_crc != computed_crc {
            self.pos += record_len;
            return ReadOutcome::Err(ReaderError::CrcMismatch {
                offset,
                stored: stored_crc,
                computed: computed_crc,
            });
        }

        let payload = body[24..24 + payload_len].to_vec();
        self.pos += record_len;
        ReadOutcome::Record(Record { cursor, ts_unix_nanos: ts, payload })
    }

    /// Reset the reader to right after the segment header so the
    /// next `next_record` call returns the first record.  Useful
    /// for tests and the W2-4 aggregator's "rewind" path.
    pub fn rewind(&mut self) {
        self.pos = SEGMENT_HEADER_LEN;
    }

    fn tail_atomic(&self) -> &AtomicU64 {
        // SAFETY: byte 24 is 8-aligned (mmap base is page-aligned);
        // header region is always mapped; cast through AtomicU64
        // for ordered atomic ops.
        unsafe { &*(self.mmap.as_ptr().add(24) as *const AtomicU64) }
    }

    fn record_len_atomic(&self, offset: usize) -> &AtomicU32 {
        // SAFETY: offset is 8-aligned (writer aligns each record_len
        // to a 4+-byte boundary via align_record_len), well within
        // the mmap (offset+4 <= segment_size guard above for
        // structural cases; this fn is only called with valid
        // `self.pos`).
        unsafe { &*(self.mmap.as_ptr().add(offset) as *const AtomicU32) }
    }
}

// `Mmap` is Send + Sync.
unsafe impl Send for MmapSegmentReader {}
unsafe impl Sync for MmapSegmentReader {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::v2::mmap_segment_writer::{AppendOutcome, MmapSegmentWriter};
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    fn tmpdir() -> TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    fn write_records(path: &std::path::Path, records: &[(u64, &[u8])]) {
        let w = MmapSegmentWriter::create(path, 4096, records.first().map(|(c, _)| *c).unwrap_or(0))
            .unwrap();
        for (c, p) in records {
            match w.append(*c, 0, p).unwrap() {
                AppendOutcome::Ok { .. } => (),
                other => panic!("write_records: unexpected outcome {other:?}"),
            }
        }
        w.flush_async().unwrap();
    }

    #[test]
    fn open_validates_v2_header() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        let _w = MmapSegmentWriter::create(&path, 4096, 42).unwrap();
        drop(_w);
        let r = MmapSegmentReader::open(&path).unwrap();
        assert_eq!(r.first_cursor(), 42);
        assert_eq!(r.segment_size(), 4096);
        assert_eq!(r.tail(), SEGMENT_HEADER_LEN as u64);
    }

    #[test]
    fn open_rejects_legacy_v1_segment() {
        // Construct a minimal v0.1-format segment by hand and try to
        // open it via the v2 reader.
        let dir = tmpdir();
        let path = dir.path().join("v1.seg");
        let mut bytes = vec![0u8; SEGMENT_HEADER_LEN];
        bytes[0..8].copy_from_slice(&SEGMENT_MAGIC.to_le_bytes());
        bytes[8..12].copy_from_slice(&1u32.to_le_bytes()); // version 1
        std::fs::write(&path, &bytes).unwrap();
        match MmapSegmentReader::open(&path) {
            Err(ReaderError::LegacyV1Segment) => (),
            Err(e) => panic!("expected LegacyV1Segment, got Err({e:?})"),
            Ok(_) => panic!("expected LegacyV1Segment, got Ok(reader)"),
        }
    }

    #[test]
    fn open_rejects_bad_magic() {
        let dir = tmpdir();
        let path = dir.path().join("bad.seg");
        let mut bytes = vec![0u8; SEGMENT_HEADER_LEN];
        bytes[0..8].copy_from_slice(&0xDEAD_BEEF_DEAD_BEEFu64.to_le_bytes());
        std::fs::write(&path, &bytes).unwrap();
        match MmapSegmentReader::open(&path) {
            Err(ReaderError::Header(SegmentHeaderError::BadMagic { .. })) => (),
            Err(e) => panic!("expected BadMagic, got Err({e:?})"),
            Ok(_) => panic!("expected BadMagic, got Ok(reader)"),
        }
    }

    #[test]
    fn open_rejects_short_file() {
        let dir = tmpdir();
        let path = dir.path().join("short.seg");
        std::fs::write(&path, [0u8; 16]).unwrap();
        match MmapSegmentReader::open(&path) {
            Err(ReaderError::Header(SegmentHeaderError::Truncated(16))) => (),
            Err(e) => panic!("expected Truncated(16), got Err({e:?})"),
            Ok(_) => panic!("expected Truncated(16), got Ok(reader)"),
        }
    }

    #[test]
    fn next_record_reads_committed_records_in_order() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        write_records(&path, &[(0, b"alpha"), (1, b"beta"), (2, b"gamma")]);
        let mut r = MmapSegmentReader::open(&path).unwrap();
        for (i, expected) in [b"alpha".to_vec(), b"beta".to_vec(), b"gamma".to_vec()]
            .iter()
            .enumerate()
        {
            match r.next_record() {
                ReadOutcome::Record(rec) => {
                    assert_eq!(rec.cursor, i as u64);
                    assert_eq!(rec.payload, *expected);
                }
                other => panic!("expected Record, got {other:?}"),
            }
        }
        // Past the live tail: AwaitMore (writer's tail advanced past
        // last record, our pos == tail).
        assert!(matches!(r.next_record(), ReadOutcome::AwaitMore));
    }

    #[test]
    fn next_record_await_more_at_live_tail() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        let _w = MmapSegmentWriter::create(&path, 4096, 0).unwrap();
        drop(_w);
        let mut r = MmapSegmentReader::open(&path).unwrap();
        // No records written → first call is AwaitMore.
        assert!(matches!(r.next_record(), ReadOutcome::AwaitMore));
    }

    #[test]
    fn next_record_skip_to_end_sentinel_yields_end_of_segment() {
        // Hand-craft a segment with a SKIP_TO_END marker at the
        // first record slot.  We bypass the writer (it doesn't emit
        // SKIP_TO_END until W2-3) and write the bytes directly.
        let dir = tmpdir();
        let path = dir.path().join("skip.seg");
        // Create via the writer to get a valid header.
        let w = MmapSegmentWriter::create(&path, 4096, 7).unwrap();
        drop(w);
        // Now overwrite bytes [24..32] (tail) past SEGMENT_HEADER_LEN
        // and bytes [32..36] (record_len) with the sentinel.
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[24..32].copy_from_slice(&((SEGMENT_HEADER_LEN as u64) + 4).to_le_bytes());
        bytes[SEGMENT_HEADER_LEN..SEGMENT_HEADER_LEN + 4]
            .copy_from_slice(&SKIP_TO_END_SENTINEL.to_le_bytes());
        std::fs::write(&path, &bytes).unwrap();

        let mut r = MmapSegmentReader::open(&path).unwrap();
        assert!(matches!(r.next_record(), ReadOutcome::EndOfSegment));
    }

    #[test]
    fn next_record_detects_crc_mismatch() {
        let dir = tmpdir();
        let path = dir.path().join("crc.seg");
        write_records(&path, &[(0, b"clean")]);
        // Flip one byte inside the payload region (the crc check
        // covers cursor + ts + payload_len + payload + padding).
        // Payload starts at SEGMENT_HEADER_LEN + 24 = 56.
        let payload_byte = SEGMENT_HEADER_LEN + 24;
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[payload_byte] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();
        let mut r = MmapSegmentReader::open(&path).unwrap();
        match r.next_record() {
            ReadOutcome::Err(ReaderError::CrcMismatch { .. }) => (),
            other => panic!("expected CrcMismatch, got {other:?}"),
        }
    }

    #[test]
    fn rewind_replays_from_first_record() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        write_records(&path, &[(0, b"one"), (1, b"two")]);
        let mut r = MmapSegmentReader::open(&path).unwrap();
        let _ = r.next_record();
        let _ = r.next_record();
        assert!(matches!(r.next_record(), ReadOutcome::AwaitMore));
        r.rewind();
        match r.next_record() {
            ReadOutcome::Record(rec) => assert_eq!(rec.payload, b"one"),
            other => panic!("rewind: expected first record, got {other:?}"),
        }
    }

    #[test]
    fn concurrent_writer_reader_no_loss_or_dup() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        // 64 KiB segment, plenty of room for the burst.
        let segment_size = 64 * 1024;
        let writer = Arc::new(MmapSegmentWriter::create(&path, segment_size, 0).unwrap());

        const N: u64 = 200;
        let writer_clone = writer.clone();
        let writer_handle = thread::spawn(move || {
            for i in 0..N {
                let payload = i.to_le_bytes();
                match writer_clone.append(i, 0, &payload).unwrap() {
                    AppendOutcome::Ok { .. } => (),
                    AppendOutcome::SegmentFull => {
                        panic!("segment unexpectedly filled at cursor {i}");
                    }
                }
            }
        });

        let reader_path = path.clone();
        let reader_handle = thread::spawn(move || {
            // Reader thread races the writer.  Open inside the thread
            // so the header is guaranteed to be written.
            let mut r = loop {
                match MmapSegmentReader::open(&reader_path) {
                    Ok(r) => break r,
                    Err(_) => thread::sleep(Duration::from_millis(1)),
                }
            };
            let mut got: Vec<u64> = Vec::new();
            let deadline = Instant::now() + Duration::from_secs(5);
            while got.len() < N as usize {
                match r.next_record() {
                    ReadOutcome::Record(rec) => {
                        assert_eq!(rec.payload.len(), 8);
                        let v = u64::from_le_bytes(rec.payload.try_into().unwrap());
                        got.push(v);
                    }
                    ReadOutcome::AwaitMore => {
                        if Instant::now() >= deadline {
                            panic!(
                                "reader timed out at {} / {} records",
                                got.len(),
                                N
                            );
                        }
                        std::hint::spin_loop();
                    }
                    other => panic!("unexpected outcome: {other:?}"),
                }
            }
            got
        });

        writer_handle.join().unwrap();
        let got = reader_handle.join().unwrap();
        assert_eq!(got.len(), N as usize);
        for (i, v) in got.iter().enumerate() {
            assert_eq!(*v, i as u64, "record at position {i} has value {v}");
        }
    }
}
