//! Single-segment reader + recovery scan.
//!
//! Two layers:
//!
//! * `SegmentReader::open(path)` parses the 32 B header (rejects on
//!   bad magic / version / short read) and returns a handle whose
//!   `iter()` yields `Record`s in order.  Read-only — never mutates
//!   the file.
//!
//! * `recover_truncate(path)` runs the full scan and `ftruncate`s
//!   the segment at the first bad record (or short read).  This is
//!   how the WAL guarantees that on the next `Wal::open` every
//!   segment's tail is intact: a power-loss-torn record at the end
//!   gets dropped before any reader sees it.  Idempotent — calling
//!   on an already-clean segment is a no-op.

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::wal::record::{
    Record, SegmentHeader, SegmentHeaderError, MAX_RECORD_LEN, RECORD_FRAMING,
    SEGMENT_HEADER_LEN,
};

#[derive(Debug, thiserror::Error)]
pub enum ReaderError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("segment header: {0}")]
    Header(#[from] SegmentHeaderError),

    /// The decoded record_len exceeded MAX_RECORD_LEN — likely
    /// corruption.  Surface this as a hard error from `iter()`; the
    /// recovery scan treats it the same as a CRC mismatch (truncate
    /// at the bad record's offset).
    #[error("record_len {record_len} exceeds MAX_RECORD_LEN ({MAX_RECORD_LEN})")]
    OversizeRecord { record_len: usize },

    /// The trailing CRC didn't match the body bytes — corrupt or
    /// torn write.
    #[error("CRC mismatch at offset {offset}: stored {stored:#010x}, computed {computed:#010x}")]
    CrcMismatch { offset: u64, stored: u32, computed: u32 },

    /// File ended in the middle of a record (header or body or CRC).
    /// Indistinguishable from "writer hadn't fsynced" in flight; the
    /// recovery scan truncates at the offset of the start of the
    /// short record.
    #[error("short read at offset {offset}: needed {needed} more bytes")]
    ShortRead { offset: u64, needed: u64 },
}

/// Read-only view over one segment.  Path + parsed header + a
/// `BufReader<File>` cursor.
pub struct SegmentReader {
    /// Operator-friendly origin for diagnostics; not used by the
    /// iterator but useful in WARN logs and future replayer code
    /// that needs to identify the segment.
    #[allow(dead_code)]
    path: PathBuf,
    header: SegmentHeader,
    file: BufReader<File>,
    /// Total file length captured at open — used by `iter()` so it
    /// can compare to the running position and surface ShortRead
    /// before issuing the read.
    file_len: u64,
}

impl SegmentReader {
    pub fn open(path: &Path) -> Result<Self, ReaderError> {
        let file = File::open(path)?;
        let file_len = file.metadata()?.len();
        let mut file = BufReader::new(file);
        let mut header_bytes = [0u8; SEGMENT_HEADER_LEN];
        match file.read_exact(&mut header_bytes) {
            Ok(()) => (),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(ReaderError::Header(SegmentHeaderError::Truncated(
                    file_len as usize,
                )));
            }
            Err(e) => return Err(ReaderError::Io(e)),
        };
        let header = SegmentHeader::parse(&header_bytes)?;
        Ok(Self { path: path.to_owned(), header, file, file_len })
    }

    pub fn header(&self) -> &SegmentHeader {
        &self.header
    }

    pub fn first_cursor(&self) -> u64 {
        self.header.first_cursor
    }

    pub fn file_len(&self) -> u64 {
        self.file_len
    }

    /// Yield records in order from the current position.  Stops at
    /// the first error (the caller decides whether to give up or
    /// invoke `recover_truncate`).
    pub fn iter(&mut self) -> RecordIter<'_> {
        RecordIter { reader: self, pos: SEGMENT_HEADER_LEN as u64, halted: false }
    }
}

/// Iterator returned by [`SegmentReader::iter`].  Yields
/// `Result<Record, ReaderError>`; halts permanently on the first
/// error.
pub struct RecordIter<'a> {
    reader: &'a mut SegmentReader,
    pos: u64,
    halted: bool,
}

impl<'a> Iterator for RecordIter<'a> {
    type Item = Result<Record, ReaderError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.halted {
            return None;
        }
        match read_one_record(&mut self.reader.file, self.pos, self.reader.file_len) {
            Ok(Some((record, advance))) => {
                self.pos += advance;
                Some(Ok(record))
            }
            Ok(None) => None, // clean EOF
            Err(e) => {
                self.halted = true;
                Some(Err(e))
            }
        }
    }
}

/// Read one record starting at byte `pos` in `file`.  Returns:
///
/// * `Ok(Some((record, bytes_consumed)))` on success.
/// * `Ok(None)` when `pos == file_len` (clean EOF).
/// * `Err(ReaderError)` for any structural failure.
fn read_one_record(
    file: &mut BufReader<File>,
    pos: u64,
    file_len: u64,
) -> Result<Option<(Record, u64)>, ReaderError> {
    let remaining = file_len.saturating_sub(pos);
    if remaining == 0 {
        return Ok(None);
    }
    if remaining < 4 {
        return Err(ReaderError::ShortRead { offset: pos, needed: 4 - remaining });
    }

    file.seek(SeekFrom::Start(pos))?;
    let mut len_bytes = [0u8; 4];
    file.read_exact(&mut len_bytes)?;
    let record_len = u32::from_le_bytes(len_bytes) as usize;
    if record_len > MAX_RECORD_LEN {
        return Err(ReaderError::OversizeRecord { record_len });
    }
    if record_len < RECORD_FRAMING {
        return Err(ReaderError::OversizeRecord { record_len });
    }

    let total_after_len = record_len - 4; // bytes that follow the record_len prefix
    let remaining_after_len = remaining - 4;
    if (remaining_after_len as usize) < total_after_len {
        return Err(ReaderError::ShortRead {
            offset: pos,
            needed: total_after_len as u64 - remaining_after_len,
        });
    }

    // Read the body + crc into a single buffer so the CRC over the
    // body is one slice op (no second seek).
    let mut body_and_crc = vec![0u8; total_after_len];
    file.read_exact(&mut body_and_crc)?;
    let body_len = total_after_len - 4;
    let body = &body_and_crc[..body_len];
    let stored_crc = u32::from_le_bytes(body_and_crc[body_len..total_after_len].try_into().unwrap());
    let computed_crc = crc32c::crc32c(body);
    if stored_crc != computed_crc {
        return Err(ReaderError::CrcMismatch {
            offset: pos,
            stored: stored_crc,
            computed: computed_crc,
        });
    }

    // Decode body fields: cursor (8) + ts (8) + payload_len (4) + payload.
    let cursor = u64::from_le_bytes(body[0..8].try_into().unwrap());
    let ts = u64::from_le_bytes(body[8..16].try_into().unwrap());
    let payload_len = u32::from_le_bytes(body[16..20].try_into().unwrap()) as usize;
    // Defensive: payload_len must match what record_len implies.
    if payload_len + RECORD_FRAMING != record_len {
        return Err(ReaderError::OversizeRecord { record_len });
    }
    let payload = body[20..20 + payload_len].to_vec();

    Ok(Some((
        Record { cursor, ts_unix_nanos: ts, payload },
        record_len as u64,
    )))
}

/// Outcome of a recovery scan over a single segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryReport {
    /// File length after recovery (truncated, or unchanged if no
    /// corruption was found).
    pub final_len: u64,
    /// Bytes dropped by the truncate (0 if the segment was clean).
    pub bytes_dropped: u64,
    /// Cursor of the highest intact record in the segment.  Equals
    /// `first_cursor - 1` (i.e. the segment is "before" its own
    /// first_cursor) when no records survived.
    pub last_cursor: Option<u64>,
}

/// Scan `path` from the segment header forward; on the first
/// `ReaderError::CrcMismatch` / `OversizeRecord` / `ShortRead`,
/// `ftruncate` the file at the start of the bad record.  Logs a
/// WARN to stderr with the truncate offset.
///
/// Returns a `RecoveryReport` summarising the operation.  Idempotent:
/// a re-run on an already-clean segment is a no-op.
pub fn recover_truncate(path: &Path) -> Result<RecoveryReport, ReaderError> {
    let mut reader = SegmentReader::open(path)?;
    let original_len = reader.file_len;
    let mut pos = SEGMENT_HEADER_LEN as u64;
    // last_good_end is the byte offset where the next record would
    // start if everything past `pos` were valid — it advances as we
    // successfully decode each record.  On corruption we truncate to
    // this value, dropping the first bad record + anything after it.
    let mut last_good_end: u64 = pos;
    let mut last_cursor: Option<u64> = None;
    let mut hit_bad_record = false;

    loop {
        match read_one_record(&mut reader.file, pos, reader.file_len) {
            Ok(None) => break, // clean EOF
            Ok(Some((record, advance))) => {
                pos += advance;
                last_good_end = pos;
                last_cursor = Some(record.cursor);
            }
            Err(ReaderError::CrcMismatch { .. })
            | Err(ReaderError::OversizeRecord { .. })
            | Err(ReaderError::ShortRead { .. }) => {
                hit_bad_record = true;
                break;
            }
            Err(e) => return Err(e),
        }
    }

    if hit_bad_record {
        // Truncate the file at last_good_end (which is the start of
        // the first bad record).
        drop(reader);
        let file = OpenOptions::new().write(true).open(path)?;
        file.set_len(last_good_end)?;
        file.sync_all()?;
        let dropped = original_len - last_good_end;
        eprintln!(
            "mmbus::wal: recover_truncate {} dropped {} bytes at offset {}",
            path.display(),
            dropped,
            last_good_end
        );
        Ok(RecoveryReport {
            final_len: last_good_end,
            bytes_dropped: dropped,
            last_cursor,
        })
    } else {
        Ok(RecoveryReport {
            final_len: original_len,
            bytes_dropped: 0,
            last_cursor,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::segment_writer::SegmentWriter;
    use std::io::{Seek, Write};

    fn tmpdir() -> tempfile::TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    fn write_segment(path: &Path, first_cursor: u64, records: &[(u64, &[u8])]) {
        let mut w = SegmentWriter::create(path, first_cursor).unwrap();
        for (c, p) in records {
            w.append(*c, 0, p).unwrap();
        }
        w.close().unwrap();
    }

    #[test]
    fn opens_and_iterates_clean_segment() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        write_segment(&path, 0, &[(0, b"alpha"), (1, b"beta"), (2, b"gamma")]);
        let mut r = SegmentReader::open(&path).unwrap();
        assert_eq!(r.first_cursor(), 0);
        let records: Vec<_> = r.iter().collect::<Result<_, _>>().unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].cursor, 0);
        assert_eq!(records[0].payload, b"alpha");
        assert_eq!(records[1].payload, b"beta");
        assert_eq!(records[2].payload, b"gamma");
    }

    #[test]
    fn open_rejects_bad_magic() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        write_segment(&path, 0, &[]);
        // Corrupt the first byte of the magic.
        {
            let mut f = OpenOptions::new().write(true).open(&path).unwrap();
            f.seek(SeekFrom::Start(0)).unwrap();
            f.write_all(&[0xFF]).unwrap();
        }
        match SegmentReader::open(&path) {
            Err(ReaderError::Header(SegmentHeaderError::BadMagic { .. })) => (),
            Err(e) => panic!("expected BadMagic, got Err({e:?})"),
            Ok(_) => panic!("expected BadMagic, got Ok(reader)"),
        }
    }

    #[test]
    fn iter_detects_crc_mismatch() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        write_segment(&path, 0, &[(0, b"clean")]);
        // Corrupt one byte of the payload — CRC stays the same so the
        // computed CRC will differ.  Payload starts at
        // SEGMENT_HEADER_LEN + 4 (record_len) + 8 (cursor) + 8 (ts) +
        // 4 (payload_len) = 32 + 24 = 56.
        let payload_start = SEGMENT_HEADER_LEN as u64 + 4 + 8 + 8 + 4;
        {
            let mut f = OpenOptions::new().write(true).open(&path).unwrap();
            f.seek(SeekFrom::Start(payload_start)).unwrap();
            f.write_all(&[0xFF]).unwrap();
        }
        let mut r = SegmentReader::open(&path).unwrap();
        let first = r.iter().next().unwrap();
        match first {
            Err(ReaderError::CrcMismatch { .. }) => (),
            other => panic!("expected CrcMismatch, got {other:?}"),
        }
    }

    #[test]
    fn iter_detects_short_final_record() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        write_segment(&path, 0, &[(0, b"a"), (1, b"b")]);
        // Truncate to just past the first record.
        let truncate_at = {
            let r = SegmentReader::open(&path).unwrap();
            // Header + one full record = 32 + (28 + 1) = 61.
            r.file_len - 5 // chop bytes off the second record's tail
        };
        let f = OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(truncate_at).unwrap();

        let mut r = SegmentReader::open(&path).unwrap();
        let mut iter = r.iter();
        let first = iter.next().unwrap().unwrap();
        assert_eq!(first.payload, b"a");
        let second = iter.next().unwrap();
        assert!(matches!(second, Err(ReaderError::ShortRead { .. })));
    }

    #[test]
    fn recover_truncate_on_clean_segment_is_noop() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        write_segment(&path, 0, &[(0, b"x"), (1, b"y"), (2, b"z")]);
        let original_len = std::fs::metadata(&path).unwrap().len();
        let report = recover_truncate(&path).unwrap();
        assert_eq!(report.bytes_dropped, 0);
        assert_eq!(report.final_len, original_len);
        assert_eq!(report.last_cursor, Some(2));
        assert_eq!(std::fs::metadata(&path).unwrap().len(), original_len);
    }

    #[test]
    fn recover_truncate_removes_torn_tail() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        write_segment(&path, 0, &[(0, b"first"), (1, b"second")]);
        let original_len = std::fs::metadata(&path).unwrap().len();
        // Chop the last 3 bytes (corrupts the second record's CRC).
        let target_len = original_len - 3;
        let f = OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(target_len).unwrap();

        let report = recover_truncate(&path).unwrap();
        assert!(report.bytes_dropped > 0);
        // The first record was clean → last_cursor = 0; final_len is
        // header + first record = 32 + (28 + 5) = 65.
        assert_eq!(report.last_cursor, Some(0));
        let post_len = std::fs::metadata(&path).unwrap().len();
        assert_eq!(post_len, report.final_len);

        // After recovery, the segment must iterate cleanly with only
        // the first record.
        let mut r = SegmentReader::open(&path).unwrap();
        let surviving: Vec<_> = r.iter().collect::<Result<_, _>>().unwrap();
        assert_eq!(surviving.len(), 1);
        assert_eq!(surviving[0].payload, b"first");
    }

    #[test]
    fn recover_truncate_is_idempotent() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        write_segment(&path, 0, &[(0, b"a"), (1, b"b")]);
        // Corrupt the second record's payload.
        let payload_b_start =
            SEGMENT_HEADER_LEN as u64 + (RECORD_FRAMING as u64 + 1) + 4 + 8 + 8 + 4;
        {
            let mut f = OpenOptions::new().write(true).open(&path).unwrap();
            f.seek(SeekFrom::Start(payload_b_start)).unwrap();
            f.write_all(&[0xFF]).unwrap();
        }
        let first = recover_truncate(&path).unwrap();
        let second = recover_truncate(&path).unwrap();
        assert_eq!(first.final_len, second.final_len);
        assert_eq!(second.bytes_dropped, 0, "second run must be a no-op");
    }
}
