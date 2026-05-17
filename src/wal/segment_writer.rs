//! Single-segment writer.  Wraps a `BufWriter<File>` so per-append
//! cost is one user-space `write_all`; `fsync()` is the only operation
//! that crosses the kernel boundary for durability.
//!
//! Used by W1-c's `Wal` aggregator (one writer per active segment;
//! rotation closes the writer and opens a new one with a fresh
//! first_cursor).

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::wal::record::{
    encode_segment_header, Record, MAX_PAYLOAD_LEN, SEGMENT_HEADER_LEN,
};

/// Owns the on-disk file handle for one WAL segment.
pub struct SegmentWriter {
    path: PathBuf,
    file: BufWriter<File>,
    /// Cursor stamped into the segment header — equals the cursor of
    /// the first record we'll append.
    first_cursor: u64,
    /// `last_appended_cursor + 1`, equal to `first_cursor` before any
    /// append.  This is the "next cursor to be written" pointer.
    pending_cursor: u64,
    /// Updated by `fsync()`; equals the highest cursor durable on
    /// disk.  Subscribers under `FsyncPolicy::Batched` are clamped to
    /// this value.
    durable_cursor: u64,
    /// Total bytes written to the underlying file (header + every
    /// record).  Tracks file size without an extra `stat` syscall;
    /// used by the WAL aggregator to detect when to rotate.
    bytes_written: u64,
    /// Reusable encode buffer — avoids per-append allocation.
    scratch: Vec<u8>,
}

#[derive(Debug, thiserror::Error)]
pub enum WriterError {
    #[error("payload too large: {payload_len} > MAX_PAYLOAD_LEN ({MAX_PAYLOAD_LEN})")]
    PayloadTooLarge { payload_len: usize },

    #[error("cursor must be monotonic: tried to append {got} after pending_cursor was {expected}")]
    CursorRegressed { got: u64, expected: u64 },

    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

impl SegmentWriter {
    /// Open a fresh segment file at `path` and write its header.
    /// Fails (with `WriterError::Io` of kind `AlreadyExists`) if
    /// `path` already exists — segment filenames are deterministic
    /// (`<first_cursor>.seg`) so collisions indicate a logic bug.
    pub fn create(path: &Path, first_cursor: u64) -> Result<Self, WriterError> {
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)?;
        let mut file = BufWriter::new(file);
        let header = encode_segment_header(first_cursor);
        file.write_all(&header)?;
        Ok(Self {
            path: path.to_owned(),
            file,
            first_cursor,
            pending_cursor: first_cursor,
            durable_cursor: first_cursor,
            bytes_written: SEGMENT_HEADER_LEN as u64,
            scratch: Vec::with_capacity(4096),
        })
    }

    /// Append one record.  Does not fsync — call [`Self::fsync`] to
    /// make the record durable.  Cursor MUST equal
    /// `pending_cursor()`; out-of-order appends are a logic bug
    /// (the WAL is a strict log of the SPMC publish stream).
    pub fn append(
        &mut self,
        cursor: u64,
        ts_unix_nanos: u64,
        payload: &[u8],
    ) -> Result<(), WriterError> {
        if payload.len() > MAX_PAYLOAD_LEN {
            return Err(WriterError::PayloadTooLarge { payload_len: payload.len() });
        }
        if cursor != self.pending_cursor {
            return Err(WriterError::CursorRegressed {
                got: cursor,
                expected: self.pending_cursor,
            });
        }
        self.scratch.clear();
        Record {
            cursor,
            ts_unix_nanos,
            // Re-encode without copying the payload — encode_into
            // reads from a slice, but we need a Record by value.
            // The temporary Vec here is dropped immediately after
            // encode; in practice the optimiser inlines the slice
            // copy.  Profile if it ever matters.
            payload: payload.to_vec(),
        }
        .encode_into(&mut self.scratch);
        self.file.write_all(&self.scratch)?;
        self.bytes_written += self.scratch.len() as u64;
        self.pending_cursor = cursor + 1;
        Ok(())
    }

    /// Flush the in-memory BufWriter to the OS, then `fdatasync` so
    /// the kernel pushes the bytes to stable storage.  Returns the
    /// new `durable_cursor`.
    pub fn fsync(&mut self) -> Result<u64, WriterError> {
        self.file.flush()?;
        self.file.get_ref().sync_data()?;
        self.durable_cursor = self.pending_cursor;
        Ok(self.durable_cursor)
    }

    /// Final fsync + drop.  Equivalent to `fsync()` then letting
    /// the writer fall out of scope; named separately so callers
    /// can explicit the close point.
    pub fn close(mut self) -> Result<(), WriterError> {
        self.fsync()?;
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn first_cursor(&self) -> u64 {
        self.first_cursor
    }
    /// `last_appended_cursor + 1`.  Equal to [`Self::first_cursor`]
    /// before any append.
    pub fn pending_cursor(&self) -> u64 {
        self.pending_cursor
    }
    /// Highest cursor durable on disk.  Lags `pending_cursor` until
    /// `fsync()` is called.
    pub fn durable_cursor(&self) -> u64 {
        self.durable_cursor
    }
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::record::{SegmentHeader, SEGMENT_HEADER_LEN};
    use std::io::Read;

    fn tmpdir() -> tempfile::TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    fn read_all(path: &Path) -> Vec<u8> {
        let mut buf = Vec::new();
        File::open(path).unwrap().read_to_end(&mut buf).unwrap();
        buf
    }

    #[test]
    fn creates_header_on_open() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        let w = SegmentWriter::create(&path, 42).expect("create");
        // The writer's BufWriter holds the header bytes until flush.
        // Force the flush so an external reader can see them.
        drop(w);
        let bytes = read_all(&path);
        assert_eq!(bytes.len(), SEGMENT_HEADER_LEN);
        let header = SegmentHeader::parse(&bytes).expect("parse header");
        assert_eq!(header.first_cursor, 42);
    }

    #[test]
    fn append_writes_expected_bytes() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        let mut w = SegmentWriter::create(&path, 0).unwrap();
        w.append(0, 1000, b"hello").unwrap();
        w.fsync().unwrap();
        let bytes = read_all(&path);
        // Header + one record of payload len 5.
        assert_eq!(
            bytes.len(),
            SEGMENT_HEADER_LEN + crate::wal::record::RECORD_FRAMING + 5
        );
        // Spot-check the record's record_len prefix at byte 32.
        let record_len =
            u32::from_le_bytes(bytes[32..36].try_into().unwrap()) as usize;
        assert_eq!(record_len, crate::wal::record::RECORD_FRAMING + 5);
    }

    #[test]
    fn fsync_advances_durable_cursor() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        let mut w = SegmentWriter::create(&path, 0).unwrap();
        assert_eq!(w.durable_cursor(), 0);
        assert_eq!(w.pending_cursor(), 0);
        w.append(0, 0, b"a").unwrap();
        w.append(1, 0, b"b").unwrap();
        assert_eq!(w.pending_cursor(), 2);
        assert_eq!(w.durable_cursor(), 0, "no fsync yet");
        let durable = w.fsync().unwrap();
        assert_eq!(durable, 2);
        assert_eq!(w.durable_cursor(), 2);
    }

    #[test]
    fn bytes_written_matches_file_size() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        let mut w = SegmentWriter::create(&path, 0).unwrap();
        for i in 0..10u64 {
            w.append(i, 0, &i.to_le_bytes()).unwrap();
        }
        let claimed = w.bytes_written();
        w.fsync().unwrap();
        let actual = std::fs::metadata(&path).unwrap().len();
        assert_eq!(claimed, actual);
    }

    #[test]
    fn rejects_oversize_payload() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        let mut w = SegmentWriter::create(&path, 0).unwrap();
        let payload = vec![0u8; MAX_PAYLOAD_LEN + 1];
        match w.append(0, 0, &payload) {
            Err(WriterError::PayloadTooLarge { payload_len }) => {
                assert_eq!(payload_len, MAX_PAYLOAD_LEN + 1);
            }
            other => panic!("expected PayloadTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn rejects_out_of_order_cursor() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        let mut w = SegmentWriter::create(&path, 10).unwrap();
        match w.append(11, 0, b"x") {
            Err(WriterError::CursorRegressed { got: 11, expected: 10 }) => (),
            other => panic!("expected CursorRegressed, got {other:?}"),
        }
    }

    #[test]
    fn create_fails_if_path_exists() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        let _w1 = SegmentWriter::create(&path, 0).expect("first create");
        match SegmentWriter::create(&path, 0) {
            Err(WriterError::Io(e)) => {
                assert_eq!(e.kind(), io::ErrorKind::AlreadyExists);
            }
            Err(e) => panic!("expected Io(AlreadyExists), got Err({e:?})"),
            Ok(_) => panic!("expected Io(AlreadyExists), got Ok"),
        }
    }

    #[test]
    fn close_fsyncs_implicitly() {
        let dir = tmpdir();
        let path = dir.path().join("0.seg");
        let mut w = SegmentWriter::create(&path, 0).unwrap();
        w.append(0, 0, b"x").unwrap();
        // pending != durable until close/fsync.
        assert_ne!(w.pending_cursor(), w.durable_cursor());
        w.close().unwrap();
        let bytes = read_all(&path);
        assert!(bytes.len() > SEGMENT_HEADER_LEN, "close must flush");
    }
}
