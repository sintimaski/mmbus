//! WAL on-disk record + segment-header layout.
//!
//! See `docs/rfc-wal-phase-b.md` §4.  All multi-byte fields
//! little-endian.

use std::time::{SystemTime, UNIX_EPOCH};

/// Segment header magic — `"mmbusWAL"` packed as a little-endian u64
/// (matches the ring-buffer magic style — first 8 bytes of the file
/// read in reverse via hexdump).
pub const SEGMENT_MAGIC: u64 = 0x6D6D_6275_7357_414C;

/// Current segment-format version.
pub const SEGMENT_VERSION: u32 = 1;

/// Bytes occupied by the segment header.  Written on
/// `SegmentWriter::create`; validated on `SegmentReader::open`.
pub const SEGMENT_HEADER_LEN: usize = 32;

/// Fixed per-record overhead on disk:
///   `u32 record_len + u64 cursor + u64 ts + u32 payload_len + u32 crc`
/// = 28 bytes.  Total record bytes for a payload of `N` bytes is
/// `RECORD_FRAMING + N`, which is also the value of the `record_len`
/// prefix (record_len includes itself).
pub const RECORD_FRAMING: usize = 28;

/// Hard cap on record_len.  16 MiB matches the bridge-frame cap;
/// payloads cannot exceed `MAX_RECORD_LEN - RECORD_FRAMING`.
pub const MAX_RECORD_LEN: usize = 16 * 1024 * 1024;

/// Convenience: cap on the payload size itself.
pub const MAX_PAYLOAD_LEN: usize = MAX_RECORD_LEN - RECORD_FRAMING;

/// In-memory representation of one decoded WAL record.  The codec
/// returns owned `payload` buffers — callers that want to avoid the
/// copy can use the iterator's borrow form (W1-b's SegmentReader
/// exposes both).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub cursor: u64,
    pub ts_unix_nanos: u64,
    pub payload: Vec<u8>,
}

impl Record {
    /// On-disk size of this record, including the `record_len` prefix
    /// and the trailing CRC.
    pub fn encoded_len(&self) -> usize {
        RECORD_FRAMING + self.payload.len()
    }

    /// Append the encoded record bytes to `out`.  The CRC is computed
    /// over the cursor + ts + payload_len + payload bytes (everything
    /// after `record_len`, excluding the crc itself).
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        debug_assert!(
            self.payload.len() <= MAX_PAYLOAD_LEN,
            "payload {} > MAX_PAYLOAD_LEN {}",
            self.payload.len(),
            MAX_PAYLOAD_LEN
        );
        let start = out.len();
        let record_len = (RECORD_FRAMING + self.payload.len()) as u32;
        out.extend_from_slice(&record_len.to_le_bytes());
        let crc_input_start = out.len();
        out.extend_from_slice(&self.cursor.to_le_bytes());
        out.extend_from_slice(&self.ts_unix_nanos.to_le_bytes());
        out.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.payload);
        let crc_input_end = out.len();
        let crc = crc32c::crc32c(&out[crc_input_start..crc_input_end]);
        out.extend_from_slice(&crc.to_le_bytes());
        debug_assert_eq!(out.len() - start, record_len as usize);
    }
}

/// Build the 32-byte segment header for a fresh segment whose first
/// record carries cursor `first_cursor`.  `created_unix_nanos` is
/// captured at call time.
pub fn encode_segment_header(first_cursor: u64) -> [u8; SEGMENT_HEADER_LEN] {
    let created = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let mut b = [0u8; SEGMENT_HEADER_LEN];
    b[0..8].copy_from_slice(&SEGMENT_MAGIC.to_le_bytes());
    b[8..12].copy_from_slice(&SEGMENT_VERSION.to_le_bytes());
    // bytes 12..16 = reserved, zero-initialised.
    b[16..24].copy_from_slice(&first_cursor.to_le_bytes());
    b[24..32].copy_from_slice(&created.to_le_bytes());
    b
}

/// Parsed segment header.  Returned by W1-b's `SegmentReader::open`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentHeader {
    pub first_cursor: u64,
    pub created_unix_nanos: u64,
}

/// Errors that can surface while parsing the header.  Read-side
/// users (W1-b) will wrap this in their own error type.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SegmentHeaderError {
    #[error("segment magic mismatch: got {got:#018x}, want {SEGMENT_MAGIC:#018x}")]
    BadMagic { got: u64 },
    #[error("segment version {got} not supported (this build speaks {SEGMENT_VERSION})")]
    UnsupportedVersion { got: u32 },
    #[error("segment header truncated: {0} bytes < {SEGMENT_HEADER_LEN}")]
    Truncated(usize),
}

impl SegmentHeader {
    pub fn parse(bytes: &[u8]) -> Result<Self, SegmentHeaderError> {
        if bytes.len() < SEGMENT_HEADER_LEN {
            return Err(SegmentHeaderError::Truncated(bytes.len()));
        }
        let magic = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        if magic != SEGMENT_MAGIC {
            return Err(SegmentHeaderError::BadMagic { got: magic });
        }
        let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        if version != SEGMENT_VERSION {
            return Err(SegmentHeaderError::UnsupportedVersion { got: version });
        }
        let first_cursor = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
        let created_unix_nanos = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
        Ok(Self { first_cursor, created_unix_nanos })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoded_len_matches_actual_bytes() {
        let r = Record { cursor: 42, ts_unix_nanos: 100, payload: vec![1, 2, 3, 4, 5] };
        let mut buf = Vec::new();
        r.encode_into(&mut buf);
        assert_eq!(buf.len(), r.encoded_len());
        assert_eq!(buf.len(), RECORD_FRAMING + 5);
    }

    #[test]
    fn record_len_prefix_equals_total_record_bytes() {
        let r = Record { cursor: 7, ts_unix_nanos: 0, payload: vec![0xAB; 100] };
        let mut buf = Vec::new();
        r.encode_into(&mut buf);
        let record_len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
        assert_eq!(record_len, buf.len(), "record_len includes itself");
        assert_eq!(record_len, RECORD_FRAMING + 100);
    }

    #[test]
    fn crc_is_computed_over_post_length_bytes() {
        let r = Record {
            cursor: 0x1122_3344_5566_7788,
            ts_unix_nanos: 1,
            payload: vec![0x11, 0x22, 0x33],
        };
        let mut buf = Vec::new();
        r.encode_into(&mut buf);
        // CRC bytes are the last 4 of the encoded record.
        let n = buf.len();
        let stored_crc = u32::from_le_bytes(buf[n - 4..n].try_into().unwrap());
        // CRC input is everything between record_len (first 4) and crc
        // (last 4) — i.e. cursor + ts + payload_len + payload.
        let expected_crc = crc32c::crc32c(&buf[4..n - 4]);
        assert_eq!(stored_crc, expected_crc);
    }

    #[test]
    fn segment_header_round_trips() {
        let header_bytes = encode_segment_header(1234);
        let parsed = SegmentHeader::parse(&header_bytes).unwrap();
        assert_eq!(parsed.first_cursor, 1234);
        assert!(parsed.created_unix_nanos > 0);
    }

    #[test]
    fn segment_header_rejects_bad_magic() {
        let mut b = encode_segment_header(0);
        b[0] = b[0].wrapping_add(1);
        match SegmentHeader::parse(&b) {
            Err(SegmentHeaderError::BadMagic { .. }) => (),
            other => panic!("expected BadMagic, got {other:?}"),
        }
    }

    #[test]
    fn segment_header_rejects_truncated_input() {
        let short = [0u8; 16];
        match SegmentHeader::parse(&short) {
            Err(SegmentHeaderError::Truncated(16)) => (),
            other => panic!("expected Truncated(16), got {other:?}"),
        }
    }

    #[test]
    fn segment_header_rejects_unsupported_version() {
        let mut b = encode_segment_header(0);
        b[8..12].copy_from_slice(&99u32.to_le_bytes());
        match SegmentHeader::parse(&b) {
            Err(SegmentHeaderError::UnsupportedVersion { got: 99 }) => (),
            other => panic!("expected UnsupportedVersion(99), got {other:?}"),
        }
    }
}
