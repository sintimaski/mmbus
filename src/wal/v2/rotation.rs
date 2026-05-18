//! Segment rotation primitives (W2-3).
//!
//! Single-writer rotation: when [`MmapSegmentWriter::append`] returns
//! [`AppendOutcome::SegmentFull`](crate::wal::v2::mmap_segment_writer::AppendOutcome::SegmentFull),
//! the aggregator (W2-4) calls [`rotate`] which:
//!
//! 1. Writes a `SKIP_TO_END` sentinel at the dying segment's tail so
//!    any in-flight reader transitions to `ReadOutcome::EndOfSegment`
//!    cleanly.
//! 2. Creates the next segment file (`{new_first_cursor:020}.seg`).
//! 3. Updates `wal/active.dat` so future-opening subscribers find the
//!    new segment, and existing subscribers re-reading the coord file
//!    discover it.
//!
//! Crash safety: each step is restartable.
//!
//! * Crash after step 1, before step 2: replay sees a closed segment;
//!   recovery creates a fresh one from the next cursor.
//! * Crash after step 2, before step 3: two segment files exist on
//!   disk; recovery picks the one with the higher `first_cursor` and
//!   updates `active.dat` to match.
//! * Crash during step 3: `active.dat` write is a single
//!   `AtomicU64::store` to a stable mmap page — torn at most for the
//!   `generation` slot (a u64 store on aligned memory is naturally
//!   atomic on every platform we target).

use crate::wal::v2::active::ActiveCoord;
use crate::wal::v2::mmap_segment_writer::{MmapSegmentWriter, WriterError};
use std::path::{Path, PathBuf};

/// Filename convention for a v2 segment: zero-padded first_cursor
/// + `.seg`.  20 digits covers the full u64 range so files sort
///   lexically by cursor (same convention as v0.1).
pub fn segment_path(dir: &Path, first_cursor: u64) -> PathBuf {
    dir.join(format!("{first_cursor:020}.seg"))
}

/// Errors returned by the rotation helpers.  Mirrors the writer's
/// error shape since rotation is mostly delegating to it.
#[derive(Debug, thiserror::Error)]
pub enum RotateError {
    #[error("writer: {0}")]
    Writer(#[from] WriterError),

    #[error("I/O error updating active.dat: {0}")]
    Io(#[from] std::io::Error),
}

/// Perform one rotation: close the dying segment with a SKIP_TO_END
/// marker, create a fresh segment for `new_first_cursor`, and
/// publish it via `active.dat`.  Returns the new [`MmapSegmentWriter`]
/// — the caller drops the old one to release its mmap.
///
/// Safe to call only from the single-writer side (aggregator).
pub fn rotate(
    dir: &Path,
    old_writer: &MmapSegmentWriter,
    active: &ActiveCoord,
    new_first_cursor: u64,
    segment_size: usize,
) -> Result<MmapSegmentWriter, RotateError> {
    // 1. Mark the old segment closed.
    old_writer.write_skip_to_end()?;
    // Push dirty pages so concurrent readers see the marker promptly
    // (msync is async; the OS will write back regardless, this just
    // shortens the window).
    let _ = old_writer.flush_async();

    // 2. Create the new segment.
    let new_path = segment_path(dir, new_first_cursor);
    let new_writer = MmapSegmentWriter::create(&new_path, segment_size, new_first_cursor)?;

    // 3. Publish via active.dat.  Order matters: file exists before
    //    the coord update so subscribers never see an active_first
    //    pointing at a missing file.
    active.store_active(new_first_cursor)?;

    Ok(new_writer)
}

/// Open `segment_path(dir, first_cursor)` as a
/// [`MmapSegmentReader`](crate::wal::v2::mmap_segment_reader::MmapSegmentReader).
/// Convenience used by the W2-4 aggregator and tests.
pub fn open_segment_reader(
    dir: &Path,
    first_cursor: u64,
) -> Result<crate::wal::v2::mmap_segment_reader::MmapSegmentReader, crate::wal::v2::mmap_segment_reader::ReaderError>
{
    let path = segment_path(dir, first_cursor);
    crate::wal::v2::mmap_segment_reader::MmapSegmentReader::open(&path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::v2::mmap_segment_reader::{MmapSegmentReader, ReadOutcome};
    use crate::wal::v2::mmap_segment_writer::AppendOutcome;
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    fn tmpdir() -> TempDir {
        tempfile::tempdir().expect("tempdir")
    }

    /// Helper that builds a tiny segment + writer pair sized so the
    /// second append overflows.  Returns the dir + active coord +
    /// writer holding cursor=0.
    fn tiny_setup(payload_len: usize) -> (TempDir, ActiveCoord, MmapSegmentWriter) {
        let dir = tmpdir();
        let active = ActiveCoord::open_or_create(dir.path()).unwrap();
        active.store_active(0).unwrap();
        // Header (32) + one aligned record only.  RECORD_FRAMING=28,
        // so aligned size = align_up_8(28 + payload_len).
        let aligned = ((28 + payload_len) + 7) & !7;
        let segment_size = 32 + aligned;
        let path = segment_path(dir.path(), 0);
        let w = MmapSegmentWriter::create(&path, segment_size, 0).unwrap();
        (dir, active, w)
    }

    #[test]
    fn segment_path_zero_pads_to_20_digits() {
        let dir = std::path::Path::new("/tmp/wal");
        assert_eq!(
            segment_path(dir, 0).file_name().unwrap().to_str().unwrap(),
            "00000000000000000000.seg"
        );
        assert_eq!(
            segment_path(dir, 12345).file_name().unwrap().to_str().unwrap(),
            "00000000000000012345.seg"
        );
        assert_eq!(
            segment_path(dir, u64::MAX).file_name().unwrap().to_str().unwrap(),
            "18446744073709551615.seg"
        );
    }

    #[test]
    fn write_skip_to_end_makes_reader_see_end_of_segment() {
        let dir = tmpdir();
        let path = segment_path(dir.path(), 0);
        let w = MmapSegmentWriter::create(&path, 4096, 0).unwrap();
        // No records — write the marker at tail = SEGMENT_HEADER_LEN.
        let wrote = w.write_skip_to_end().unwrap();
        assert!(wrote, "should have written the marker");
        w.flush_async().unwrap();
        drop(w);

        let mut r = MmapSegmentReader::open(&path).unwrap();
        match r.next_record() {
            ReadOutcome::EndOfSegment => (),
            other => panic!("expected EndOfSegment, got {other:?}"),
        }
    }

    #[test]
    fn write_skip_to_end_no_room_returns_false() {
        let dir = tmpdir();
        let path = segment_path(dir.path(), 0);
        // Exactly the header + one 32-byte record.  After one append
        // the tail is at segment_size, so no room for the marker.
        let segment_size = 32 + 32;
        let w = MmapSegmentWriter::create(&path, segment_size, 0).unwrap();
        assert!(matches!(w.append(0, 0, b"a").unwrap(), AppendOutcome::Ok { .. }));
        let wrote = w.write_skip_to_end().unwrap();
        assert!(!wrote, "no room for marker — should return false");
        // Reader still sees EndOfSegment after the one record.
        w.flush_async().unwrap();
        drop(w);
        let mut r = MmapSegmentReader::open(&path).unwrap();
        assert!(matches!(r.next_record(), ReadOutcome::Record(_)));
        assert!(matches!(r.next_record(), ReadOutcome::EndOfSegment));
    }

    #[test]
    fn rotate_creates_new_segment_and_updates_active_dat() {
        let (dir, active, w) = tiny_setup(8);
        // Fill the first segment with one record so the next would
        // overflow.
        match w.append(0, 0, b"01234567").unwrap() {
            AppendOutcome::Ok { next_cursor, .. } => {
                assert_eq!(next_cursor, 1);
            }
            other => panic!("first append should fit, got {other:?}"),
        }
        // Confirm the segment is now full.
        assert_eq!(w.append(1, 0, b"01234567").unwrap(), AppendOutcome::SegmentFull);

        // Rotate to new first_cursor = 1.
        let new_w = rotate(dir.path(), &w, &active, 1, w.segment_size()).unwrap();
        assert_eq!(new_w.first_cursor(), 1);
        // Old segment's tail moved past header (one record + maybe
        // the skip marker).  Reader observation in next test.
        assert_eq!(active.load_first_cursor(), 1);
        assert_eq!(active.load_generation(), 2);

        // New segment file exists at the canonical path.
        assert!(segment_path(dir.path(), 1).exists());
        // Old segment file still exists (retention is a separate
        // concern from rotation).
        assert!(segment_path(dir.path(), 0).exists());

        // The new writer accepts an append.
        match new_w.append(1, 0, b"hi").unwrap() {
            AppendOutcome::Ok { next_cursor, .. } => assert_eq!(next_cursor, 2),
            other => panic!("new segment should accept append, got {other:?}"),
        }
    }

    #[test]
    fn reader_follows_rotation_via_active_dat() {
        // E2E test: writer publishes 3 records, rotates after the
        // first, publishes 2 more in the new segment.  Reader pump
        // walks both segments using active.dat as the discovery
        // mechanism.
        let dir = tmpdir();
        let active = ActiveCoord::open_or_create(dir.path()).unwrap();
        // Segment must hold 2 records (~40B aligned each) in segment 1
        // since we cram "second" + "third" there after the rotation.
        let segment_size = 32 + 40 + 40;
        let path0 = segment_path(dir.path(), 0);
        let w0 = MmapSegmentWriter::create(&path0, segment_size, 0).unwrap();
        active.store_active(0).unwrap();

        // First record goes to segment 0.
        match w0.append(0, 0, b"first").unwrap() {
            AppendOutcome::Ok { .. } => (),
            other => panic!("first append, got {other:?}"),
        }
        // Force a rotation even though segment 0 isn't full — exercises
        // the path where the aggregator decides to rotate by size policy
        // mid-segment.
        let w1 = rotate(dir.path(), &w0, &active, 1, segment_size).unwrap();
        match w1.append(1, 0, b"second").unwrap() {
            AppendOutcome::Ok { .. } => (),
            other => panic!("second append, got {other:?}"),
        }
        match w1.append(2, 0, b"third").unwrap() {
            AppendOutcome::Ok { .. } => (),
            other => panic!("third append, got {other:?}"),
        }
        w0.flush_async().unwrap();
        w1.flush_async().unwrap();

        // Reader pump.  Holds an ActiveCoord and follows rotations
        // by re-reading first_cursor on EndOfSegment.
        let reader_active = ActiveCoord::open_or_create(dir.path()).unwrap();
        let mut current_first = 0u64;
        let mut reader = MmapSegmentReader::open(&segment_path(dir.path(), current_first)).unwrap();
        let mut payloads: Vec<Vec<u8>> = Vec::new();
        loop {
            match reader.next_record() {
                ReadOutcome::Record(rec) => payloads.push(rec.payload),
                ReadOutcome::EndOfSegment => {
                    let next_first = reader_active.load_first_cursor();
                    if next_first == current_first {
                        // No new segment to chase.
                        break;
                    }
                    current_first = next_first;
                    reader = MmapSegmentReader::open(&segment_path(dir.path(), current_first))
                        .unwrap();
                }
                ReadOutcome::AwaitMore => break,
                ReadOutcome::Err(e) => panic!("reader error: {e:?}"),
            }
        }
        let payloads: Vec<&[u8]> = payloads.iter().map(|v| v.as_slice()).collect();
        assert_eq!(payloads, vec![&b"first"[..], &b"second"[..], &b"third"[..]]);
    }

    #[test]
    fn two_consecutive_rotations_chain_segments() {
        let (dir, active, w0) = tiny_setup(8);
        let segment_size = w0.segment_size();

        // Fill segment 0 with 1 record, rotate to segment 1.
        assert!(matches!(w0.append(0, 0, b"01234567").unwrap(), AppendOutcome::Ok { .. }));
        let w1 = rotate(dir.path(), &w0, &active, 1, segment_size).unwrap();
        assert_eq!(active.load_first_cursor(), 1);
        assert_eq!(active.load_generation(), 2);

        // Fill segment 1 with 1 record, rotate to segment 2.
        assert!(matches!(w1.append(1, 0, b"01234567").unwrap(), AppendOutcome::Ok { .. }));
        let w2 = rotate(dir.path(), &w1, &active, 2, segment_size).unwrap();
        assert_eq!(active.load_first_cursor(), 2);
        assert_eq!(active.load_generation(), 3);

        // Three segment files now exist.
        for fc in [0u64, 1, 2] {
            assert!(segment_path(dir.path(), fc).exists(), "missing segment {fc}");
        }
        assert_eq!(w2.first_cursor(), 2);
    }

    #[test]
    fn rotation_under_concurrent_reader_no_loss() {
        // Reader thread races the writer across one rotation.  The
        // writer publishes 100 records distributed across two
        // segments; the reader must collect all 100 in order.
        let dir = tmpdir();
        let dir_path = dir.path().to_path_buf();
        let active = Arc::new(ActiveCoord::open_or_create(&dir_path).unwrap());

        // Segment sized to hold ~40 records of 8-byte payloads (40B aligned).
        let segment_size = 32 + 40 * 40;
        let path0 = segment_path(&dir_path, 0);
        let w0 = MmapSegmentWriter::create(&path0, segment_size, 0).unwrap();
        active.store_active(0).unwrap();

        let active_writer = active.clone();
        let dir_for_writer = dir_path.clone();
        let writer_handle = thread::spawn(move || {
            let mut writer = w0;
            for i in 0u64..100 {
                let payload = i.to_le_bytes();
                loop {
                    match writer.append(i, 0, &payload).unwrap() {
                        AppendOutcome::Ok { .. } => break,
                        AppendOutcome::SegmentFull => {
                            // Rotate and retry on the new writer.
                            writer = rotate(
                                &dir_for_writer,
                                &writer,
                                &active_writer,
                                i,
                                segment_size,
                            )
                            .unwrap();
                        }
                    }
                }
            }
            // Flush before exiting so the reader sees the final segment.
            writer.flush_async().unwrap();
        });

        let active_reader = active.clone();
        let dir_for_reader = dir_path.clone();
        let reader_handle = thread::spawn(move || {
            let mut current_first = active_reader.load_first_cursor();
            let mut reader = loop {
                match MmapSegmentReader::open(&segment_path(&dir_for_reader, current_first)) {
                    Ok(r) => break r,
                    Err(_) => {
                        thread::sleep(Duration::from_millis(1));
                        current_first = active_reader.load_first_cursor();
                    }
                }
            };
            let mut got: Vec<u64> = Vec::new();
            let deadline = Instant::now() + Duration::from_secs(10);
            while got.len() < 100 {
                match reader.next_record() {
                    ReadOutcome::Record(rec) => {
                        let v = u64::from_le_bytes(rec.payload.try_into().unwrap());
                        got.push(v);
                    }
                    ReadOutcome::AwaitMore => {
                        if Instant::now() >= deadline {
                            panic!("reader timeout at {} records", got.len());
                        }
                        // Maybe the writer rotated past us — check.
                        let latest = active_reader.load_first_cursor();
                        if latest != current_first {
                            // Defer the swap until we've drained the current
                            // segment to EndOfSegment so we don't miss records.
                            std::hint::spin_loop();
                        } else {
                            std::hint::spin_loop();
                        }
                    }
                    ReadOutcome::EndOfSegment => {
                        let latest = active_reader.load_first_cursor();
                        if latest == current_first {
                            // No new segment yet — spin.
                            if Instant::now() >= deadline {
                                panic!(
                                    "reader stuck at EndOfSegment after {} records",
                                    got.len()
                                );
                            }
                            std::hint::spin_loop();
                            continue;
                        }
                        current_first = latest;
                        reader = MmapSegmentReader::open(&segment_path(
                            &dir_for_reader,
                            current_first,
                        ))
                        .unwrap();
                    }
                    ReadOutcome::Err(e) => panic!("reader error at {} records: {e:?}", got.len()),
                }
            }
            got
        });

        writer_handle.join().unwrap();
        let got = reader_handle.join().unwrap();
        assert_eq!(got.len(), 100);
        for (i, v) in got.iter().enumerate() {
            assert_eq!(*v, i as u64, "record {i} mismatch: got {v}");
        }
    }

    #[test]
    fn crash_after_new_segment_before_active_update_recovers_via_max_first_cursor() {
        // Simulate a crash: create segment 0, write a record, create
        // segment 1, but DON'T update active.dat (active still points
        // at 0 with generation 1).  Recovery picks the highest
        // first_cursor on disk and republishes it.
        let dir = tmpdir();
        let active = ActiveCoord::open_or_create(dir.path()).unwrap();
        let segment_size = 4096;
        let path0 = segment_path(dir.path(), 0);
        let w0 = MmapSegmentWriter::create(&path0, segment_size, 0).unwrap();
        active.store_active(0).unwrap();
        w0.append(0, 0, b"in-segment-0").unwrap();
        w0.write_skip_to_end().unwrap();
        let _w1 = MmapSegmentWriter::create(&segment_path(dir.path(), 1), segment_size, 1).unwrap();
        // ⤴ would normally call active.store_active(1) here but
        //   we simulate the crash by NOT doing so.
        drop(w0);
        drop(_w1);

        // Recovery scan: find the highest first_cursor segment on
        // disk and republish.  This mirrors what W2-4's Wal::open
        // will do.
        let mut highest: Option<u64> = None;
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name();
            let name = name.to_str().unwrap_or("");
            if !name.ends_with(".seg") {
                continue;
            }
            let stem = &name[..name.len() - 4];
            if let Ok(fc) = stem.parse::<u64>() {
                highest = Some(highest.map(|h| h.max(fc)).unwrap_or(fc));
            }
        }
        let highest = highest.expect("at least one segment");
        assert_eq!(highest, 1);

        // Republish.
        active.store_active(highest).unwrap();
        assert_eq!(active.load_first_cursor(), 1);
    }
}
