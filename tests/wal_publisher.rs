//! Publisher-side WAL integration (W1-d).
//!
//! Subscriber-side replay is W1-e; these tests assert observable
//! publisher behaviour: records hit the WAL before the ring publish,
//! the ring tail aligns with the WAL on restart, and recovery copes
//! with a torn segment tail.

use mmbus::wal::{FsyncPolicy, WalConfig};
use mmbus::{BusConfig, Publisher};
use std::path::PathBuf;
use std::time::Duration;

#[cfg(not(feature = "wal_v2"))]
use mmbus::wal::SegmentReader;
#[cfg(feature = "wal_v2")]
use mmbus::wal::v2::MmapSegmentReader;

fn base_cfg(name: &str, policy: FsyncPolicy) -> BusConfig {
    BusConfig {
        capacity: 16,
        slot_size: 64,
        base_dir: std::env::temp_dir().join("mmbus_wal_pub").join(name),
        wal: WalConfig {
            enabled: true,
            fsync_policy: policy,
            // Short interval so the Batched policy advances durable_cursor
            // within test poll windows.
            fsync_interval: Duration::from_millis(10),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn cleanup(cfg: &BusConfig) {
    let _ = std::fs::remove_dir_all(&cfg.base_dir);
}

fn wal_segments(cfg: &BusConfig) -> Vec<PathBuf> {
    let wal_dir = cfg.base_dir.join("bus").join("wal");
    let mut out: Vec<_> = std::fs::read_dir(&wal_dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.extension().map(|s| s == "seg").unwrap_or(false))
                .collect()
        })
        .unwrap_or_default();
    out.sort();
    out
}

#[cfg(not(feature = "wal_v2"))]
fn read_wal_records(cfg: &BusConfig) -> Vec<(u64, Vec<u8>)> {
    let mut out = Vec::new();
    for seg in wal_segments(cfg) {
        let mut r = SegmentReader::open(&seg).expect("open segment");
        while let Some(record) = r.next_record() {
            let record = record.expect("decode record");
            out.push((record.cursor, record.payload));
        }
    }
    out
}

#[cfg(feature = "wal_v2")]
fn read_wal_records(cfg: &BusConfig) -> Vec<(u64, Vec<u8>)> {
    use mmbus::wal::v2::ReadOutcome;
    let mut out = Vec::new();
    for seg in wal_segments(cfg) {
        let mut r = MmapSegmentReader::open(&seg).expect("open segment");
        loop {
            match r.next_record() {
                ReadOutcome::Record(record) => out.push((record.cursor, record.payload)),
                ReadOutcome::AwaitMore | ReadOutcome::EndOfSegment => break,
                ReadOutcome::Err(e) => panic!("decode record: {e:?}"),
            }
        }
    }
    out
}

#[test]
fn each_policy_appends_and_fsyncs_before_ring_publish() {
    let cfg = base_cfg("each_roundtrip", FsyncPolicy::Each);
    cleanup(&cfg);
    let mut p = Publisher::create("bus", cfg.clone()).unwrap();
    for i in 0..5u64 {
        p.publish(&i.to_le_bytes()).unwrap();
    }
    let stats = p.stats();
    let wal = stats.wal.expect("WAL stats present when WAL enabled");
    assert_eq!(wal.pending_cursor, 5);
    assert_eq!(
        wal.durable_cursor, 5,
        "Each policy must advance durable_cursor inline"
    );
    drop(p);
    // After drop, the on-disk records survive and decode.
    let records = read_wal_records(&cfg);
    assert_eq!(records.len(), 5);
    for (i, (cursor, payload)) in records.iter().enumerate() {
        assert_eq!(*cursor, i as u64);
        assert_eq!(payload, &(i as u64).to_le_bytes());
    }
    cleanup(&cfg);
}

#[test]
fn batched_policy_durable_cursor_advances_via_flusher() {
    let cfg = base_cfg("batched_roundtrip", FsyncPolicy::Batched);
    cleanup(&cfg);
    let mut p = Publisher::create("bus", cfg.clone()).unwrap();
    for i in 0..3u64 {
        p.publish(&i.to_le_bytes()).unwrap();
    }
    // pending_cursor is immediate; durable_cursor catches up after the
    // flusher tick (default 10 ms in this test config).
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let s = p.stats().wal.expect("wal stats");
        if s.durable_cursor == s.pending_cursor && s.pending_cursor == 3 {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "flusher never advanced durable_cursor: pending={}, durable={}",
                s.pending_cursor, s.durable_cursor
            );
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    cleanup(&cfg);
}

#[test]
fn none_policy_appends_without_inline_fsync() {
    let cfg = base_cfg("none_roundtrip", FsyncPolicy::None);
    cleanup(&cfg);
    let mut p = Publisher::create("bus", cfg.clone()).unwrap();
    for i in 0..4u64 {
        p.publish(&[i as u8; 8]).unwrap();
    }
    let s = p.stats().wal.expect("wal stats");
    assert_eq!(s.pending_cursor, 4);
    // None policy leaves durable_cursor at whatever value the WAL was
    // opened at; we just assert pending_cursor reflects every append.
    drop(p);
    cleanup(&cfg);
}

#[test]
fn ring_tail_aligns_with_wal_pending_cursor_on_restart() {
    let cfg = base_cfg("restart_align", FsyncPolicy::Each);
    cleanup(&cfg);
    {
        let mut p = Publisher::create("bus", cfg.clone()).unwrap();
        for i in 0..7u64 {
            p.publish(&i.to_le_bytes()).unwrap();
        }
        assert_eq!(p.stats().ring.tail, 7);
        drop(p);
    }
    // Reopen.  Without WAL alignment the ring would start at 0; with
    // WAL alignment it resumes at 7.
    let p2 = Publisher::create("bus", cfg.clone()).unwrap();
    let s = p2.stats();
    assert_eq!(s.ring.tail, 7, "ring tail must align with WAL pending cursor");
    let w = s.wal.expect("wal stats");
    assert_eq!(w.pending_cursor, 7);
    drop(p2);
    cleanup(&cfg);
}

// v0.1's recover_truncate ftruncates the segment to the last clean
// boundary on open.  v2's recovery model is different (in-mmap tail +
// per-slot seqlock); the equivalent corruption-recovery test for v2
// lands as part of W2-8's acceptance suite once we settle the on-disk
// post-mortem semantics for an mmap-backed WAL.
#[cfg(not(feature = "wal_v2"))]
#[test]
fn restart_after_torn_tail_drops_corrupt_records_and_resumes() {
    let cfg = base_cfg("torn_recovery", FsyncPolicy::Each);
    cleanup(&cfg);
    {
        let mut p = Publisher::create("bus", cfg.clone()).unwrap();
        for i in 0..6u64 {
            p.publish(&i.to_le_bytes()).unwrap();
        }
        drop(p);
    }
    // Corrupt the active segment: chop the final 3 bytes.  recover_truncate
    // will scan, hit the now-bad CRC on the last record, and ftruncate to
    // the last good boundary.
    let segs = wal_segments(&cfg);
    let active = segs.last().expect("at least one segment");
    let orig_len = std::fs::metadata(active).unwrap().len();
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(active)
        .unwrap();
    f.set_len(orig_len - 3).unwrap();

    // Reopen — Wal::open runs recover_truncate.
    let p2 = Publisher::create("bus", cfg.clone()).unwrap();
    let s = p2.stats();
    let wal = s.wal.expect("wal");
    // The torn last record was dropped, so pending_cursor should be < 6.
    // We don't care about the exact value (depends on whether the corrupt
    // record was the only one in the segment vs not) — only that the
    // publisher came up cleanly, the ring tail aligns with the WAL's
    // pending cursor, and a subsequent publish appends at the new cursor.
    assert!(
        wal.pending_cursor < 6,
        "torn tail must have been dropped (got pending_cursor={})",
        wal.pending_cursor
    );
    assert_eq!(s.ring.tail, wal.pending_cursor);
    drop(p2);
    cleanup(&cfg);
}

#[test]
fn disabled_wal_leaves_publisher_and_stats_unchanged() {
    let cfg = BusConfig {
        capacity: 16,
        slot_size: 32,
        base_dir: std::env::temp_dir().join("mmbus_wal_pub").join("disabled"),
        // v0.2.1 flipped Default WAL → on; this test specifically
        // verifies the disabled case so opt out explicitly.
        wal: WalConfig::disabled(),
        ..Default::default()
    };
    cleanup(&cfg);
    let mut p = Publisher::create("bus", cfg.clone()).unwrap();
    p.publish(b"x").unwrap();
    let s = p.stats();
    assert!(s.wal.is_none(), "WAL disabled — stats.wal must be None");
    // No wal directory should appear.
    assert!(
        !cfg.base_dir.join("bus").join("wal").exists(),
        "no wal/ dir should be created with WAL disabled"
    );
    cleanup(&cfg);
}
