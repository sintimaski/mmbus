//! Subscriber-side WAL replay (W1-e).
//!
//! Verifies that `subscribe_from(cursor)` with a cursor older than
//! the ring's oldest visible slot transparently replays through the
//! WAL and then transitions to the live ring.

use mmbus::wal::{FsyncPolicy, WalConfig};
use mmbus::{BusConfig, Publisher, StartPos, Subscriber};
use std::time::Duration;

fn cfg(name: &str, policy: FsyncPolicy) -> BusConfig {
    BusConfig {
        capacity: 8, // small ring so we can rotate cursor past the window quickly
        slot_size: 32,
        base_dir: std::env::temp_dir().join("mmbus_wal_sub").join(name),
        wal: WalConfig {
            enabled: true,
            fsync_policy: policy,
            fsync_interval: Duration::from_millis(5),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn cleanup(cfg: &BusConfig) {
    let _ = std::fs::remove_dir_all(&cfg.base_dir);
}

#[test]
fn subscribe_from_zero_replays_all_records_via_wal() {
    let cfg = cfg("from_zero", FsyncPolicy::Each);
    cleanup(&cfg);
    // Publish 20 records into a capacity-8 ring so cursors 0..11 fall
    // out of the ring's window.
    let mut p = Publisher::create("bus", cfg.clone()).unwrap();
    // No live subscriber → publisher would block on Error backpressure;
    // we publish under DropOldest, which is the default.
    for i in 0..20u64 {
        p.publish(&i.to_le_bytes()).unwrap();
    }
    drop(p);

    // Reopen the publisher so the ring keeps the WAL-aligned tail (20).
    let _p2 = Publisher::create("bus", cfg.clone()).unwrap();

    let mut sub =
        Subscriber::connect_with("bus", &cfg, Duration::from_secs(5), StartPos::Explicit(0))
            .unwrap();

    // The 20 records from cursor 0..20 must all surface, in order.
    let mut got: Vec<u64> = Vec::new();
    for _ in 0..20 {
        let payload = sub
            .receive_timeout(Duration::from_secs(2))
            .unwrap()
            .expect("WAL must deliver every prior record");
        let value = u64::from_le_bytes(payload.try_into().unwrap());
        got.push(value);
    }
    assert_eq!(got, (0..20).collect::<Vec<_>>());

    cleanup(&cfg);
}

#[test]
fn subscribe_from_mid_cursor_replays_from_that_point() {
    let cfg = cfg("from_mid", FsyncPolicy::Each);
    cleanup(&cfg);
    let mut p = Publisher::create("bus", cfg.clone()).unwrap();
    for i in 0..15u64 {
        p.publish(&i.to_le_bytes()).unwrap();
    }
    drop(p);
    let _p2 = Publisher::create("bus", cfg.clone()).unwrap();

    let mut sub =
        Subscriber::connect_with("bus", &cfg, Duration::from_secs(5), StartPos::Explicit(7))
            .unwrap();
    let mut got: Vec<u64> = Vec::new();
    for _ in 7..15 {
        let payload = sub
            .receive_timeout(Duration::from_secs(2))
            .unwrap()
            .expect("WAL must deliver from requested cursor");
        got.push(u64::from_le_bytes(payload.try_into().unwrap()));
    }
    assert_eq!(got, (7..15).collect::<Vec<_>>());

    cleanup(&cfg);
}

#[test]
fn subscribe_from_too_old_returns_cursor_too_old() {
    let cfg_base = BusConfig {
        capacity: 8,
        slot_size: 32,
        base_dir: std::env::temp_dir().join("mmbus_wal_sub").join("too_old"),
        wal: WalConfig {
            enabled: true,
            fsync_policy: FsyncPolicy::Each,
            // Tiny retention forces oldest cursors to be deleted.
            segment_size_max: 200,
            retention_bytes: 200,
            fsync_interval: Duration::from_millis(5),
            ..Default::default()
        },
        ..Default::default()
    };
    cleanup(&cfg_base);
    let mut p = Publisher::create("bus", cfg_base.clone()).unwrap();
    for i in 0..50u64 {
        p.publish(&[i as u8; 16]).unwrap();
    }
    let wal_stats = p.stats().wal.unwrap();
    assert!(
        wal_stats.oldest_cursor > 0,
        "retention must have deleted older segments"
    );
    drop(p);
    let _p2 = Publisher::create("bus", cfg_base.clone()).unwrap();

    match Subscriber::connect_with(
        "bus",
        &cfg_base,
        Duration::from_secs(2),
        StartPos::Explicit(0),
    ) {
        Err(mmbus::Error::CursorTooOld {
            requested: 0,
            oldest,
        }) => {
            assert!(oldest > 0);
        }
        Ok(_) => panic!("expected CursorTooOld for cursor 0, got Ok"),
        Err(e) => panic!("expected CursorTooOld, got {e:?}"),
    }

    cleanup(&cfg_base);
}

#[test]
fn replay_then_live_ring_transitions_cleanly() {
    let cfg = cfg("replay_then_live", FsyncPolicy::Each);
    cleanup(&cfg);
    // First, build up some history via a publisher that exits.
    {
        let mut p = Publisher::create("bus", cfg.clone()).unwrap();
        for i in 0..12u64 {
            p.publish(&i.to_le_bytes()).unwrap();
        }
        drop(p);
    }
    // Restart the publisher: ring tail aligns at 12 (per W1-d).  We can
    // now subscribe_from(0) — WAL serves cursors 0..12, then live ring
    // continues from 12.
    let mut p2 = Publisher::create("bus", cfg.clone()).unwrap();

    let mut sub =
        Subscriber::connect_with("bus", &cfg, Duration::from_secs(5), StartPos::Explicit(0))
            .unwrap();
    p2.wait_for_subscribers(1, Duration::from_secs(5)).unwrap();

    // Drain the WAL prefix.
    for i in 0..12u64 {
        let payload = sub
            .receive_timeout(Duration::from_secs(2))
            .unwrap()
            .expect("wal prefix");
        let value = u64::from_le_bytes(payload.try_into().unwrap());
        assert_eq!(value, i, "WAL replay must yield records in order");
    }

    // Publish 5 fresh records — these MUST come from the live ring.
    for i in 12..17u64 {
        p2.publish(&i.to_le_bytes()).unwrap();
    }
    for i in 12..17u64 {
        let payload = sub
            .receive_timeout(Duration::from_secs(2))
            .unwrap()
            .expect("live ring continuation");
        let value = u64::from_le_bytes(payload.try_into().unwrap());
        assert_eq!(value, i);
    }

    cleanup(&cfg);
}

#[test]
fn subscribe_from_works_when_cursor_is_inside_ring() {
    let cfg = cfg("inside_ring", FsyncPolicy::Each);
    cleanup(&cfg);
    let mut p = Publisher::create("bus", cfg.clone()).unwrap();
    for i in 0..3u64 {
        p.publish(&i.to_le_bytes()).unwrap();
    }
    // Cursor 1 is < tail (3) and >= ring_oldest (max(0, 3 - 8) = 0).
    // The legacy in-ring path must still serve it — no WAL replayer.
    let mut sub =
        Subscriber::connect_with("bus", &cfg, Duration::from_secs(5), StartPos::Explicit(1))
            .unwrap();
    let got = sub
        .receive_timeout(Duration::from_secs(2))
        .unwrap()
        .expect("ring record");
    assert_eq!(u64::from_le_bytes(got.try_into().unwrap()), 1);
    drop(p);
    cleanup(&cfg);
}
