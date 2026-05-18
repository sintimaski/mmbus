//! O-3: verify the observability counters on TopicStats / WalStats
//! tick on the expected code paths.

use mmbus::wal::WalConfig;
use mmbus::{BackpressurePolicy, BusConfig, Publisher, Subscriber};
use std::time::Duration;

fn cfg(name: &str, wal: WalConfig) -> BusConfig {
    let dir = std::env::temp_dir().join("mmbus_obs_counters").join(name);
    let _ = std::fs::remove_dir_all(&dir);
    BusConfig {
        capacity: 16,
        slot_size: 32,
        base_dir: dir,
        wal,
        ..Default::default()
    }
}

#[test]
fn published_total_ticks_per_publish() {
    let cfg = cfg("published_total", WalConfig::disabled());
    let mut p = Publisher::create("bus", cfg).unwrap();
    let s0 = p.stats();
    assert_eq!(s0.published_total, 0);
    for _ in 0..5 {
        p.publish(b"x").unwrap();
    }
    let s = p.stats();
    assert_eq!(s.published_total, 5);
}

#[test]
fn published_total_counts_publish_many_records() {
    let cfg = cfg("publish_many_total", WalConfig::disabled());
    let mut p = Publisher::create("bus", cfg).unwrap();
    let payloads: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d"];
    let n = p.publish_many(&payloads).unwrap();
    assert_eq!(n, 4);
    assert_eq!(p.stats().published_total, 4);
}

#[test]
fn full_rejected_total_ticks_when_error_backpressure_full() {
    let mut cfg = cfg("full_rejected", WalConfig::disabled());
    cfg.backpressure = BackpressurePolicy::Error;
    cfg.capacity = 4;
    let mut p = Publisher::create("bus", cfg.clone()).unwrap();
    // Attach a non-draining subscriber so the ring actually fills.
    // Without one, is_full returns false (no cursors claim the tail).
    let sub_cfg = cfg.clone();
    let _sub_handle = std::thread::spawn(move || {
        let _sub = Subscriber::connect("bus", &sub_cfg, Duration::from_secs(5)).unwrap();
        std::thread::sleep(Duration::from_secs(2));
    });
    p.wait_for_subscribers(1, Duration::from_secs(5)).unwrap();
    // Fill the ring (4 slots).
    for _ in 0..4 {
        p.publish(b"x").unwrap();
    }
    // 5th publish should fail with Full and bump full_rejected_total.
    assert!(p.publish(b"x").is_err());
    let s = p.stats();
    assert_eq!(s.full_rejected_total, 1);
    assert_eq!(s.published_total, 4);
}

#[test]
fn wal_appends_total_ticks_per_published_record() {
    let cfg = cfg("wal_appends", WalConfig::batched());
    let mut p = Publisher::create("bus", cfg).unwrap();
    for _ in 0..3 {
        p.publish(b"x").unwrap();
    }
    let s = p.stats().wal.expect("wal enabled");
    assert_eq!(s.appends_total, 3);
    assert_eq!(s.append_bytes_total, 3);
}

#[test]
fn wal_flushes_total_ticks_under_each_policy() {
    let mut cfg = cfg("wal_flushes_each", WalConfig::batched());
    cfg.wal.fsync_policy = mmbus::wal::FsyncPolicy::Each;
    let mut p = Publisher::create("bus", cfg).unwrap();
    for _ in 0..4 {
        p.publish(b"x").unwrap();
    }
    let s = p.stats().wal.expect("wal enabled");
    // Each policy fsyncs inline per publish.
    assert_eq!(s.flushes_total, 4);
}

#[test]
fn subscribers_dropped_total_ticks_when_peer_disconnects() {
    let cfg = cfg("subs_dropped", WalConfig::disabled());
    let mut p = Publisher::create("bus", cfg.clone()).unwrap();
    let sub_cfg = cfg.clone();
    let sub_handle = std::thread::spawn(move || {
        let _sub = Subscriber::connect("bus", &sub_cfg, Duration::from_secs(5)).unwrap();
        // Drop after a moment so the publisher sees the disconnect.
        std::thread::sleep(Duration::from_millis(50));
    });
    p.wait_for_subscribers(1, Duration::from_secs(5)).unwrap();
    sub_handle.join().unwrap();
    // Subscriber dropped — next broadcast_wakeup discovers it.
    // publish() triggers broadcast_wakeup; observe drop counter.
    p.publish(b"x").unwrap();
    let s = p.stats();
    assert_eq!(s.subscribers_dropped_total, 1, "should have dropped 1 peer");
}
