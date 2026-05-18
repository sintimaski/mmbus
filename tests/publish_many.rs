//! `Publisher::publish_many` + `Bus::publish_many` — batched publish
//! with a single wakeup per subscriber.  Pairs with the existing
//! `Subscriber::receive_into` tests.

use mmbus::{BackpressurePolicy, BusConfig, Publisher, Subscriber};
use std::time::Duration;

fn cfg(name: &str) -> BusConfig {
    BusConfig {
        capacity: 16,
        slot_size: 32,
        base_dir: std::env::temp_dir().join("mmbus_publish_many").join(name),
        ..Default::default()
    }
}

fn cleanup(cfg: &BusConfig) {
    let _ = std::fs::remove_dir_all(&cfg.base_dir);
}

#[test]
fn publish_many_delivers_all_records_in_order() {
    let cfg = cfg("delivers_all");
    cleanup(&cfg);
    let mut p = Publisher::create("bus", cfg.clone()).unwrap();
    let sub_cfg = cfg.clone();
    let handle = std::thread::spawn(move || {
        let mut sub = Subscriber::connect("bus", &sub_cfg, Duration::from_secs(5)).unwrap();
        let mut got = Vec::new();
        for _ in 0..10 {
            got.push(sub.receive().unwrap());
        }
        got
    });
    p.wait_for_subscribers(1, Duration::from_secs(5)).unwrap();
    let payloads: Vec<Vec<u8>> = (0..10u64).map(|i| i.to_le_bytes().to_vec()).collect();
    let n = p.publish_many(&payloads).unwrap();
    assert_eq!(n, 10, "all 10 must be published when ring has room");
    let got = handle.join().unwrap();
    for (i, msg) in got.iter().enumerate() {
        assert_eq!(msg, &(i as u64).to_le_bytes().to_vec());
    }
    cleanup(&cfg);
}

#[test]
fn publish_many_returns_partial_count_on_ring_full_under_error_backpressure() {
    let cfg = cfg("partial_on_full");
    cleanup(&cfg);
    // capacity 16 + a slow consumer claiming cursor 0 → publisher can
    // write 16 then is_full.
    let mut p = Publisher::create("bus", cfg.clone()).unwrap();
    let sub_cfg = cfg.clone();
    // Spawn a subscriber that DOESN'T drain — it just claims a cursor
    // so the publisher backpressures on capacity.
    let (sub_ready_tx, sub_ready_rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        let _sub = Subscriber::connect("bus", &sub_cfg, Duration::from_secs(5)).unwrap();
        sub_ready_tx.send(()).unwrap();
        // Hold the subscriber alive (and its cursor at 0) until the
        // test signals via drop.
        std::thread::sleep(Duration::from_secs(2));
    });
    p.wait_for_subscribers(1, Duration::from_secs(5)).unwrap();
    sub_ready_rx.recv().unwrap();

    let payloads: Vec<Vec<u8>> = (0..30u64).map(|i| i.to_le_bytes().to_vec()).collect();
    let n = p.publish_many(&payloads).unwrap();
    assert!(
        n > 0 && n < 30,
        "expected partial publish (0 < n < 30), got n={n}"
    );
    assert!(n <= 16, "capacity bounds the published count; got n={n}");
    handle.join().unwrap();
    cleanup(&cfg);
}

#[test]
fn publish_many_drop_oldest_always_publishes_all() {
    let cfg = BusConfig {
        capacity: 8,
        slot_size: 32,
        base_dir: std::env::temp_dir().join("mmbus_publish_many").join("drop"),
        backpressure: BackpressurePolicy::DropOldest,
        ..Default::default()
    };
    cleanup(&cfg);
    let mut p = Publisher::create("bus", cfg.clone()).unwrap();
    // No subscriber drains; DropOldest must still write every record
    // (older ones get overwritten in the ring).
    let payloads: Vec<Vec<u8>> = (0..50u64).map(|i| i.to_le_bytes().to_vec()).collect();
    let n = p.publish_many(&payloads).unwrap();
    assert_eq!(n, 50, "DropOldest must publish every record");
    cleanup(&cfg);
}

#[test]
fn publish_many_rejects_oversize_payload() {
    let cfg = cfg("oversize");
    cleanup(&cfg);
    let mut p = Publisher::create("bus", cfg.clone()).unwrap();
    let payloads: Vec<Vec<u8>> = vec![
        b"ok".to_vec(),
        vec![0u8; 64], // > slot_size (32)
        b"never-reached".to_vec(),
    ];
    let result = p.publish_many(&payloads);
    match result {
        Err(mmbus::Error::TooLarge { size: 64, max: 32 }) => (),
        other => panic!("expected TooLarge {{64, 32}}, got {other:?}"),
    }
    cleanup(&cfg);
}

#[test]
fn publish_many_wakeup_fires_once_per_subscriber_after_batch() {
    // Indirect check: a single subscriber thread calling recv() in a
    // loop must see every message in the burst even though the
    // publisher fires only one wakeup at the end.  The subscriber's
    // receive_into loop tries try_receive before waiting, so the
    // single wake kicks it once, then it drains the rest without
    // additional wakeups.
    let cfg = cfg("single_wake");
    cleanup(&cfg);
    let mut p = Publisher::create("bus", cfg.clone()).unwrap();
    let sub_cfg = cfg.clone();
    let handle = std::thread::spawn(move || {
        let mut sub = Subscriber::connect("bus", &sub_cfg, Duration::from_secs(5)).unwrap();
        let mut got = Vec::new();
        for _ in 0..8 {
            got.push(sub.receive().unwrap());
        }
        got
    });
    p.wait_for_subscribers(1, Duration::from_secs(5)).unwrap();
    let payloads: Vec<Vec<u8>> = (0..8u64).map(|i| i.to_le_bytes().to_vec()).collect();
    let n = p.publish_many(&payloads).unwrap();
    assert_eq!(n, 8);
    let got = handle.join().unwrap();
    assert_eq!(got.len(), 8);
    for (i, msg) in got.iter().enumerate() {
        assert_eq!(msg, &(i as u64).to_le_bytes().to_vec());
    }
    cleanup(&cfg);
}
