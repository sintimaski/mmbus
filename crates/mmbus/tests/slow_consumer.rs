//! `Bus::slow_subscribers` reports laggard cursors above a threshold.

use mmbus::{Bus, BusConfig};
use std::time::Duration;

fn cfg(name: &str) -> BusConfig {
    BusConfig {
        capacity: 64,
        slot_size: 16,
        base_dir: std::env::temp_dir().join("mmslow").join(name),
        ..Default::default()
    }
}

fn cleanup(cfg: &BusConfig) {
    let _ = std::fs::remove_dir_all(&cfg.base_dir);
}

/// No publisher → empty Vec (not an error).
#[test]
fn no_publisher_returns_empty() {
    let cfg = cfg("none");
    cleanup(&cfg);
    let bus = Bus::with_config("bus", cfg.clone());
    assert!(bus.slow_subscribers("ch", 0).is_empty());
}

/// No subscribers → empty Vec.
#[test]
fn no_subscribers_returns_empty() {
    let cfg = cfg("no_subs");
    cleanup(&cfg);
    let mut bus = Bus::with_config("bus", cfg.clone());
    bus.publish("ch", b"hello").unwrap();
    assert!(bus.slow_subscribers("ch", 0).is_empty());
    cleanup(&cfg);
}

/// All caught-up subscribers → empty Vec.
#[test]
fn caught_up_subscribers_omitted() {
    let cfg = cfg("caught_up");
    cleanup(&cfg);
    let mut bus = Bus::with_config("bus", cfg.clone());

    let sub_cfg = cfg.clone();
    let t = std::thread::spawn(move || {
        let sub_bus = Bus::with_config("bus", sub_cfg);
        let mut sub = sub_bus
            .subscribe_timeout("ch", Duration::from_secs(5))
            .unwrap();
        let mut got = Vec::new();
        for _ in 0..3 {
            got.push(sub.recv().unwrap());
        }
        got
    });

    bus.wait_for_subscribers("ch", 1, Duration::from_secs(5))
        .unwrap();
    for i in 0..3 {
        bus.publish("ch", &[i]).unwrap();
    }
    let _got = t.join().unwrap();
    // Give the subscriber a moment to advance its cursor.
    std::thread::sleep(Duration::from_millis(50));

    assert!(bus.slow_subscribers("ch", 1).is_empty());
    cleanup(&cfg);
}

/// One slow subscriber (artificially delayed read) → reported with lag.
#[test]
fn slow_subscriber_reported_with_lag() {
    let cfg = cfg("slow");
    cleanup(&cfg);
    let mut bus = Bus::with_config("bus", cfg.clone());

    // Subscriber connects but never reads.
    let sub_cfg = cfg.clone();
    let (block_tx, block_rx) = std::sync::mpsc::channel::<()>();
    let t = std::thread::spawn(move || {
        let sub_bus = Bus::with_config("bus", sub_cfg);
        let _sub = sub_bus
            .subscribe_timeout("ch", Duration::from_secs(5))
            .unwrap();
        // Hold the cursor without reading until released.
        block_rx.recv().unwrap();
    });

    bus.wait_for_subscribers("ch", 1, Duration::from_secs(5))
        .unwrap();
    for i in 0..10u8 {
        bus.publish("ch", &[i]).unwrap();
    }

    // Threshold 0 → any active subscriber with lag > 0.
    let lagging = bus.slow_subscribers("ch", 1);
    assert_eq!(lagging.len(), 1, "exactly one slow sub  got {lagging:?}");
    let (idx, lag) = lagging[0];
    assert_eq!(lag, 10, "10 unread messages");
    assert!(idx < 16, "idx within max_subscribers");

    // Threshold above current lag → empty
    assert!(bus.slow_subscribers("ch", 100).is_empty());

    block_tx.send(()).unwrap();
    t.join().unwrap();
    cleanup(&cfg);
}
