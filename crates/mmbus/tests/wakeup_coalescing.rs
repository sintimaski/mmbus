//! M2 wakeup-coalescing tests (wire v5).
//!
//! The publisher fires a wakeup syscall only for subscribers whose
//! per-cursor `needs_wakeup` flag is set (an eventcount handshake).  These
//! tests assert the two properties that matter:
//!
//!   1. **No missed wakeup.**  Under a mix of bursts and idle gaps the
//!      subscriber must receive every message, in order, without hanging.
//!      A missed wakeup would strand the subscriber asleep — caught here by
//!      a per-message `recv_timeout` on the collecting side.
//!   2. **Coalescing actually happens.**  When the publisher outpaces the
//!      subscriber, `wakeups_sent_total` is strictly below `published_total`.

use mmbus::{Bus, BusConfig, Error};
use std::sync::mpsc;
use std::time::Duration;

fn cfg(label: &str, capacity: u32) -> BusConfig {
    BusConfig {
        capacity,
        slot_size: 64,
        base_dir: std::env::temp_dir().join("mmbus_tests").join(label),
        ..Default::default()
    }
}

fn cleanup(cfg: &BusConfig) {
    let _ = std::fs::remove_dir_all(&cfg.base_dir);
}

#[test]
fn no_missed_wakeup_under_mixed_burst_and_idle() {
    let cfg = cfg("wake_mixed", 64);
    cleanup(&cfg);
    let n: u64 = 5_000;

    let (tx, rx) = mpsc::channel();
    let sub_cfg = cfg.clone();
    let consumer = std::thread::spawn(move || {
        let bus = Bus::with_config("app", sub_cfg);
        let mut sub = bus.subscribe("ch").unwrap();
        let mut buf = Vec::new();
        for _ in 0..n {
            // Blocking recv: the eventcount handshake must wake us for every
            // message even when the publisher coalesces.
            sub.recv_into(&mut buf).unwrap();
            tx.send(u64::from_le_bytes(buf.as_slice().try_into().unwrap()))
                .unwrap();
        }
    });

    let mut bus = Bus::with_config("app", cfg.clone());
    bus.wait_for_subscribers("ch", 1, Duration::from_secs(5))
        .unwrap();
    for i in 0..n {
        let payload = i.to_le_bytes();
        loop {
            match bus.publish("ch", &payload) {
                Ok(()) => break,
                Err(Error::Full) => std::hint::spin_loop(),
                Err(e) => panic!("{e}"),
            }
        }
        // Force the subscriber to drain-to-empty and re-arm on ~1/8 of
        // publishes, exercising the empty -> set-flag -> sleep -> wake path
        // (the missed-wakeup-prone window) rather than only the burst path.
        if i % 8 == 0 {
            std::thread::sleep(Duration::from_micros(30));
        }
    }

    // A missed wakeup strands the consumer asleep -> recv_timeout fires.
    for i in 0..n {
        let got = rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap_or_else(|e| panic!("missed wakeup / hang at message {i}: {e}"));
        assert_eq!(got, i, "out-of-order or lost message");
    }
    consumer.join().unwrap();
    cleanup(&cfg);
}

#[test]
fn coalescing_reduces_wakeups_when_publisher_outpaces_subscriber() {
    // Large ring + Error backpressure (n < capacity so the publisher never
    // blocks).  The publisher bursts all messages while the slower
    // subscriber drains in bulk — most publishes land while the subscriber's
    // flag is clear (actively draining), so their wakeups are coalesced.
    let cfg = cfg("wake_coalesce", 8192);
    cleanup(&cfg);
    let n: u64 = 4_000;

    let (tx, rx) = mpsc::channel();
    let sub_cfg = cfg.clone();
    let consumer = std::thread::spawn(move || {
        let bus = Bus::with_config("app", sub_cfg);
        let mut sub = bus.subscribe("ch").unwrap();
        let mut buf = Vec::new();
        for _ in 0..n {
            sub.recv_into(&mut buf).unwrap();
            // Per-message work (the channel send) keeps the subscriber
            // behind the tight publish loop.
            tx.send(()).unwrap();
        }
    });

    let mut bus = Bus::with_config("app", cfg.clone());
    bus.wait_for_subscribers("ch", 1, Duration::from_secs(5))
        .unwrap();
    let payload = [0u8; 8];
    for _ in 0..n {
        bus.publish("ch", &payload).unwrap();
    }

    for _ in 0..n {
        rx.recv_timeout(Duration::from_secs(5))
            .expect("missed wakeup / hang");
    }
    consumer.join().unwrap();

    let s = bus.stats("ch").expect("publisher exists");
    assert_eq!(s.published_total, n, "all messages published");
    assert!(
        s.wakeups_sent_total < s.published_total,
        "coalescing should fire fewer wakeups than publishes: {} wakeups vs {} publishes",
        s.wakeups_sent_total,
        s.published_total,
    );
    assert!(
        s.wakeups_sent_total > 0,
        "subscriber must have been woken at least once"
    );
    cleanup(&cfg);
}
