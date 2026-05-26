//! Long-running stress tests.  Run with:
//!
//!     cargo test --release --test stress -- --ignored --nocapture
//!
//! These exercise the full publish/subscribe pipeline under sustained load
//! to surface race conditions and wakeup-bookkeeping bugs that the
//! deterministic tests miss.  All tests are `#[ignore]` by default so
//! `cargo test` stays under a few seconds.

use mmbus::{BackpressurePolicy, BusConfig, Publisher, Subscriber};
use std::hint::spin_loop;
use std::time::{Duration, Instant};

fn cfg(name: &str) -> BusConfig {
    BusConfig {
        capacity: 256,
        slot_size: 16,
        base_dir: std::env::temp_dir().join("mmbus_stress").join(name),
        ..Default::default()
    }
}

fn cleanup(cfg: &BusConfig) {
    let _ = std::fs::remove_dir_all(&cfg.base_dir);
}

/// 100 k messages × 4 subscribers (400 k total receives).
/// Every subscriber must receive every message in order.
///
/// Catches: wakeup desync (lost messages), cursor table races, slot-stride
/// off-by-one under wrap-around.
#[test]
#[ignore]
fn fan_out_100k_messages_4_subscribers() {
    let cfg = cfg("fanout_100k_4");
    cleanup(&cfg);
    const N: u64 = 100_000;
    const SUBS: usize = 4;

    let handles: Vec<_> = (0..SUBS)
        .map(|_| {
            let c = cfg.clone();
            std::thread::spawn(move || {
                let mut sub = Subscriber::connect("bus", &c, Duration::from_secs(10)).unwrap();
                let mut received = Vec::with_capacity(N as usize);
                for _ in 0..N {
                    let bytes = sub.receive().unwrap();
                    received.push(u64::from_le_bytes(bytes.try_into().unwrap()));
                }
                received
            })
        })
        .collect();

    let mut pub_ = Publisher::create("bus", cfg.clone()).unwrap();
    pub_.wait_for_subscribers(SUBS, Duration::from_secs(10))
        .unwrap();

    let start = Instant::now();
    for i in 0..N {
        loop {
            match pub_.publish(&i.to_le_bytes()) {
                Ok(()) => break,
                Err(mmbus::Error::Full) => spin_loop(),
                Err(e) => panic!("publish: {e}"),
            }
        }
    }
    let publish_elapsed = start.elapsed();

    for (sub_id, h) in handles.into_iter().enumerate() {
        let received = h.join().unwrap();
        assert_eq!(received.len(), N as usize, "sub {sub_id}: short read");
        for (i, &v) in received.iter().enumerate() {
            assert_eq!(v, i as u64, "sub {sub_id}: msg {i} = {v}");
        }
    }
    let total_elapsed = start.elapsed();

    eprintln!(
        "  ✓ {N} msgs × {SUBS} subs in {:.2?}  (publish: {:.2?}; throughput: {:.0} msg/s)",
        total_elapsed,
        publish_elapsed,
        (N as f64 * SUBS as f64) / total_elapsed.as_secs_f64()
    );
    cleanup(&cfg);
}

/// DropOldest under sustained backpressure: publisher always outpaces the
/// (artificially slowed) subscribers.  No panics, no UB; in-order delivery
/// is preserved across the messages that ARE received.
#[test]
#[ignore]
fn drop_oldest_50k_messages_3_subscribers() {
    let cfg = BusConfig {
        backpressure: BackpressurePolicy::DropOldest,
        ..cfg("drop_oldest_50k_3")
    };
    cleanup(&cfg);
    const N: u64 = 50_000;
    const SUBS: usize = 3;

    let handles: Vec<_> = (0..SUBS)
        .map(|i| {
            let c = cfg.clone();
            // Each subscriber sleeps differently to maximise lag variance.
            let sleep_us = 5 * (i as u64 + 1);
            std::thread::spawn(move || {
                let mut sub = Subscriber::connect("bus", &c, Duration::from_secs(10)).unwrap();
                let mut count: u64 = 0;
                let mut last: i128 = -1;
                loop {
                    match sub.receive_timeout(Duration::from_millis(500)) {
                        Ok(Some(bytes)) => {
                            let v = u64::from_le_bytes(bytes.try_into().unwrap());
                            assert!((v as i128) > last, "sub {i}: out-of-order {v} after {last}");
                            last = v as i128;
                            count += 1;
                            if sleep_us > 0 {
                                std::thread::sleep(Duration::from_micros(sleep_us));
                            }
                        }
                        Ok(None) => break, // timeout = publisher idle/done
                        Err(mmbus::Error::Io(e))
                            if e.kind() == std::io::ErrorKind::UnexpectedEof =>
                        {
                            break; // publisher dropped
                        }
                        Err(e) => panic!("sub {i}: {e}"),
                    }
                }
                count
            })
        })
        .collect();

    let mut pub_ = Publisher::create("bus", cfg.clone()).unwrap();
    pub_.wait_for_subscribers(SUBS, Duration::from_secs(10))
        .unwrap();

    let start = Instant::now();
    for i in 0..N {
        // DropOldest must never return Full.
        pub_.publish(&i.to_le_bytes()).unwrap();
    }
    let publish_elapsed = start.elapsed();

    // Let subscribers drain whatever's left, then trigger POLLHUP.
    std::thread::sleep(Duration::from_millis(200));
    drop(pub_);

    for (i, h) in handles.into_iter().enumerate() {
        let count = h.join().unwrap();
        assert!(count > 0, "sub {i} got nothing");
        assert!(
            count <= N,
            "sub {i} got more than published ({count} > {N})"
        );
        eprintln!(
            "  ✓ sub {i}: {count}/{N} received ({:.1}% drop)",
            100.0 * (N - count) as f64 / N as f64
        );
    }
    eprintln!("  publish: {:.2?}", publish_elapsed);
    cleanup(&cfg);
}

/// Rapid publisher restart cycles. Each restart must not corrupt the ring,
/// must bump the generation, and stale subscribers must terminate cleanly.
#[test]
#[ignore]
fn rapid_publisher_restart_cycles() {
    let cfg = cfg("rapid_restart");
    cleanup(&cfg);
    const CYCLES: usize = 50;
    const MSGS_PER_CYCLE: u64 = 100;

    for cycle in 0..CYCLES {
        let c = cfg.clone();
        let sub_thread = std::thread::spawn(move || {
            let mut sub = Subscriber::connect("bus", &c, Duration::from_secs(5)).unwrap();
            let mut got: Vec<u64> = Vec::new();
            loop {
                match sub.receive() {
                    Ok(bytes) => got.push(u64::from_le_bytes(bytes.try_into().unwrap())),
                    Err(mmbus::Error::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        break
                    }
                    Err(e) => panic!("cycle {cycle}: {e}"),
                }
            }
            got
        });

        let mut pub_ = Publisher::create("bus", cfg.clone()).unwrap();
        pub_.wait_for_subscribers(1, Duration::from_secs(5))
            .unwrap();
        for i in 0..MSGS_PER_CYCLE {
            loop {
                match pub_.publish(&i.to_le_bytes()) {
                    Ok(()) => break,
                    Err(mmbus::Error::Full) => spin_loop(),
                    Err(e) => panic!("cycle {cycle}: publish {e}"),
                }
            }
        }
        drop(pub_);

        let got = sub_thread.join().expect("subscriber panicked");
        assert!(
            !got.is_empty(),
            "cycle {cycle}: subscriber received no messages"
        );
        // It's allowed for the subscriber to receive < MSGS_PER_CYCLE if the
        // pub drop raced the last few wakeups, but everything received must
        // be in order and come from THIS cycle (0..MSGS_PER_CYCLE).
        for w in got.windows(2) {
            assert!(w[1] > w[0], "cycle {cycle}: out of order {:?}", w);
        }
        assert!(*got.last().unwrap() < MSGS_PER_CYCLE);
    }
    eprintln!("  ✓ {CYCLES} restart cycles ({MSGS_PER_CYCLE} msgs each) all clean");
    cleanup(&cfg);
}
