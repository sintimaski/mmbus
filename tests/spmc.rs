/// SPMC (single-producer, multi-consumer) correctness tests.
///
/// These tests exercise the SPMC-specific invariants:
///   - Fan-out: every active subscriber gets every message independently.
///   - DropOldest: publisher never blocks; slow subscribers skip messages.
///   - Cursor lifecycle: drop releases the slot; slots can be reclaimed.
///   - Capacity: TooManySubscribers is returned when the cursor table is full.
///   - Lag: subscribers can query how far behind they are.
use mmbus::{BackpressurePolicy, BusConfig, Error, Publisher, Subscriber};
use std::hint::spin_loop;
use std::time::Duration;

fn cfg(name: &str) -> BusConfig {
    BusConfig {
        capacity: 64,
        slot_size: 32,
        base_dir: std::env::temp_dir().join("mmbus_tests_spmc").join(name),
        ..Default::default()
    }
}

fn cleanup(cfg: &BusConfig) {
    let _ = std::fs::remove_dir_all(&cfg.base_dir);
}

// ── Fan-out ───────────────────────────────────────────────────────────────────

/// Two subscribers each receive every message independently and in order.
#[test]
fn fan_out_two_subscribers() {
    let cfg = cfg("fanout2");
    cleanup(&cfg);
    let n = 200usize;

    let cfg1 = cfg.clone();
    let h1 = std::thread::spawn(move || {
        let mut sub = Subscriber::connect("bus", &cfg1, Duration::from_secs(5)).unwrap();
        (0..n).map(|_| sub.receive().unwrap()).collect::<Vec<_>>()
    });

    let cfg2 = cfg.clone();
    let h2 = std::thread::spawn(move || {
        let mut sub = Subscriber::connect("bus", &cfg2, Duration::from_secs(5)).unwrap();
        (0..n).map(|_| sub.receive().unwrap()).collect::<Vec<_>>()
    });

    let mut pub_ = Publisher::create("bus", cfg.clone()).unwrap();
    pub_.wait_for_subscribers(2, Duration::from_secs(5)).unwrap();

    for i in 0..n {
        loop {
            match pub_.publish(format!("{i:04}").as_bytes()) {
                Ok(()) => break,
                Err(Error::Full) => spin_loop(),
                Err(e) => panic!("{e}"),
            }
        }
    }

    for (sub_id, received) in [(1, h1.join().unwrap()), (2, h2.join().unwrap())] {
        assert_eq!(received.len(), n, "subscriber {sub_id} missing messages");
        for (i, msg) in received.iter().enumerate() {
            assert_eq!(
                msg.as_slice(),
                format!("{i:04}").as_bytes(),
                "subscriber {sub_id} message {i} corrupted"
            );
        }
    }
    cleanup(&cfg);
}

/// Three subscribers each receive 500 messages under concurrent load.
/// Stress-tests the atomic cursor table under contention.
#[test]
fn fan_out_three_subscribers_stress() {
    let cfg = BusConfig {
        capacity: 256,
        slot_size: 16,
        base_dir: std::env::temp_dir().join("mmbus_tests_spmc").join("fanout3_stress"),
        ..Default::default()
    };
    cleanup(&cfg);
    let n = 500usize;

    let handles: Vec<_> = (0..3)
        .map(|_| {
            let c = cfg.clone();
            std::thread::spawn(move || {
                let mut sub = Subscriber::connect("bus", &c, Duration::from_secs(5)).unwrap();
                (0..n)
                    .map(|_| {
                        let bytes = sub.receive().unwrap();
                        u64::from_le_bytes(bytes.try_into().expect("wrong msg length"))
                    })
                    .collect::<Vec<u64>>()
            })
        })
        .collect();

    let mut pub_ = Publisher::create("bus", cfg.clone()).unwrap();
    pub_.wait_for_subscribers(3, Duration::from_secs(5)).unwrap();

    for i in 0..n as u64 {
        loop {
            match pub_.publish(&i.to_le_bytes()) {
                Ok(()) => break,
                Err(Error::Full) => spin_loop(),
                Err(e) => panic!("{e}"),
            }
        }
    }

    for (idx, handle) in handles.into_iter().enumerate() {
        let received = handle.join().unwrap();
        assert_eq!(received.len(), n, "subscriber {idx} got wrong count");
        for (i, &val) in received.iter().enumerate() {
            assert_eq!(val, i as u64, "subscriber {idx}: position {i} got {val}");
        }
    }
    cleanup(&cfg);
}

// ── DropOldest ────────────────────────────────────────────────────────────────

/// With DropOldest policy, publish never returns Error::Full, even when the
/// subscriber is not reading. The subscriber may skip messages but receives
/// valid data at whatever position it polls.
#[test]
fn drop_oldest_never_returns_full() {
    let cfg = BusConfig {
        capacity: 4,
        slot_size: 16,
        backpressure: BackpressurePolicy::DropOldest,
        base_dir: std::env::temp_dir().join("mmbus_tests_spmc").join("dropoldest"),
        ..Default::default()
    };
    cleanup(&cfg);
    let n = 200usize;

    // Subscriber is present but never reads — would block producer with Error policy.
    let (signal_tx, signal_rx) = std::sync::mpsc::channel::<()>();
    let cfg_sub = cfg.clone();
    let sub_thread = std::thread::spawn(move || {
        let _sub = Subscriber::connect("bus", &cfg_sub, Duration::from_secs(5)).unwrap();
        let _ = signal_rx.recv();
    });

    let mut pub_ = Publisher::create("bus", cfg.clone()).unwrap();
    pub_.wait_for_subscribers(1, Duration::from_secs(5)).unwrap();

    for i in 0..n as u64 {
        match pub_.publish(&i.to_le_bytes()) {
            Ok(()) => {}
            Err(Error::Full) => panic!("DropOldest must never return Full"),
            Err(e) => panic!("unexpected: {e}"),
        }
    }

    signal_tx.send(()).unwrap();
    sub_thread.join().unwrap();
    cleanup(&cfg);
}

/// After force-advance, the subscriber's next receive returns valid data at
/// the skipped-to position (not corrupted payload).
#[test]
fn drop_oldest_subscriber_reads_valid_data_after_skip() {
    let cfg = BusConfig {
        capacity: 4,
        slot_size: 16,
        backpressure: BackpressurePolicy::DropOldest,
        base_dir: std::env::temp_dir().join("mmbus_tests_spmc").join("dropoldest_valid"),
        ..Default::default()
    };
    cleanup(&cfg);

    let (ready_tx, ready_rx) = std::sync::mpsc::channel::<()>();
    let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
    let cfg_sub = cfg.clone();
    let sub_thread = std::thread::spawn(move || {
        let mut sub = Subscriber::connect("bus", &cfg_sub, Duration::from_secs(5)).unwrap();
        ready_tx.send(()).unwrap(); // signal we're connected

        // Wait for the producer to overfill the ring, then read once.
        go_rx.recv().unwrap();
        let msg = sub.receive().unwrap();
        assert_eq!(msg.len(), 8, "message length must be 8 bytes");
        u64::from_le_bytes(msg.try_into().unwrap())
    });

    let mut pub_ = Publisher::create("bus", cfg.clone()).unwrap();
    pub_.wait_for_subscribers(1, Duration::from_secs(5)).unwrap();

    // Publish enough to force-advance the subscriber's cursor past slot 0.
    ready_rx.recv().unwrap();
    for i in 0..20u64 {
        pub_.publish(&i.to_le_bytes()).unwrap(); // DropOldest, never fails
    }
    go_tx.send(()).unwrap();

    let val = sub_thread.join().unwrap();
    // The subscriber read some value in [0, 20); exact slot depends on timing.
    assert!(val < 20, "received value {val} out of published range");
    cleanup(&cfg);
}

// ── Cursor lifecycle ──────────────────────────────────────────────────────────

/// Dropping a subscriber releases its cursor slot so the same slot can be
/// reclaimed by a new subscriber.
#[test]
fn subscriber_drop_releases_cursor_slot() {
    let cfg = BusConfig {
        capacity: 16,
        slot_size: 8,
        max_subscribers: 1,
        base_dir: std::env::temp_dir().join("mmbus_tests_spmc").join("cursor_release"),
        ..Default::default()
    };
    cleanup(&cfg);

    let mut pub_ = Publisher::create("bus", cfg.clone()).unwrap();

    // First subscriber connects and then drops.
    {
        let sub = Subscriber::connect("bus", &cfg, Duration::from_secs(5)).unwrap();
        pub_.wait_for_subscribers(1, Duration::from_secs(5)).unwrap();
        drop(sub); // releases cursor slot 0
    }

    // A second subscriber should be able to claim the freed slot.
    let _sub2 = Subscriber::connect("bus", &cfg, Duration::from_secs(5))
        .expect("cursor slot should be free after first subscriber dropped");
    cleanup(&cfg);
}

// ── Capacity ─────────────────────────────────────────────────────────────────

/// Connecting more subscribers than max_subscribers returns TooManySubscribers.
#[test]
fn too_many_subscribers_returns_error() {
    let cfg = BusConfig {
        capacity: 16,
        slot_size: 8,
        max_subscribers: 2,
        base_dir: std::env::temp_dir().join("mmbus_tests_spmc").join("too_many"),
        ..Default::default()
    };
    cleanup(&cfg);

    let (done_tx, _done_rx) = std::sync::mpsc::channel::<()>();

    let cfg1 = cfg.clone();
    let done_rx1 = done_tx.clone();
    let h1 = std::thread::spawn(move || {
        let sub = Subscriber::connect("bus", &cfg1, Duration::from_secs(5)).unwrap();
        let _ = done_rx1; // keep alive until done_tx is dropped
        sub
    });
    let cfg2 = cfg.clone();
    let h2 = std::thread::spawn(move || {
        Subscriber::connect("bus", &cfg2, Duration::from_secs(5)).unwrap()
    });

    let mut pub_ = Publisher::create("bus", cfg.clone()).unwrap();
    // Wait until both slots are claimed.
    pub_.wait_for_subscribers(2, Duration::from_secs(5)).unwrap();

    // Third subscriber must fail immediately (no retry for TooManySubscribers).
    match Subscriber::connect("bus", &cfg, Duration::from_millis(100)) {
        Err(Error::TooManySubscribers(n)) => assert_eq!(n, 2),
        Err(e) => panic!("expected TooManySubscribers(2), got Err({e})"),
        Ok(_) => panic!("expected TooManySubscribers(2), got Ok"),
    }

    drop(done_tx); // signal subscribers to exit
    drop(h1.join().unwrap());
    drop(h2.join().unwrap());
    cleanup(&cfg);
}

// ── Lag ───────────────────────────────────────────────────────────────────────

/// `Subscriber::lag()` returns the number of unconsumed messages.
#[test]
fn subscriber_lag_tracking() {
    let cfg = cfg("lag");
    cleanup(&cfg);
    let n = 10usize;

    let (connected_tx, connected_rx) = std::sync::mpsc::channel::<()>();
    let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
    let cfg_sub = cfg.clone();
    let sub_thread = std::thread::spawn(move || {
        let mut sub = Subscriber::connect("bus", &cfg_sub, Duration::from_secs(5)).unwrap();
        connected_tx.send(()).unwrap();
        go_rx.recv().unwrap(); // wait until producer has published all messages

        let lag_before = sub.lag();
        // Drain all messages.
        for _ in 0..n {
            sub.receive().unwrap();
        }
        let lag_after = sub.lag();
        (lag_before, lag_after)
    });

    let mut pub_ = Publisher::create("bus", cfg.clone()).unwrap();
    connected_rx.recv().unwrap(); // subscriber is connected and cursor claimed
    pub_.wait_for_subscribers(1, Duration::from_secs(5)).unwrap();

    for i in 0..n {
        loop {
            match pub_.publish(format!("{i}").as_bytes()) {
                Ok(()) => break,
                Err(Error::Full) => spin_loop(),
                Err(e) => panic!("{e}"),
            }
        }
    }
    go_tx.send(()).unwrap();

    let (lag_before, lag_after) = sub_thread.join().unwrap();
    assert_eq!(lag_before, n as u64, "lag before drain should be {n}");
    assert_eq!(lag_after, 0, "lag after drain should be 0");
    cleanup(&cfg);
}

// ── Publisher stats ───────────────────────────────────────────────────────────

// ── Producer lock ─────────────────────────────────────────────────────────────

/// Trying to create a second publisher for the same topic while one is alive
/// returns `AlreadyPublishing`. The lock is released when the first publisher
/// is dropped, allowing a new publisher to take over.
#[test]
fn duplicate_publisher_returns_error() {
    let cfg = cfg("dup_pub");
    cleanup(&cfg);

    let pub1 = Publisher::create("bus", cfg.clone()).unwrap();

    match Publisher::create("bus", cfg.clone()) {
        Err(Error::AlreadyPublishing(_)) => {}
        Err(e) => panic!("expected AlreadyPublishing, got Err({e})"),
        Ok(_) => panic!("expected AlreadyPublishing, got Ok"),
    }

    // After the first publisher is dropped the slot is free.
    drop(pub1);
    Publisher::create("bus", cfg.clone()).expect("should succeed after first publisher dropped");

    cleanup(&cfg);
}

// ── Publisher stats ───────────────────────────────────────────────────────────

/// `Publisher::stats()` reports correct tail and active subscriber count.
#[test]
fn publisher_stats() {
    let cfg = cfg("pub_stats");
    cleanup(&cfg);

    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    let cfg_sub = cfg.clone();
    let sub_thread = std::thread::spawn(move || {
        let _sub = Subscriber::connect("bus", &cfg_sub, Duration::from_secs(5)).unwrap();
        let _ = done_rx.recv();
    });

    let mut pub_ = Publisher::create("bus", cfg.clone()).unwrap();
    pub_.wait_for_subscribers(1, Duration::from_secs(5)).unwrap();

    // Publish a few messages and check stats.
    for _ in 0..5 {
        pub_.publish(b"x").unwrap();
    }

    let stats = pub_.stats();
    assert_eq!(stats.ring.tail, 5, "tail should be 5 after 5 publishes");
    assert_eq!(stats.ring.active_subscribers, 1);
    assert_eq!(stats.ring.lags.len(), 1);
    // Lag: subscriber hasn't consumed anything.
    assert_eq!(stats.ring.lags[0], 5);

    done_tx.send(()).unwrap();
    sub_thread.join().unwrap();
    cleanup(&cfg);
}
