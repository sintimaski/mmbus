use mmbus::{BusConfig, Publisher, Subscriber};
use std::time::Duration;

fn make_cfg(name: &str) -> BusConfig {
    BusConfig {
        capacity: 32,
        slot_size: 128,
        base_dir: std::env::temp_dir().join("mmbus_tests").join(name),
    }
}

fn cleanup(cfg: &BusConfig) {
    let _ = std::fs::remove_dir_all(&cfg.base_dir);
}

#[test]
fn single_message_roundtrip() {
    let cfg = make_cfg("single");
    cleanup(&cfg);

    let cfg_sub = cfg.clone();
    let consumer = std::thread::spawn(move || {
        let mut sub = Subscriber::connect("bus", &cfg_sub, Duration::from_secs(5)).unwrap();
        sub.receive().unwrap()
    });

    let mut pub_ = Publisher::create("bus", cfg.clone()).unwrap();
    // Block until the subscriber's socket connection is accepted.
    pub_.wait_for_subscribers(1, Duration::from_secs(5)).unwrap();
    pub_.publish(b"hello world").unwrap();

    let received = consumer.join().unwrap();
    assert_eq!(received, b"hello world");
    cleanup(&cfg);
}

#[test]
fn multi_message_ordered() {
    let cfg = make_cfg("multi");
    cleanup(&cfg);
    let n = 200usize;

    let cfg_sub = cfg.clone();
    let consumer = std::thread::spawn(move || {
        let mut sub = Subscriber::connect("bus", &cfg_sub, Duration::from_secs(5)).unwrap();
        (0..n).map(|_| sub.receive().unwrap()).collect::<Vec<_>>()
    });

    let mut pub_ = Publisher::create("bus", cfg.clone()).unwrap();
    pub_.wait_for_subscribers(1, Duration::from_secs(5)).unwrap();
    for i in 0..n {
        loop {
            match pub_.publish(format!("msg-{i:04}").as_bytes()) {
                Ok(()) => break,
                Err(mmbus::Error::Full) => std::hint::spin_loop(),
                Err(e) => panic!("{e}"),
            }
        }
    }

    let received = consumer.join().unwrap();
    assert_eq!(received.len(), n);
    for (i, msg) in received.iter().enumerate() {
        assert_eq!(msg.as_slice(), format!("msg-{i:04}").as_bytes(), "message {i}");
    }
    cleanup(&cfg);
}

#[test]
fn empty_message() {
    let cfg = make_cfg("empty");
    cleanup(&cfg);

    let cfg_sub = cfg.clone();
    let consumer = std::thread::spawn(move || {
        let mut sub = Subscriber::connect("bus", &cfg_sub, Duration::from_secs(5)).unwrap();
        sub.receive().unwrap()
    });

    let mut pub_ = Publisher::create("bus", cfg.clone()).unwrap();
    pub_.wait_for_subscribers(1, Duration::from_secs(5)).unwrap();
    pub_.publish(b"").unwrap();

    assert_eq!(consumer.join().unwrap(), b"");
    cleanup(&cfg);
}

#[test]
fn message_at_max_slot_size() {
    let cfg = make_cfg("maxslot");
    cleanup(&cfg);

    let max = cfg.slot_size as usize;
    let payload: Vec<u8> = (0..max).map(|i| (i % 251) as u8).collect();
    let expected = payload.clone();

    let cfg_sub = cfg.clone();
    let consumer = std::thread::spawn(move || {
        let mut sub = Subscriber::connect("bus", &cfg_sub, Duration::from_secs(5)).unwrap();
        sub.receive().unwrap()
    });

    let mut pub_ = Publisher::create("bus", cfg.clone()).unwrap();
    pub_.wait_for_subscribers(1, Duration::from_secs(5)).unwrap();
    pub_.publish(&payload).unwrap();

    assert_eq!(consumer.join().unwrap(), expected);
    cleanup(&cfg);
}

#[test]
fn ring_full_returns_error() {
    let cfg = BusConfig {
        capacity: 4,
        slot_size: 8,
        base_dir: std::env::temp_dir().join("mmbus_tests").join("full"),
    };
    cleanup(&cfg);

    // No subscriber — ring fills up after `capacity` publishes.
    let mut pub_ = Publisher::create("bus", cfg.clone()).unwrap();
    let mut fulls = 0;
    for _ in 0..16 {
        match pub_.publish(b"data") {
            Ok(()) => {}
            Err(mmbus::Error::Full) => fulls += 1,
            Err(e) => panic!("unexpected: {e}"),
        }
    }
    assert!(fulls > 0, "expected Full errors");
    cleanup(&cfg);
}

#[test]
fn wrap_around_correctness() {
    // Publish more messages than the ring capacity to exercise wrap-around.
    let cfg = make_cfg("wrap");
    cleanup(&cfg);
    let n = 100usize; // > capacity (32)

    let cfg_sub = cfg.clone();
    let consumer = std::thread::spawn(move || {
        let mut sub = Subscriber::connect("bus", &cfg_sub, Duration::from_secs(5)).unwrap();
        (0..n).map(|_| sub.receive().unwrap()).collect::<Vec<_>>()
    });

    let mut pub_ = Publisher::create("bus", cfg.clone()).unwrap();
    pub_.wait_for_subscribers(1, Duration::from_secs(5)).unwrap();
    for i in 0..n {
        // Retry on Full — consumer is concurrently draining the ring.
        loop {
            match pub_.publish(format!("{i:03}").as_bytes()) {
                Ok(()) => break,
                Err(mmbus::Error::Full) => std::hint::spin_loop(),
                Err(e) => panic!("{e}"),
            }
        }
    }

    let received = consumer.join().unwrap();
    assert_eq!(received.len(), n);
    for (i, msg) in received.iter().enumerate() {
        assert_eq!(msg.as_slice(), format!("{i:03}").as_bytes(), "slot {i}");
    }
    cleanup(&cfg);
}
