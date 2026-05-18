//! Buffer-reuse recv APIs (Subscriber::receive_into,
//! Subscription::recv_into + try_recv_into + recv_timeout_into).
//!
//! These back the Python recv hot path: PySubscription holds one
//! `Vec<u8>` across calls and reuses it for every recv, saving a
//! per-call allocation.

use mmbus::{BusConfig, Publisher, Subscriber};
use std::time::Duration;

fn cfg(name: &str) -> BusConfig {
    BusConfig {
        capacity: 16,
        slot_size: 64,
        base_dir: std::env::temp_dir().join("mmbus_recv_into").join(name),
        ..Default::default()
    }
}

fn cleanup(cfg: &BusConfig) {
    let _ = std::fs::remove_dir_all(&cfg.base_dir);
}

#[test]
fn receive_into_reuses_buffer_capacity() {
    let cfg = cfg("reuse_capacity");
    cleanup(&cfg);
    let mut p = Publisher::create("bus", cfg.clone()).unwrap();
    let sub_cfg = cfg.clone();
    let handle = std::thread::spawn(move || {
        let mut sub = Subscriber::connect("bus", &sub_cfg, Duration::from_secs(5)).unwrap();
        let mut buf = Vec::with_capacity(4);
        let starting_cap = buf.capacity();

        // First recv: payload longer than capacity → buf grows.
        sub.receive_into(&mut buf).unwrap();
        assert_eq!(&buf[..], b"hello-world-foobar");
        let grown_cap = buf.capacity();
        assert!(grown_cap > starting_cap, "buffer must have grown for the first payload");

        // Subsequent recvs: shorter payloads → buf capacity does NOT shrink.
        sub.receive_into(&mut buf).unwrap();
        assert_eq!(&buf[..], b"hi");
        assert_eq!(buf.capacity(), grown_cap, "capacity must not shrink between recvs");

        // A third recv with the same length: still the same allocation.
        sub.receive_into(&mut buf).unwrap();
        assert_eq!(&buf[..], b"ok");
        assert_eq!(buf.capacity(), grown_cap, "second short recv must reuse the buffer");
    });
    p.wait_for_subscribers(1, Duration::from_secs(5)).unwrap();
    p.publish(b"hello-world-foobar").unwrap();
    p.publish(b"hi").unwrap();
    p.publish(b"ok").unwrap();
    handle.join().unwrap();
    cleanup(&cfg);
}

#[test]
fn receive_into_clears_buf_on_entry() {
    let cfg = cfg("clears_on_entry");
    cleanup(&cfg);
    let mut p = Publisher::create("bus", cfg.clone()).unwrap();
    let sub_cfg = cfg.clone();
    let handle = std::thread::spawn(move || {
        let mut sub = Subscriber::connect("bus", &sub_cfg, Duration::from_secs(5)).unwrap();
        let mut buf = b"stale".to_vec();
        sub.receive_into(&mut buf).unwrap();
        assert_eq!(&buf[..], b"fresh", "previous content must not survive the next receive");
    });
    p.wait_for_subscribers(1, Duration::from_secs(5)).unwrap();
    p.publish(b"fresh").unwrap();
    handle.join().unwrap();
    cleanup(&cfg);
}

#[test]
fn try_receive_into_returns_false_when_empty() {
    let cfg = cfg("try_empty");
    cleanup(&cfg);
    let mut p = Publisher::create("bus", cfg.clone()).unwrap();
    let sub_cfg = cfg.clone();
    let handle = std::thread::spawn(move || {
        let mut sub = Subscriber::connect("bus", &sub_cfg, Duration::from_secs(5)).unwrap();
        let mut buf = b"old".to_vec();
        // Nothing published yet — try_receive_into returns false and
        // clears the buffer.
        assert!(!sub.try_receive_into(&mut buf));
        assert!(buf.is_empty(), "buffer must be cleared even on empty try_receive_into");
    });
    p.wait_for_subscribers(1, Duration::from_secs(5)).unwrap();
    handle.join().unwrap();
    cleanup(&cfg);
}

#[test]
fn receive_timeout_into_signals_timeout_with_false() {
    let cfg = cfg("timeout_false");
    cleanup(&cfg);
    let mut p = Publisher::create("bus", cfg.clone()).unwrap();
    let sub_cfg = cfg.clone();
    let handle = std::thread::spawn(move || {
        let mut sub = Subscriber::connect("bus", &sub_cfg, Duration::from_secs(5)).unwrap();
        let mut buf = Vec::new();
        // No publish yet → timeout.
        let got = sub.receive_timeout_into(Duration::from_millis(20), &mut buf).unwrap();
        assert!(!got, "no publish should be visible within 20 ms");
        assert!(buf.is_empty());
    });
    p.wait_for_subscribers(1, Duration::from_secs(5)).unwrap();
    handle.join().unwrap();
    cleanup(&cfg);
}

#[test]
fn buffer_reuse_matches_recv_semantics() {
    // Equivalence test: a loop calling recv_into into a reusable
    // buffer must produce the same payload sequence as a loop
    // calling recv() returning fresh Vecs.
    let cfg = cfg("equivalence");
    cleanup(&cfg);
    let mut p = Publisher::create("bus", cfg.clone()).unwrap();
    let sub_cfg = cfg.clone();
    let handle = std::thread::spawn(move || {
        let mut sub_a = Subscriber::connect("bus", &sub_cfg, Duration::from_secs(5)).unwrap();
        let mut sub_b = Subscriber::connect("bus", &sub_cfg, Duration::from_secs(5)).unwrap();
        let mut buf_b = Vec::new();
        let mut out_a = Vec::new();
        let mut out_b = Vec::new();
        for _ in 0..5u64 {
            let a = sub_a.receive().unwrap();
            sub_b.receive_into(&mut buf_b).unwrap();
            out_a.push(a);
            out_b.push(buf_b.clone());
        }
        assert_eq!(out_a, out_b);
    });
    p.wait_for_subscribers(2, Duration::from_secs(5)).unwrap();
    for i in 0..5u64 {
        p.publish(&i.to_le_bytes()).unwrap();
    }
    handle.join().unwrap();
    cleanup(&cfg);
}
