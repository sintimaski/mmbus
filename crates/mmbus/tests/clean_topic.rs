//! `Bus::clean_topic` removes on-disk state safely.

use mmbus::{Bus, BusConfig, Error};
use std::time::Duration;

fn base_dir(suffix: &str) -> std::path::PathBuf {
    std::env::temp_dir().join("mmclean").join(suffix)
}

fn config(suffix: &str) -> BusConfig {
    BusConfig {
        capacity: 16,
        slot_size: 32,
        base_dir: base_dir(suffix),
        ..Default::default()
    }
}

#[test]
fn clean_topic_on_nonexistent_is_ok() {
    let cfg = config("nonexistent");
    let _ = std::fs::remove_dir_all(&cfg.base_dir);
    let mut bus = Bus::with_config("bus", cfg);
    bus.clean_topic("never-existed").expect("should be a no-op");
}

#[test]
fn clean_topic_removes_existing_state() {
    let cfg = config("removes");
    let _ = std::fs::remove_dir_all(&cfg.base_dir);
    let topic_dir = cfg.base_dir.join("bus").join("events");

    {
        // Create a publisher, write a message, drop it — leaves files on disk.
        let mut bus = Bus::with_config("bus", cfg.clone());
        bus.publish("events", b"hello").unwrap();
    }
    assert!(
        topic_dir.exists(),
        "publisher should have created the topic dir"
    );

    let mut bus = Bus::with_config("bus", cfg.clone());
    bus.clean_topic("events").expect("clean should succeed");
    assert!(!topic_dir.exists(), "topic dir should be gone");
}

#[test]
fn clean_topic_refuses_when_publisher_active() {
    let cfg = config("active_pub");
    let _ = std::fs::remove_dir_all(&cfg.base_dir);

    // Hold an active publisher in this process via one Bus...
    let mut owner = Bus::with_config("bus", cfg.clone());
    owner.publish("events", b"hello").unwrap();

    // ...and try to clean from a different Bus instance (same process).
    // Should fail because the in-process producer lock is held by `owner`.
    let mut cleaner = Bus::with_config("bus", cfg.clone());
    match cleaner.clean_topic("events") {
        Err(Error::AlreadyPublishing(_)) => (),
        other => panic!("expected AlreadyPublishing, got {other:?}"),
    }

    // After the owner drops, cleaning succeeds.
    drop(owner);
    cleaner
        .clean_topic("events")
        .expect("clean after owner-drop should succeed");

    let _ = std::fs::remove_dir_all(&cfg.base_dir);
}

#[test]
#[cfg_attr(
    windows,
    ignore = "flaky on Windows CI — subscriber recv occasionally observes \
              'publisher disconnected (pipe broken)' before the second publish lands. \
              Pre-existed v0.2.0; tracked as a follow-up. Linux + macOS CI pass cleanly."
)]
fn clean_topic_then_republish_works() {
    let cfg = config("repub");
    let _ = std::fs::remove_dir_all(&cfg.base_dir);

    let mut bus = Bus::with_config("bus", cfg.clone());
    bus.publish("events", b"first").unwrap();
    bus.clean_topic("events").unwrap();

    // After cleanup, a fresh publish creates the topic from scratch (no
    // stale state from the prior pub).  Subscriber connects to the NEW
    // ring and sees only the new message.
    let sub_cfg = cfg.clone();
    let t = std::thread::spawn(move || {
        let sub_bus = Bus::with_config("bus", sub_cfg);
        let mut sub = sub_bus
            .subscribe_timeout("events", Duration::from_secs(5))
            .unwrap();
        sub.recv().unwrap()
    });

    bus.wait_for_subscribers("events", 1, Duration::from_secs(5))
        .unwrap();
    bus.publish("events", b"second").unwrap();

    let msg = t.join().unwrap();
    assert_eq!(msg, b"second");
    let _ = std::fs::remove_dir_all(&cfg.base_dir);
}
