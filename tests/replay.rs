//! Phase-A in-ring replay: `subscribe_with_history` + `subscribe_from`.

use mmbus::{Bus, BusConfig, Error};
use std::hint::spin_loop;
use std::time::Duration;

fn cfg(name: &str) -> BusConfig {
    BusConfig {
        capacity: 32,
        slot_size: 16,
        base_dir: std::env::temp_dir().join("mmreplay").join(name),
        ..Default::default()
    }
}

fn cleanup(cfg: &BusConfig) {
    let _ = std::fs::remove_dir_all(&cfg.base_dir);
}

/// Subscribing with `n_messages_back = N/2` replays the last N/2 of N
/// already-published messages (no losses, in order).
#[test]
fn subscribe_with_history_replays_recent_messages() {
    let cfg = cfg("history_half");
    cleanup(&cfg);
    const N: u64 = 16; // less than capacity (32) so no overrun

    let mut bus = Bus::with_config("bus", cfg.clone());
    for i in 0..N {
        bus.publish("ch", &i.to_le_bytes()).unwrap();
    }

    let sub_cfg = cfg.clone();
    let t = std::thread::spawn(move || {
        let sub_bus = Bus::with_config("bus", sub_cfg);
        let mut sub = sub_bus.subscribe_with_history("ch", N / 2).unwrap();
        let mut got = Vec::new();
        for _ in 0..(N / 2) {
            got.push(u64::from_le_bytes(sub.recv().unwrap().try_into().unwrap()));
        }
        got
    });

    // Subscriber may already be connected, or may still be waiting — but
    // its cursor is already at tail - N/2.  We don't need wait_for_subs;
    // those messages are already in the ring.  Drop the bus when the test
    // ends to trigger POLLHUP cleanup.
    let got = t.join().unwrap();
    assert_eq!(got.len(), (N / 2) as usize);
    for (i, &v) in got.iter().enumerate() {
        let expected = N / 2 + i as u64;
        assert_eq!(v, expected, "msg {i}: expected {expected} got {v}");
    }
    drop(bus);
    cleanup(&cfg);
}

/// Asking for more history than the ring holds is best-effort: the seqlock
/// in `try_receive` skips forward to the oldest available slot.  Subscriber
/// may receive fewer messages than requested, in order, no panic.
#[test]
fn subscribe_with_history_beyond_capacity_is_best_effort() {
    let cfg = cfg("history_overflow");
    cleanup(&cfg);
    const N: u64 = 200; // 6.25x capacity; first ~168 messages already overwritten
    const CAPACITY: u64 = 32;

    let mut bus = Bus::with_config("bus", cfg.clone());
    for i in 0..N {
        loop {
            // No subscribers yet → publish always succeeds (no pinning cursor).
            match bus.publish("ch", &i.to_le_bytes()) {
                Ok(()) => break,
                Err(Error::Full) => spin_loop(),
                Err(e) => panic!("{e}"),
            }
        }
    }

    let sub_cfg = cfg.clone();
    let t = std::thread::spawn(move || {
        let sub_bus = Bus::with_config("bus", sub_cfg);
        // Ask for 5x capacity worth.  Only ~capacity is available.
        let mut sub = sub_bus.subscribe_with_history("ch", 5 * CAPACITY).unwrap();
        let mut got = Vec::new();
        // Read what's available; bail when nothing arrives for 200 ms.
        while let Some(bytes) =
            sub.recv_timeout(Duration::from_millis(200)).unwrap()
        {
            got.push(u64::from_le_bytes(bytes.try_into().unwrap()));
        }
        got
    });

    let got = t.join().unwrap();
    assert!(
        !got.is_empty() && got.len() <= CAPACITY as usize,
        "expected 1..{} messages, got {}",
        CAPACITY,
        got.len()
    );
    // Strictly monotonic.
    for w in got.windows(2) {
        assert!(w[1] > w[0], "out of order: {} after {}", w[1], w[0]);
    }
    // All values are in the post-(N-capacity) tail (allowing for skip-ahead).
    let first_possible = N - CAPACITY;
    assert!(got[0] >= first_possible, "msg {} predates the ring", got[0]);
    assert!(*got.last().unwrap() < N, "msg from the future");

    drop(bus);
    cleanup(&cfg);
}

/// `subscribe_from(0)` when the publisher hasn't wrapped yet replays
/// everything from the beginning.
#[test]
fn subscribe_from_zero_when_unwrapped_replays_all() {
    let cfg = cfg("from_zero");
    cleanup(&cfg);
    const N: u64 = 10; // well under capacity

    let mut bus = Bus::with_config("bus", cfg.clone());
    for i in 0..N {
        bus.publish("ch", &i.to_le_bytes()).unwrap();
    }

    let sub_cfg = cfg.clone();
    let t = std::thread::spawn(move || {
        let sub_bus = Bus::with_config("bus", sub_cfg);
        let mut sub = sub_bus.subscribe_from("ch", 0).unwrap();
        (0..N)
            .map(|_| u64::from_le_bytes(sub.recv().unwrap().try_into().unwrap()))
            .collect::<Vec<_>>()
    });

    let got = t.join().unwrap();
    assert_eq!(got, (0..N).collect::<Vec<_>>());

    drop(bus);
    cleanup(&cfg);
}

/// `subscribe_from(very_old)` when the publisher HAS wrapped returns
/// `Error::CursorTooOld`.
#[test]
fn subscribe_from_predating_ring_returns_cursor_too_old() {
    let cfg = cfg("from_too_old");
    cleanup(&cfg);
    const CAPACITY: u64 = 32;
    const N: u64 = 200;

    let mut bus = Bus::with_config("bus", cfg.clone());
    for i in 0..N {
        loop {
            match bus.publish("ch", &i.to_le_bytes()) {
                Ok(()) => break,
                Err(Error::Full) => spin_loop(),
                Err(e) => panic!("{e}"),
            }
        }
    }
    // Tail is now N; oldest in-ring slot is N - CAPACITY.  Request cursor=10
    // which is way older.
    let result = bus.subscribe_from("ch", 10);
    match result {
        Err(Error::CursorTooOld { requested, oldest }) => {
            assert_eq!(requested, 10);
            assert_eq!(oldest, N - CAPACITY);
        }
        Err(e) => panic!("expected CursorTooOld, got error {e}"),
        Ok(_) => panic!("expected CursorTooOld, got Ok(subscription)"),
    }

    drop(bus);
    cleanup(&cfg);
}

/// Round-trip via cursor checkpoint: subscribe, read some, record cursor,
/// disconnect; reconnect with `subscribe_from(checkpoint)` and continue.
#[test]
fn cursor_checkpoint_round_trip() {
    let cfg = cfg("checkpoint");
    cleanup(&cfg);
    const FIRST: u64 = 5;
    const SECOND: u64 = 5;
    const TOTAL: u64 = FIRST + SECOND;

    let mut bus = Bus::with_config("bus", cfg.clone());
    for i in 0..TOTAL {
        bus.publish("ch", &i.to_le_bytes()).unwrap();
    }

    // First subscriber consumes FIRST messages, records its cursor.
    let checkpoint = {
        let mut sub = bus.subscribe_with_history("ch", TOTAL).unwrap();
        for _ in 0..FIRST {
            sub.recv().unwrap();
        }
        sub.cursor()
        // sub drops here, cursor slot released.
    };
    assert_eq!(checkpoint, FIRST, "should have consumed exactly {FIRST}");

    // Second subscriber resumes from the checkpoint.
    let mut sub = bus.subscribe_from("ch", checkpoint).unwrap();
    let mut got = Vec::new();
    for _ in 0..SECOND {
        got.push(u64::from_le_bytes(sub.recv().unwrap().try_into().unwrap()));
    }
    assert_eq!(got, (FIRST..TOTAL).collect::<Vec<_>>());

    drop(bus);
    cleanup(&cfg);
}
