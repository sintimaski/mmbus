use mmbus::{Bus, BusConfig, Error, Publisher, Subscriber};
use std::time::Duration;

fn make_cfg(name: &str) -> BusConfig {
    BusConfig {
        capacity: 32,
        slot_size: 128,
        base_dir: std::env::temp_dir().join("mmbus_tests").join(name),
        ..Default::default()
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
        ..Default::default()
    };
    cleanup(&cfg);

    // A subscriber that connects but never reads holds the ring at capacity.
    // Without an active cursor the producer has no backpressure — we need
    // at least one subscriber for Full to be returned.
    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    let cfg_sub = cfg.clone();
    let sub_thread = std::thread::spawn(move || {
        let _sub = Subscriber::connect("bus", &cfg_sub, Duration::from_secs(5)).unwrap();
        let _ = done_rx.recv(); // hold cursor until signaled
    });

    let mut pub_ = Publisher::create("bus", cfg.clone()).unwrap();
    pub_.wait_for_subscribers(1, Duration::from_secs(5)).unwrap();

    let mut fulls = 0;
    for _ in 0..16 {
        match pub_.publish(b"data") {
            Ok(()) => {}
            Err(mmbus::Error::Full) => fulls += 1,
            Err(e) => panic!("unexpected: {e}"),
        }
    }
    assert!(fulls > 0, "expected Full errors");

    done_tx.send(()).unwrap();
    sub_thread.join().unwrap();
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

#[test]
fn too_large_returns_error() {
    let cfg = make_cfg("toolarge");
    cleanup(&cfg);
    let mut pub_ = Publisher::create("bus", cfg.clone()).unwrap();

    let oversized = vec![0u8; cfg.slot_size as usize + 1];
    match pub_.publish(&oversized) {
        Err(Error::TooLarge { size, max }) => {
            assert_eq!(size, cfg.slot_size as usize + 1);
            assert_eq!(max, cfg.slot_size as usize);
        }
        other => panic!("expected TooLarge, got {other:?}"),
    }
    cleanup(&cfg);
}

#[test]
fn late_subscriber_starts_at_current_tail() {
    // Publish N messages before any subscriber connects; subscriber should
    // see cursor = N and not receive any of those old messages.
    let cfg = make_cfg("latesub");
    cleanup(&cfg);
    let n = 5usize;

    let mut pub_ = Publisher::create("bus", cfg.clone()).unwrap();
    for i in 0..n {
        pub_.publish(format!("old-{i}").as_bytes()).unwrap();
    }

    // Subscriber connects after messages are already published.
    let cfg_sub = cfg.clone();
    let consumer = std::thread::spawn(move || {
        let mut sub = Subscriber::connect("bus", &cfg_sub, Duration::from_secs(5)).unwrap();
        let starting_cursor = sub.cursor();
        // Receive one new message published after connect.
        let msg = sub.receive().unwrap();
        (starting_cursor, msg)
    });

    pub_.wait_for_subscribers(1, Duration::from_secs(5)).unwrap();
    pub_.publish(b"new").unwrap();

    let (starting_cursor, msg) = consumer.join().unwrap();
    assert_eq!(starting_cursor, n as u64, "cursor should skip old messages");
    assert_eq!(msg, b"new");
    cleanup(&cfg);
}

#[test]
fn try_receive_nonblocking() {
    let cfg = make_cfg("tryrecv");
    cleanup(&cfg);

    let cfg_sub = cfg.clone();
    let consumer = std::thread::spawn(move || {
        let mut sub = Subscriber::connect("bus", &cfg_sub, Duration::from_secs(5)).unwrap();
        // Poll until a message arrives.
        loop {
            if let Some(msg) = sub.try_receive() {
                return msg;
            }
            std::hint::spin_loop();
        }
    });

    let mut pub_ = Publisher::create("bus", cfg.clone()).unwrap();
    pub_.wait_for_subscribers(1, Duration::from_secs(5)).unwrap();
    pub_.publish(b"nonblocking").unwrap();

    assert_eq!(consumer.join().unwrap(), b"nonblocking");
    cleanup(&cfg);
}

#[test]
fn stress_content_integrity() {
    // High-concurrency run: verify every message arrives with correct byte content.
    // Uses 10K messages with a u64 sequence number in the first 8 bytes.
    // Catches any atomicity bugs or slot corruption under concurrent load.
    let cfg = BusConfig {
        capacity: 64,
        slot_size: 16,
        base_dir: std::env::temp_dir().join("mmbus_tests").join("stress"),
        ..Default::default()
    };
    cleanup(&cfg);
    let n = 10_000usize;

    let cfg_sub = cfg.clone();
    let consumer = std::thread::spawn(move || {
        let mut sub = Subscriber::connect("bus", &cfg_sub, Duration::from_secs(10)).unwrap();
        (0..n).map(|_| {
            let bytes = sub.receive().unwrap();
            assert_eq!(bytes.len(), 8, "unexpected message length");
            u64::from_le_bytes(bytes.try_into().unwrap())
        }).collect::<Vec<u64>>()
    });

    let mut pub_ = Publisher::create("bus", cfg.clone()).unwrap();
    pub_.wait_for_subscribers(1, Duration::from_secs(5)).unwrap();
    for i in 0..n as u64 {
        let payload = i.to_le_bytes();
        loop {
            match pub_.publish(&payload) {
                Ok(()) => break,
                Err(Error::Full) => std::hint::spin_loop(),
                Err(e) => panic!("{e}"),
            }
        }
    }

    let received = consumer.join().unwrap();
    assert_eq!(received.len(), n);
    for (i, &val) in received.iter().enumerate() {
        assert_eq!(val, i as u64, "content mismatch at position {i}: got {val}");
    }
    cleanup(&cfg);
}

#[test]
fn subscriber_connect_times_out_when_no_publisher() {
    // No Publisher::create call → handshake socket never appears → the
    // subscriber must surface Error::Timeout, not hang forever or panic.
    let cfg = make_cfg("connect_timeout");
    cleanup(&cfg);
    let started = std::time::Instant::now();
    let result = Subscriber::connect("bus", &cfg, Duration::from_millis(200));
    let elapsed = started.elapsed();
    cleanup(&cfg);
    match result {
        Err(Error::Timeout(topic)) => assert_eq!(topic, "bus"),
        Err(other) => panic!("expected Error::Timeout, got {other}"),
        Ok(_) => panic!("expected Error::Timeout, got Ok(Subscriber)"),
    }
    // Should not have over-slept the deadline by more than 1s.
    assert!(
        elapsed < Duration::from_secs(1),
        "connect waited too long ({elapsed:?})"
    );
}

// ── Bus API tests ─────────────────────────────────────────────────────────────

fn bus_cfg(label: &str) -> BusConfig {
    BusConfig {
        capacity: 32,
        slot_size: 256,
        base_dir: std::env::temp_dir().join("mmbus_tests").join(label),
        ..Default::default()
    }
}

fn cleanup_bus(cfg: &BusConfig) {
    let _ = std::fs::remove_dir_all(&cfg.base_dir);
}

#[test]
fn bus_api_single_topic_roundtrip() {
    let cfg = bus_cfg("bus_single");
    cleanup_bus(&cfg);

    let cfg_sub = cfg.clone();
    let consumer = std::thread::spawn(move || {
        let bus = Bus::with_config("app", cfg_sub);
        let mut sub = bus.subscribe("events").unwrap();
        sub.recv().unwrap()
    });

    let mut bus = Bus::with_config("app", cfg.clone());
    // Create the publisher and wait for subscriber to connect before publishing.
    bus.wait_for_subscribers("events", 1, Duration::from_secs(5)).unwrap();
    bus.publish("events", b"hello bus").unwrap();

    assert_eq!(consumer.join().unwrap(), b"hello bus");
    cleanup_bus(&cfg);
}

#[test]
fn bus_api_two_topics_independent() {
    // Messages on "topic-a" must not appear on "topic-b" and vice versa.
    let cfg = bus_cfg("bus_two_topics");
    cleanup_bus(&cfg);

    let cfg_a = cfg.clone();
    let consumer_a = std::thread::spawn(move || {
        let bus = Bus::with_config("app", cfg_a);
        bus.subscribe("topic-a").unwrap().recv().unwrap()
    });

    let cfg_b = cfg.clone();
    let consumer_b = std::thread::spawn(move || {
        let bus = Bus::with_config("app", cfg_b);
        bus.subscribe("topic-b").unwrap().recv().unwrap()
    });

    let mut bus = Bus::with_config("app", cfg.clone());
    bus.wait_for_subscribers("topic-a", 1, Duration::from_secs(5)).unwrap();
    bus.wait_for_subscribers("topic-b", 1, Duration::from_secs(5)).unwrap();
    bus.publish("topic-a", b"for-a").unwrap();
    bus.publish("topic-b", b"for-b").unwrap();

    assert_eq!(consumer_a.join().unwrap(), b"for-a");
    assert_eq!(consumer_b.join().unwrap(), b"for-b");
    cleanup_bus(&cfg);
}

#[test]
fn bus_api_recv_timeout_returns_none() {
    let cfg = bus_cfg("bus_timeout");
    cleanup_bus(&cfg);

    // Create a publisher so the subscriber can connect.
    let mut bus_pub = Bus::with_config("app", cfg.clone());
    // Force publisher creation by publishing a dummy (no subscriber yet).
    let _ = bus_pub.publish("ch", b"ignored");

    let cfg_sub = cfg.clone();
    let result = std::thread::spawn(move || {
        let bus = Bus::with_config("app", cfg_sub);
        let mut sub = bus.subscribe_timeout("ch", Duration::from_secs(5)).unwrap();
        sub.recv_timeout(Duration::from_millis(100)).unwrap()
    })
    .join()
    .unwrap();

    assert!(result.is_none(), "expected timeout (None), got {result:?}");
    cleanup_bus(&cfg);
}

#[test]
fn bus_api_iterator() {
    let cfg = bus_cfg("bus_iter");
    cleanup_bus(&cfg);
    let n = 50usize;

    let cfg_sub = cfg.clone();
    let consumer = std::thread::spawn(move || {
        let bus = Bus::with_config("app", cfg_sub);
        bus.subscribe("stream")
            .unwrap()
            .take(n)
            .map(|r| r.unwrap())
            .collect::<Vec<_>>()
    });

    let mut bus = Bus::with_config("app", cfg.clone());
    bus.wait_for_subscribers("stream", 1, Duration::from_secs(5)).unwrap();
    for i in 0..n {
        loop {
            match bus.publish("stream", format!("{i:03}").as_bytes()) {
                Ok(()) => break,
                Err(Error::Full) => std::hint::spin_loop(),
                Err(e) => panic!("{e}"),
            }
        }
    }

    let received = consumer.join().unwrap();
    assert_eq!(received.len(), n);
    for (i, msg) in received.iter().enumerate() {
        assert_eq!(msg.as_slice(), format!("{i:03}").as_bytes(), "message {i}");
    }
    cleanup_bus(&cfg);
}

#[test]
fn bus_api_recv_into_slice_variable_size() {
    // The zero-alloc receive path (M1): write messages of differing sizes
    // straight into a caller buffer, returning the actual byte count.
    let cfg = bus_cfg("bus_slice");
    cleanup_bus(&cfg);

    let cfg_sub = cfg.clone();
    let consumer = std::thread::spawn(move || {
        let bus = Bus::with_config("app", cfg_sub);
        let mut sub = bus.subscribe("ch").unwrap();
        assert_eq!(sub.slot_size(), 256, "slot_size accessor");
        let mut buf = vec![0u8; sub.slot_size() as usize];
        let mut got = Vec::new();
        // First two via the WAL-aware non-blocking path after a wakeup,
        // the rest via the blocking wait_readable + slice read loop.
        while got.len() < 3 {
            match sub.try_recv_one_into_slice(&mut buf).unwrap() {
                Some(len) => got.push(buf[..len].to_vec()),
                None => {
                    sub.wait_readable(-1).unwrap();
                }
            }
        }
        got
    });

    let mut bus = Bus::with_config("app", cfg.clone());
    bus.wait_for_subscribers("ch", 1, Duration::from_secs(5)).unwrap();
    bus.publish("ch", b"a").unwrap();
    bus.publish("ch", b"bcde").unwrap();
    bus.publish("ch", &[0x7u8; 200]).unwrap();

    let got = consumer.join().unwrap();
    assert_eq!(got[0], b"a");
    assert_eq!(got[1], b"bcde");
    assert_eq!(got[2], [0x7u8; 200]);
    cleanup_bus(&cfg);
}

#[test]
fn bus_api_recv_into_slice_too_large_errors() {
    // A buffer smaller than the live-ring payload yields Error::TooLarge.
    let cfg = bus_cfg("bus_slice_big");
    cleanup_bus(&cfg);

    let cfg_sub = cfg.clone();
    let consumer = std::thread::spawn(move || {
        let bus = Bus::with_config("app", cfg_sub);
        let mut sub = bus.subscribe("ch").unwrap();
        let mut tiny = [0u8; 4];
        loop {
            match sub.try_recv_one_into_slice(&mut tiny) {
                Ok(Some(_)) => panic!("4-byte payload should have fit; expected oversize"),
                Ok(None) => sub.wait_readable(-1).unwrap(),
                Err(e) => return e,
            };
        }
    });

    let mut bus = Bus::with_config("app", cfg.clone());
    bus.wait_for_subscribers("ch", 1, Duration::from_secs(5)).unwrap();
    bus.publish("ch", b"this payload is longer than four bytes").unwrap();

    match consumer.join().unwrap() {
        Error::TooLarge { max, .. } => assert_eq!(max, 4),
        other => panic!("expected TooLarge, got {other:?}"),
    }
    cleanup_bus(&cfg);
}
