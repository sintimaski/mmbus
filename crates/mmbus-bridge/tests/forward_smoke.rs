//! End-to-end test of the B1 forward path:
//!
//!   local mmbus publish ──► bridge subscriber thread ──► encoded Frame
//!     ──► bridge forwarder thread ──► TCP ──► test `TcpListener`
//!
//! The test plays the role of the remote peer: bind a `TcpListener`,
//! accept the bridge's connection, read the wire bytes, and decode
//! them with `bridge::frame::decode`.  Then we publish a handful of
//! messages locally and assert the listener sees them in order.

use mmbus::Bus;
use mmbus_bridge::frame::{decode, parse_peer_hello, Frame, FrameType, MIN_FRAME_LEN};
use mmbus_bridge::{Bridge, BridgeConfig};
use std::io::Read;
use std::net::TcpListener;
use std::thread;
use std::time::{Duration, Instant};

/// Loop `decode` over the buffer until at least `want` frames have been
/// parsed.  Returns the parsed frames + the un-parsed tail bytes.
fn drain_frames(buf: &[u8], want: usize) -> Option<(Vec<Frame>, usize)> {
    let mut frames = Vec::new();
    let mut consumed = 0;
    while frames.len() < want {
        match decode(&buf[consumed..]) {
            Ok((f, n)) => {
                frames.push(f);
                consumed += n;
            }
            Err(_) => return None,
        }
    }
    Some((frames, consumed))
}

#[test]
fn bridge_forwards_local_publishes_to_one_peer_over_tcp() {
    // 1) Stand up a test peer (just a TcpListener; we'll do the read
    //    side by hand and decode bytes with bridge::frame::decode).
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let peer_addr = listener.local_addr().unwrap();
    listener.set_nonblocking(false).unwrap();

    // 2) Build a bridge config pointing at that peer.  Use a unique
    //    base_dir + bus name so we don't collide with any other test.
    let tmp = tempfile::tempdir().expect("tempdir");
    let toml_text = format!(
        r#"
            bus = "bridge-smoke"
            base_dir = {base_dir:?}

            [[topics]]
            name = "events"

            [[peers]]
            name = "peer"
            endpoint = "{addr}"
            preshared_key = "k"
        "#,
        base_dir = tmp.path().to_str().unwrap(),
        addr = peer_addr,
    );
    let cfg = BridgeConfig::from_str(&toml_text).expect("parse config");

    // 3) Start a thread that publishes to mmbus on the same bus/topic.
    //    Each thread/process needs its own Bus handle; the bridge holds
    //    its own Bus in its subscriber thread.
    //
    //    The thread must keep its Bus ALIVE until the reader has the
    //    frames: when the publisher's Bus drops, the bridge's mmbus
    //    subscriber sees the publisher hang up and stops reading.  If
    //    that happens before it has drained our 5 messages (likely on a
    //    loaded runner under the heavier `--features quic` build), only
    //    the PeerHello reaches TCP and the reader times out with a
    //    partial buffer.  `done_rx` parks the thread (keeping the Bus
    //    open) until the main thread signals it has read everything.
    let publish_dir = tmp.path().to_path_buf();
    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    let publish_thread = thread::spawn(move || {
        let mut bus = Bus::with_config(
            "bridge-smoke",
            mmbus::BusConfig {
                base_dir: publish_dir,
                ..Default::default()
            },
        );
        // 30s safety-net (not a perf assertion): the bridge's subscriber
        // thread can be slow to connect on a loaded CI runner.
        bus.wait_for_subscribers("events", 1, Duration::from_secs(30))
            .expect("bridge subscriber must connect");
        for i in 0..5u64 {
            bus.publish("events", &i.to_le_bytes()).expect("publish ok");
        }
        // Hold the Bus open until the reader is done (or a generous
        // fallback elapses, so a failing test can't hang forever).
        let _ = done_rx.recv_timeout(Duration::from_secs(60));
    });

    // 4) Start the bridge.  Its subscriber thread will connect to the
    //    mmbus topic that the publish_thread above is about to create.
    let bridge = Bridge::start(&cfg).expect("bridge start");

    // 5) Accept the bridge's TCP connection.
    let (mut stream, _) = listener.accept().expect("accept bridge");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // 6) Read bytes until we have a PeerHello + 5 Msg frames.  Total
    //    minimum size = 6 * MIN_FRAME_LEN; we read in 1 KiB chunks and
    //    try decode each pass.
    let mut buf = Vec::with_capacity(8 * MIN_FRAME_LEN);
    let mut chunk = [0u8; 1024];
    // 30s safety-net (not a perf assertion); a 5s deadline is flake-prone on
    // loaded CI runners — same fix as mesh_smoke's collect_n_frames.
    let deadline = Instant::now() + Duration::from_secs(30);
    let (frames, _consumed) = loop {
        if Instant::now() >= deadline {
            panic!(
                "timed out waiting for 6 frames; got {} bytes so far",
                buf.len()
            );
        }
        match stream.read(&mut chunk) {
            Ok(0) => panic!("peer closed unexpectedly; buf={}", buf.len()),
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                continue;
            }
            Err(e) => panic!("read failed: {e}"),
        }
        if let Some(parsed) = drain_frames(&buf, 6) {
            break parsed;
        }
    };

    // 7) We have all 6 frames — the messages are safely on the wire, so
    //    the publisher can drop its Bus now.
    let _ = done_tx.send(());
    publish_thread.join().expect("publisher thread");

    let hello = &frames[0];
    assert_eq!(hello.frame_type, FrameType::PeerHello);
    assert_eq!(hello.origin_id, bridge.origin_id);
    assert_eq!(hello.topic, b"");
    let parts = parse_peer_hello(&hello.payload).expect("parse hello");
    assert_eq!(parts.origin_id, bridge.origin_id);
    assert_eq!(parts.psk, b"k", "forwarder must send the configured PSK");

    for (i, frame) in frames[1..6].iter().enumerate() {
        assert_eq!(frame.frame_type, FrameType::Msg);
        assert_eq!(frame.origin_id, bridge.origin_id);
        assert_eq!(frame.origin_seq, i as u64, "seq must be monotonic");
        assert_eq!(frame.topic, b"events");
        assert_eq!(frame.payload, (i as u64).to_le_bytes().to_vec());
    }

    drop(stream);
    bridge.shutdown();
}
