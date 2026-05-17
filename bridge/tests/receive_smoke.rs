//! End-to-end test of the B2 receive path:
//!
//!   test thread (impersonating a remote peer) ──► TCP ──►
//!     bridge listener ──► reader thread ──► publisher thread ──►
//!     local mmbus subscribe (receive_check thread)
//!
//! The test plays the role of a remote bridge: connect to the
//! configured `listen` address, send a `PeerHello` then a few `Msg`
//! frames over the wire.  A local `Bus::subscribe("events")` running
//! in another thread must receive their payloads in order.
//!
//! Also exercises loop prevention: a Msg frame stamped with the
//! bridge's OWN origin_id MUST be dropped by the reader rather than
//! republished locally.

use mmbus::Bus;
use mmbus_bridge::frame::Frame;
use mmbus_bridge::{Bridge, BridgeConfig};
use std::io::Write;
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

fn write_frame(stream: &mut TcpStream, frame: &Frame) {
    let mut buf = Vec::with_capacity(frame.encoded_len());
    frame.encode(&mut buf);
    stream.write_all(&buf).expect("write_all");
}

#[test]
fn bridge_receives_from_peer_and_republishes_locally() {
    let tmp = tempfile::tempdir().expect("tempdir");

    // Bind on an ephemeral local port so multiple test runs don't fight.
    let cfg_text = format!(
        r#"
            bus = "bridge-rx-smoke"
            base_dir = {base_dir:?}
            listen = "127.0.0.1:0"

            [[topics]]
            name = "events"
        "#,
        base_dir = tmp.path().to_str().unwrap(),
    );
    let cfg = BridgeConfig::from_str(&cfg_text).expect("parse config");
    let bridge = Bridge::start(&cfg).expect("bridge start");
    let listen_addr = bridge.listen_addr.expect("bridge bound a listener");

    // Subscribe locally so we can observe the republished messages.
    // Each thread/process needs its own Bus handle.
    //
    // We use `subscribe_with_history(100)` (not `subscribe_timeout`)
    // because there's an unavoidable race: the bridge's publisher
    // creates the ring lazily on its first `bus.publish(...)`, and the
    // first peer Msg might arrive + be republished BEFORE this thread
    // wins its subscribe.  With history-back, the subscriber starts at
    // `tail - 100` and can replay anything it missed during the
    // connect race.
    let sub_dir = tmp.path().to_path_buf();
    let receiver = thread::spawn(move || {
        let bus = Bus::with_config(
            "bridge-rx-smoke",
            mmbus::BusConfig {
                base_dir: sub_dir,
                ..Default::default()
            },
        );
        let mut sub = bus
            .subscribe_with_history_timeout("events", 100, Duration::from_secs(5))
            .expect("local subscribe must succeed");
        let mut got = Vec::new();
        for _ in 0..3 {
            let msg = sub
                .recv_timeout(Duration::from_secs(5))
                .expect("recv")
                .expect("recv timeout — bridge did not republish in time");
            got.push(msg);
        }
        got
    });

    // Connect to the bridge as a remote peer would.
    let mut peer = TcpStream::connect(listen_addr).expect("connect to bridge");
    peer.set_nodelay(true).ok();

    let peer_origin_id: u64 = 0xDEAD_BEEF_CAFE_F00D;
    write_frame(&mut peer, &Frame::peer_hello(peer_origin_id));

    // Three legitimate Msg frames.
    for i in 0..3u64 {
        write_frame(
            &mut peer,
            &Frame::msg(peer_origin_id, i, b"events".to_vec(), i.to_le_bytes().to_vec()),
        );
    }

    // Loop-prevention probe: a Msg stamped with the BRIDGE's own
    // origin_id must be dropped (never republished).  We send one
    // interleaved here; the receiver thread expects exactly 3 messages
    // so this frame appearing would make the recv() above hang on the
    // 4th call (or worse, return a 4th value before timeout).
    write_frame(
        &mut peer,
        &Frame::msg(
            bridge.origin_id,
            999,
            b"events".to_vec(),
            b"WOULD-LOOP".to_vec(),
        ),
    );

    // Drop the wire so the bridge's reader sees EOF and exits cleanly.
    drop(peer);

    let received = receiver.join().expect("receiver thread");
    assert_eq!(received.len(), 3, "must republish exactly 3 peer Msg frames");
    for (i, payload) in received.iter().enumerate() {
        assert_eq!(payload.as_slice(), &(i as u64).to_le_bytes());
    }

    bridge.shutdown();
}
