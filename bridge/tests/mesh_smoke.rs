//! Two-peer mesh: verify that a single local publish fans out to all
//! configured peers, each of which receives the same Frame stream.
//!
//! Layout:
//!
//!   mmbus publish ──► bridge subscriber ──► encode Frame ──┬──► peer A (TcpListener) ──► test reader
//!                                                          └──► peer B (TcpListener) ──► test reader

use mmbus::Bus;
use mmbus_bridge::frame::{decode, Frame, FrameType, MIN_FRAME_LEN};
use mmbus_bridge::{Bridge, BridgeConfig};
use std::io::Read;
use std::net::TcpListener;
use std::thread;
use std::time::{Duration, Instant};

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

fn collect_n_frames(mut stream: std::net::TcpStream, want: usize) -> Vec<Frame> {
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut buf = Vec::with_capacity(want * MIN_FRAME_LEN * 2);
    let mut tmp = [0u8; 1024];
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if Instant::now() >= deadline {
            panic!("peer timed out at {} bytes (wanted {} frames)", buf.len(), want);
        }
        match stream.read(&mut tmp) {
            Ok(0) => panic!("peer closed at {} bytes (wanted {} frames)", buf.len(), want),
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                continue
            }
            Err(e) => panic!("read failed: {e}"),
        }
        if let Some((frames, _)) = drain_frames(&buf, want) {
            return frames;
        }
    }
}

#[test]
fn bridge_fans_out_to_two_peers_via_mesh() {
    let tmp = tempfile::tempdir().expect("tempdir");

    // Stand up two test peer listeners.
    let listener_a = TcpListener::bind("127.0.0.1:0").expect("bind a");
    let addr_a = listener_a.local_addr().unwrap();
    let listener_b = TcpListener::bind("127.0.0.1:0").expect("bind b");
    let addr_b = listener_b.local_addr().unwrap();

    let cfg_text = format!(
        r#"
            bus = "mesh-smoke"
            base_dir = {base_dir:?}

            [[topics]]
            name = "events"

            [[peers]]
            name = "peer-a"
            endpoint = "{addr_a}"
            preshared_key = "k"

            [[peers]]
            name = "peer-b"
            endpoint = "{addr_b}"
            preshared_key = "k"
        "#,
        base_dir = tmp.path().to_str().unwrap(),
        addr_a = addr_a,
        addr_b = addr_b,
    );
    let cfg = BridgeConfig::from_str(&cfg_text).expect("parse config");

    // Local mmbus publisher.
    let publish_dir = tmp.path().to_path_buf();
    let publish_thread = thread::spawn(move || {
        let mut bus = Bus::with_config(
            "mesh-smoke",
            mmbus::BusConfig {
                base_dir: publish_dir,
                ..Default::default()
            },
        );
        bus.wait_for_subscribers("events", 1, Duration::from_secs(5))
            .expect("bridge subscriber must connect");
        for i in 0..4u64 {
            bus.publish("events", &i.to_le_bytes())
                .expect("publish ok");
        }
    });

    let bridge = Bridge::start(&cfg).expect("bridge start");

    // Accept both peer connections, collect frames in parallel.
    let accept_a = thread::spawn(move || {
        let (stream, _) = listener_a.accept().expect("accept a");
        collect_n_frames(stream, 5) // 1 hello + 4 msgs
    });
    let accept_b = thread::spawn(move || {
        let (stream, _) = listener_b.accept().expect("accept b");
        collect_n_frames(stream, 5)
    });

    publish_thread.join().expect("publisher thread");
    let frames_a = accept_a.join().expect("peer a thread");
    let frames_b = accept_b.join().expect("peer b thread");

    for (peer_label, frames) in [("a", frames_a), ("b", frames_b)] {
        assert_eq!(
            frames[0].frame_type,
            FrameType::PeerHello,
            "peer {peer_label}: first frame must be PeerHello"
        );
        assert_eq!(
            frames[0].origin_id, bridge.origin_id,
            "peer {peer_label}: hello origin_id mismatch"
        );

        for (i, frame) in frames[1..5].iter().enumerate() {
            assert_eq!(
                frame.frame_type,
                FrameType::Msg,
                "peer {peer_label}: msg {i} type"
            );
            assert_eq!(
                frame.origin_id, bridge.origin_id,
                "peer {peer_label}: msg {i} origin_id"
            );
            assert_eq!(frame.topic, b"events", "peer {peer_label}: msg {i} topic");
            assert_eq!(
                frame.payload,
                (i as u64).to_le_bytes().to_vec(),
                "peer {peer_label}: msg {i} payload"
            );
        }
    }

    bridge.shutdown();
}
