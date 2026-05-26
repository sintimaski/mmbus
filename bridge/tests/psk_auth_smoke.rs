//! PSK authentication on the receive path:
//!   * A peer that presents a PSK in our configured set is accepted
//!     and its Msg frames get republished locally.
//!   * A peer that presents the wrong PSK is dropped without any
//!     Msg frames being republished, regardless of how many it sends.
//!
//! Same shape as receive_smoke but exercises the authentication gate.

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

fn build_bridge(tmp_dir: &std::path::Path, bus_name: &str) -> (Bridge, std::net::SocketAddr) {
    let cfg_text = format!(
        r#"
            bus = {bus_name:?}
            base_dir = {base_dir:?}
            listen = "127.0.0.1:0"

            [[topics]]
            name = "events"

            [[peers]]
            name = "alice"
            endpoint = "127.0.0.1:1"   # we never dial it; only consumed for accepted-PSK set
            preshared_key = "good-psk"
        "#,
        bus_name = bus_name,
        base_dir = tmp_dir.to_str().unwrap(),
    );
    let cfg = BridgeConfig::from_str(&cfg_text).expect("parse config");
    let bridge = Bridge::start(&cfg).expect("bridge start");
    let addr = bridge.listen_addr.expect("listener bound");
    (bridge, addr)
}

#[test]
fn good_psk_authenticates_and_republishes() {
    let tmp = tempfile::tempdir().unwrap();
    let (bridge, addr) = build_bridge(tmp.path(), "psk-good");

    let sub_dir = tmp.path().to_path_buf();
    let receiver = thread::spawn(move || {
        let bus = Bus::with_config(
            "psk-good",
            mmbus::BusConfig {
                base_dir: sub_dir,
                ..Default::default()
            },
        );
        // 15 s gives plenty of headroom for cargo's parallel test
        // binaries to context-switch through the
        //   peer connect → bridge reader → publisher_main → ring create
        // chain before the deadline fires.
        let mut sub = bus
            .subscribe_with_history_timeout("events", 100, Duration::from_secs(15))
            .expect("subscribe");
        let mut got = Vec::new();
        for _ in 0..2 {
            got.push(
                sub.recv_timeout(Duration::from_secs(5))
                    .expect("recv")
                    .expect("recv timeout"),
            );
        }
        got
    });

    let mut peer = TcpStream::connect(addr).expect("connect");
    write_frame(&mut peer, &Frame::peer_hello_with_psk(7, b"good-psk"));
    for i in 0..2u64 {
        write_frame(
            &mut peer,
            &Frame::msg(7, i, b"events".to_vec(), i.to_le_bytes().to_vec()),
        );
    }
    drop(peer);

    let got = receiver.join().expect("receiver");
    assert_eq!(got.len(), 2);
    for (i, m) in got.iter().enumerate() {
        assert_eq!(m.as_slice(), &(i as u64).to_le_bytes());
    }

    bridge.shutdown();
}

#[test]
fn wrong_psk_is_dropped_and_messages_not_republished() {
    let tmp = tempfile::tempdir().unwrap();
    let (bridge, addr) = build_bridge(tmp.path(), "psk-bad");

    // Subscribe locally; we'll fail-fast assert that no message arrives
    // within a short deadline if the auth gate works.
    let sub_dir = tmp.path().to_path_buf();
    let receiver = thread::spawn(move || {
        let bus = Bus::with_config(
            "psk-bad",
            mmbus::BusConfig {
                base_dir: sub_dir,
                ..Default::default()
            },
        );
        // The bridge's publisher only creates the ring lazily on its
        // first bus.publish — and an unauthenticated peer should
        // produce ZERO publishes.  So this subscribe call should
        // time out at the configured deadline; the timeout is the
        // assertion.
        let result = bus.subscribe_with_history_timeout(
            "events",
            100,
            Duration::from_millis(800),
        );
        match result {
            Err(mmbus::Error::Timeout(_)) => Ok(()),
            Ok(mut sub) => {
                if let Some(msg) = sub
                    .recv_timeout(Duration::from_millis(500))
                    .unwrap_or(None)
                {
                    Err(format!(
                        "unauthenticated peer should not have republished: got {:?}",
                        msg
                    ))
                } else {
                    Ok(())
                }
            }
            Err(e) => Err(format!("unexpected error: {e}")),
        }
    });

    let mut peer = TcpStream::connect(addr).expect("connect");
    write_frame(&mut peer, &Frame::peer_hello_with_psk(99, b"WRONG-PSK"));
    // Spam Msg frames.  None should be republished because the bridge
    // closes our connection after rejecting the hello PSK.  That close
    // means these writes MAY fail with broken-pipe / connection-reset —
    // which is correct, expected behaviour (it's the very drop we're
    // asserting), so tolerate write errors here.  (On Linux the writes
    // tend to buffer before the RST lands; on macOS the close is observed
    // immediately and write_all fails — using the strict `write_frame`
    // here made the test flaky on macOS CI.)  The real assertion is the
    // receiver thread's timeout: zero messages republished.
    for i in 0..10u64 {
        let frame = Frame::msg(99, i, b"events".to_vec(), b"NOPE".to_vec());
        let mut buf = Vec::with_capacity(frame.encoded_len());
        frame.encode(&mut buf);
        let _ = peer.write_all(&buf);
    }
    drop(peer);

    let result = receiver.join().expect("receiver");
    result.unwrap_or_else(|err| panic!("{err}"));

    bridge.shutdown();
}
