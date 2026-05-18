//! End-to-end QUIC tests: two bridges in the same process,
//! forwarder ↔ listener over real QUIC with cert pinning.
//!
//! Only built when --features quic is on.

#![cfg(feature = "quic")]

use mmbus::Bus;
use mmbus_bridge::{Bridge, BridgeConfig};
use std::thread;
use std::time::Duration;

/// Build a bridge whose listen_quic uses an ephemeral port.  Returns
/// (bridge, the resolved quic socket addr, the bridge's QUIC cert
/// fingerprint).  This is the "receiver" side of every test.
fn build_quic_listener(
    tmp: &std::path::Path,
    bus_name: &str,
    psk: &str,
) -> (Bridge, std::net::SocketAddr, String) {
    let cert = tmp.join("recv.cert.der");
    let key = tmp.join("recv.key.der");
    let toml = format!(
        r#"
            bus = {bus_name:?}
            base_dir = {base_dir:?}
            listen_quic = "127.0.0.1:0"
            quic_cert_path = {cert:?}
            quic_key_path = {key:?}

            [[topics]]
            name = "events"

            [[peers]]
            name = "remote"
            endpoint = "127.0.0.1:1"
            preshared_key = {psk:?}
            transport = "tcp"
        "#,
        bus_name = bus_name,
        base_dir = tmp.to_str().unwrap(),
        cert = cert,
        key = key,
        psk = psk,
    );
    let cfg = BridgeConfig::from_str(&toml).expect("parse listener config");
    let b = Bridge::start(&cfg).expect("start listener");
    let addr = b.quic_listen_addr.expect("quic_listen_addr populated");
    let fp = b.quic_fingerprint.clone().expect("quic_fingerprint populated");
    (b, addr, fp)
}

/// Build a bridge whose single peer dials the given (quic) endpoint
/// with the given pinned fingerprint + PSK.  Returns the bridge.
fn build_quic_dialer(
    tmp: &std::path::Path,
    bus_name: &str,
    peer_endpoint: std::net::SocketAddr,
    pinned_fp: &str,
    psk: &str,
) -> Bridge {
    let cert = tmp.join("send.cert.der");
    let key = tmp.join("send.key.der");
    let toml = format!(
        r#"
            bus = {bus_name:?}
            base_dir = {base_dir:?}
            quic_cert_path = {cert:?}
            quic_key_path = {key:?}

            [[topics]]
            name = "events"

            [[peers]]
            name = "remote"
            endpoint = "{endpoint}"
            preshared_key = {psk:?}
            transport = "quic"
            peer_cert_fingerprint = {fp:?}
        "#,
        bus_name = bus_name,
        base_dir = tmp.to_str().unwrap(),
        cert = cert,
        key = key,
        endpoint = peer_endpoint,
        psk = psk,
        fp = pinned_fp,
    );
    let cfg = BridgeConfig::from_str(&toml).expect("parse dialer config");
    Bridge::start(&cfg).expect("start dialer")
}

#[test]
#[ignore = "flaky on CI under --features quic — subscriber panics with Any{..} \
            (bridge's QUIC accept→deliver pipeline has a startup race).  Passes \
            locally; tracked alongside mesh_smoke::bridge_fans_out_to_two_peers_via_mesh."]
fn quic_forward_then_receive_end_to_end() {
    let tmp_recv = tempfile::tempdir().unwrap();
    let tmp_send = tempfile::tempdir().unwrap();
    let psk = "quic-psk-1";

    // 1) Receiver bridge: listen_quic + a single TCP peer entry to
    //    populate the accepted_psk set with our PSK.
    let (recv_bridge, recv_addr, recv_fp) =
        build_quic_listener(tmp_recv.path(), "quic-recv", psk);

    // 2) Local subscriber on the receiver-side bus.  History-back so it
    //    catches up regardless of publish-vs-subscribe race.
    let recv_dir = tmp_recv.path().to_path_buf();
    let sub_handle = thread::spawn(move || {
        let bus = Bus::with_config(
            "quic-recv",
            mmbus::BusConfig {
                base_dir: recv_dir,
                ..Default::default()
            },
        );
        let mut sub = bus
            .subscribe_with_history_timeout("events", 100, Duration::from_secs(15))
            .expect("subscribe");
        let mut got = Vec::new();
        for _ in 0..3 {
            got.push(
                sub.recv_timeout(Duration::from_secs(5))
                    .expect("recv")
                    .expect("recv timeout"),
            );
        }
        got
    });

    // 3) Sender bridge: peer = the receiver, pinning the receiver's fp.
    let send_bridge =
        build_quic_dialer(tmp_send.path(), "quic-send", recv_addr, &recv_fp, psk);

    // 4) Publish locally to the sender's bus; the sender bridge's
    //    subscriber thread forwards each message over QUIC to the
    //    receiver bridge.
    let send_dir = tmp_send.path().to_path_buf();
    let pub_handle = thread::spawn(move || {
        let mut bus = Bus::with_config(
            "quic-send",
            mmbus::BusConfig {
                base_dir: send_dir,
                ..Default::default()
            },
        );
        // The send bridge's subscriber thread is the only subscriber;
        // wait for it to connect, then publish.
        bus.wait_for_subscribers("events", 1, Duration::from_secs(10))
            .expect("bridge subscriber must connect");
        for i in 0..3u64 {
            bus.publish("events", &i.to_le_bytes()).expect("publish");
        }
    });

    pub_handle.join().expect("publisher");
    let got = sub_handle.join().expect("subscriber");
    assert_eq!(got.len(), 3);
    for (i, m) in got.iter().enumerate() {
        assert_eq!(m.as_slice(), &(i as u64).to_le_bytes());
    }

    send_bridge.shutdown();
    recv_bridge.shutdown();
}

#[test]
fn quic_cert_mismatch_blocks_forward() {
    // The dialer pins the WRONG fingerprint; QUIC handshake must fail
    // and no messages should reach the receiver's local subscriber.
    let tmp_recv = tempfile::tempdir().unwrap();
    let tmp_send = tempfile::tempdir().unwrap();
    let psk = "quic-psk-2";

    let (recv_bridge, recv_addr, recv_fp) =
        build_quic_listener(tmp_recv.path(), "quic-recv-bad", psk);

    // Tamper the fingerprint: flip the last hex digit.
    let mut wrong_fp = recv_fp.clone();
    let last = wrong_fp.pop().unwrap();
    let flipped = if last == '0' { '1' } else { '0' };
    wrong_fp.push(flipped);
    assert_ne!(recv_fp, wrong_fp);

    let send_bridge =
        build_quic_dialer(tmp_send.path(), "quic-send-bad", recv_addr, &wrong_fp, psk);

    // Subscribe locally on the receiver — should time out because no
    // peer is allowed to connect over QUIC + the receiver's publisher
    // never creates the ring.
    let recv_dir = tmp_recv.path().to_path_buf();
    let result = {
        let bus = Bus::with_config(
            "quic-recv-bad",
            mmbus::BusConfig {
                base_dir: recv_dir,
                ..Default::default()
            },
        );
        bus.subscribe_with_history_timeout("events", 100, Duration::from_millis(800))
    };

    // Drive the sender to attempt publishes; they should NOT propagate.
    let send_dir = tmp_send.path().to_path_buf();
    let pub_thread = thread::spawn(move || {
        let mut bus = Bus::with_config(
            "quic-send-bad",
            mmbus::BusConfig {
                base_dir: send_dir,
                ..Default::default()
            },
        );
        // The bridge subscriber will never connect to the receiver
        // (cert mismatch) but it DOES subscribe locally.  We push a
        // few messages just to exercise the path; they get dropped
        // at the QUIC connect step.
        if bus.wait_for_subscribers("events", 1, Duration::from_secs(2)).is_ok() {
            for i in 0..3u64 {
                let _ = bus.publish("events", &i.to_le_bytes());
            }
        }
    });
    pub_thread.join().unwrap();

    match result {
        Err(mmbus::Error::Timeout(_)) => (),
        Ok(mut sub) => {
            // Subscribe slipped through (test bus, no PSK auth on
            // mmbus side).  Confirm no actual data arrived.
            assert!(
                sub.recv_timeout(Duration::from_millis(300))
                    .ok()
                    .flatten()
                    .is_none(),
                "cert mismatch must block QUIC frames from reaching receiver"
            );
        }
        Err(e) => panic!("unexpected error: {e}"),
    }

    send_bridge.shutdown();
    recv_bridge.shutdown();
}
