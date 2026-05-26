//! Publish to a topic + expose Prometheus metrics on
//! `http://127.0.0.1:9100/metrics`.
//!
//! Run with the `prometheus` feature on:
//!
//!   cargo run --example prometheus_exporter --features prometheus
//!
//! Then in another terminal:
//!
//!   curl -s http://127.0.0.1:9100/metrics | head -40
//!
//! Stop with Ctrl-C.

use mmbus::{Bus, BusConfig};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const BUS_NAME: &str = "prom-demo";
const TOPIC: &str = "events";

fn main() {
    let dir = std::env::temp_dir().join("mmbus-prom-demo");
    let _ = std::fs::remove_dir_all(&dir);
    // `Bus::publish` takes &mut self, so wrap in Mutex when sharing.
    // Real apps often keep one Bus per thread or use the lower-level
    // Publisher directly + an `Arc<RingStats>`-style snapshot for
    // metrics — both avoid the lock entirely on the publish hot path.
    let bus = Arc::new(Mutex::new(Bus::with_config(
        BUS_NAME,
        BusConfig {
            base_dir: dir,
            ..Default::default()
        },
    )));

    // Publisher loop — 10 messages/sec.
    let pub_bus = bus.clone();
    std::thread::spawn(move || {
        let mut i = 0u64;
        loop {
            let _ = pub_bus.lock().unwrap().publish(TOPIC, &i.to_le_bytes());
            i += 1;
            std::thread::sleep(Duration::from_millis(100));
        }
    });

    // Prometheus endpoint on the current thread.
    let addr: std::net::SocketAddr = "127.0.0.1:9100".parse().expect("addr");
    println!("Prometheus endpoint: http://{addr}/metrics");
    println!("Press Ctrl-C to stop.");
    let scrape_bus = bus.clone();
    mmbus::prometheus::serve_blocking(addr, move || {
        match scrape_bus.lock().unwrap().stats(TOPIC) {
            Some(stats) => mmbus::prometheus::render(TOPIC, &stats),
            None => String::from("# no publisher yet for topic\n"),
        }
    })
    .expect("serve");
}
