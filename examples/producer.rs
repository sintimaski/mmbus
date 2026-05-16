/// Run this in one terminal, then run `cargo run --example consumer` in another.
use mmbus::{BusConfig, Publisher};
use std::time::{Duration, Instant};

fn main() {
    let cfg = BusConfig {
        capacity: 1024,
        slot_size: 256,
        ..Default::default()
    };

    let mut pub_ = Publisher::create("demo", cfg).expect("create bus");
    println!("Producer ready at /tmp/mmbus/demo — start consumer now.");
    println!("Waiting up to 30s for a subscriber to connect...");

    pub_
        .wait_for_subscribers(1, Duration::from_secs(30))
        .expect("no subscriber connected within 30s");
    println!("Subscriber connected. Sending 1,000,000 messages...");

    let n = 1_000_000usize;
    let msg = b"hello from mmbus!";
    let mut drops = 0usize;
    let mut sent = 0usize;

    let start = Instant::now();
    while sent < n {
        match pub_.publish(msg) {
            Ok(()) => sent += 1,
            Err(mmbus::Error::Full) => {
                drops += 1;
                std::hint::spin_loop();
            }
            Err(e) => panic!("publish error: {e}"),
        }
    }

    let elapsed = start.elapsed();
    println!(
        "Sent {n} messages in {:.3}s  |  {:.0} msg/s  |  avg {:.0} ns/msg  |  drops: {drops}",
        elapsed.as_secs_f64(),
        n as f64 / elapsed.as_secs_f64(),
        elapsed.as_nanos() as f64 / n as f64,
    );
}
