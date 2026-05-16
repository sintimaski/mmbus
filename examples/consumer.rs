/// Run `cargo run --example producer` first in another terminal.
use mmbus::{BusConfig, Subscriber};
use std::time::{Duration, Instant};

fn main() {
    let cfg = BusConfig {
        capacity: 1024,
        slot_size: 256,
        ..Default::default()
    };

    println!("Connecting to bus 'demo' (waiting up to 30s for producer)...");
    let mut sub = Subscriber::connect("demo", &cfg, Duration::from_secs(30))
        .expect("connect to bus");
    println!("Connected at cursor={}. Receiving 1,000,000 messages...", sub.cursor());

    let n = 1_000_000usize;
    let start = Instant::now();
    let mut first_msg: Option<Vec<u8>> = None;

    for i in 0..n {
        let msg = sub.receive().expect("receive error");
        if i == 0 {
            first_msg = Some(msg);
        }
    }

    let elapsed = start.elapsed();
    println!(
        "Received {n} messages in {:.3}s  |  {:.0} msg/s  |  avg {:.0} ns/msg",
        elapsed.as_secs_f64(),
        n as f64 / elapsed.as_secs_f64(),
        elapsed.as_nanos() as f64 / n as f64,
    );
    if let Some(msg) = first_msg {
        println!("First message: {:?}", String::from_utf8_lossy(&msg));
    }
}
