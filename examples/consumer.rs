/// Run `cargo run --example producer` first in another terminal.
use mmbus::Bus;
use std::time::Instant;

fn main() {
    let bus = Bus::new("demo");
    println!("Connecting to demo/frames (waiting up to 30s for producer)...");

    let mut sub = bus.subscribe("frames").expect("connect failed");
    println!("Connected. Receiving 1,000,000 messages...");

    let n = 1_000_000usize;
    let start = Instant::now();
    let mut first: Option<Vec<u8>> = None;

    for (i, msg) in sub.by_ref().take(n).enumerate() {
        let msg = msg.expect("receive error");
        if i == 0 {
            first = Some(msg);
        }
    }

    let elapsed = start.elapsed();
    println!(
        "Received {n} messages in {:.3}s  |  {:.0} msg/s  |  avg {:.0} ns/msg",
        elapsed.as_secs_f64(),
        n as f64 / elapsed.as_secs_f64(),
        elapsed.as_nanos() as f64 / n as f64,
    );
    if let Some(msg) = first {
        println!("First message: {:?}", String::from_utf8_lossy(&msg));
    }
}
