/// Run this in one terminal, then run `cargo run --example consumer` in another.
use mmbus::Bus;
use std::time::Instant;

fn main() {
    let mut bus = Bus::new("demo");
    println!("Producer ready. Start consumer now (has 30s to connect).");

    // The first publish creates the ring + socket for this topic.
    // The subscriber retries connecting automatically on its side.
    let n = 1_000_000usize;
    let msg = b"hello from mmbus!";
    let mut drops = 0usize;
    let mut sent = 0usize;

    // Small sleep so the consumer can connect before we flood the ring.
    std::thread::sleep(std::time::Duration::from_millis(100));

    let start = Instant::now();
    while sent < n {
        match bus.publish("frames", msg) {
            Ok(()) => sent += 1,
            Err(mmbus::Error::Full) => {
                drops += 1;
                std::hint::spin_loop();
            }
            Err(e) => panic!("{e}"),
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
