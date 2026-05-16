/// Ring buffer micro-benchmarks — no socket, pure spin-wait.
///
/// These catch regressions in the ring buffer itself: atomic ordering changes,
/// slot math errors, or mmap access regressions. Run with:
///
///   cargo bench --bench ring
///
/// Criterion stores baselines in `target/criterion/`; re-run after changes
/// to compare.
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use mmbus::RingBuffer;
use std::hint::spin_loop;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

const BENCH_BASE: &str = "/tmp/mmbus_bench";

fn setup_ring(name: &str, capacity: u32, slot_size: u32) -> Arc<RingBuffer> {
    let dir = PathBuf::from(BENCH_BASE).join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    Arc::new(RingBuffer::create(&dir.join("ring.mmap"), capacity, slot_size).unwrap())
}

/// Throughput: how many messages per second can flow through the ring buffer
/// when producer and consumer run on separate threads with no socket overhead.
///
/// Regression signal: throughput drop > 10% suggests a memory-ordering
/// regression (e.g., SeqCst instead of Acquire/Release) or a ring math bug.
fn ring_spsc_throughput(c: &mut Criterion) {
    const CAPACITY: u32 = 4096;
    const BATCH: usize = 50_000;

    let mut group = c.benchmark_group("ring_spsc_throughput");
    group.throughput(Throughput::Elements(BATCH as u64));

    for msg_size in [32usize, 256, 1024] {
        let ring = setup_ring(&format!("throughput_{msg_size}b"), CAPACITY, msg_size as u32 + 64);
        let ring_c = Arc::clone(&ring);

        let msg = vec![0xABu8; msg_size];

        // Channels for coordinating the consumer thread across Criterion samples.
        let (go_tx, go_rx) = std::sync::mpsc::sync_channel::<usize>(1);
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();

        std::thread::spawn(move || {
            let mut cursor = ring_c.current_tail();
            let mut buf = Vec::with_capacity(1024);
            loop {
                let n = match go_rx.recv() {
                    Ok(n) => n,
                    Err(_) => break,
                };
                let mut received = 0;
                while received < n {
                    if ring_c.try_receive(cursor, &mut buf) {
                        ring_c.advance_head(cursor + 1);
                        cursor += 1;
                        received += 1;
                    } else {
                        spin_loop();
                    }
                }
                done_tx.send(()).unwrap();
            }
        });

        group.bench_with_input(
            BenchmarkId::new("msg_size", msg_size),
            &msg_size,
            |b, _| {
                b.iter_custom(|iters| {
                    let n = iters as usize * BATCH;
                    go_tx.send(n).unwrap();

                    let start = Instant::now();
                    let mut sent = 0;
                    while sent < n {
                        if ring.try_publish(&msg) {
                            sent += 1;
                        } else {
                            spin_loop();
                        }
                    }
                    done_rx.recv().unwrap();
                    start.elapsed()
                })
            },
        );
    }

    group.finish();
}

/// Single-message round-trip latency on the ring buffer (sequential, one thread).
/// Producer and consumer alternate on the same core — the lower bound on
/// ring buffer operation overhead.
///
/// Regression signal: ns/iter increase > 20% suggests overhead added to
/// try_publish or try_receive (e.g., unnecessary allocations or fences).
fn ring_single_msg_latency(c: &mut Criterion) {
    const CAPACITY: u32 = 256;
    const SLOT_SIZE: u32 = 256;

    let ring = setup_ring("latency", CAPACITY, SLOT_SIZE);
    let mut cursor = ring.current_tail();
    let msg = b"ping";
    let mut buf = Vec::with_capacity(256);

    let mut group = c.benchmark_group("ring_single_msg_latency");
    group.throughput(Throughput::Elements(1));

    group.bench_function("sequential_roundtrip", |b| {
        b.iter(|| {
            // Publish one message.
            while !ring.try_publish(msg) {
                spin_loop();
            }
            // Receive it (same thread — simulates context-switch-free cost).
            while !ring.try_receive(cursor, &mut buf) {
                spin_loop();
            }
            ring.advance_head(cursor + 1);
            cursor += 1;
        })
    });

    group.finish();
}

criterion_group!(benches, ring_spsc_throughput, ring_single_msg_latency);
criterion_main!(benches);
