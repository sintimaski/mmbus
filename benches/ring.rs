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
const MAX_SUBS: u32 = 4;

fn setup_ring(name: &str, capacity: u32, slot_size: u32) -> Arc<RingBuffer> {
    let dir = PathBuf::from(BENCH_BASE).join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    Arc::new(RingBuffer::create(&dir.join("ring.mmap"), capacity, slot_size, MAX_SUBS).unwrap())
}

/// Throughput: how many messages per second can flow through the ring buffer
/// when producer and consumer run on separate threads with no socket overhead.
///
/// Regression signal: throughput drop > 10% suggests a memory-ordering
/// regression (e.g., SeqCst instead of Acquire/Release) or a ring math bug.
fn ring_throughput(c: &mut Criterion) {
    const CAPACITY: u32 = 4096;
    const BATCH: usize = 50_000;

    let mut group = c.benchmark_group("ring_throughput");
    group.throughput(Throughput::Elements(BATCH as u64));

    for msg_size in [32usize, 256, 1024] {
        let ring = setup_ring(&format!("throughput_{msg_size}b"), CAPACITY, msg_size as u32 + 64);
        let ring_c = Arc::clone(&ring);

        let msg = vec![0xABu8; msg_size];

        // Claim a cursor for the consumer thread before spawning it so the
        // producer's backpressure check sees our position immediately.
        let cursor_start = ring.current_tail();
        let cursor_idx = ring.claim_cursor(cursor_start).unwrap();

        let (go_tx, go_rx) = std::sync::mpsc::sync_channel::<usize>(1);
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();

        std::thread::spawn(move || {
            let mut cursor = cursor_start;
            let mut buf = Vec::with_capacity(1024);
            while let Ok(n) = go_rx.recv() {
                let mut received = 0;
                while received < n {
                    if let Some(new_cursor) = ring_c.try_receive(cursor_idx, cursor, &mut buf) {
                        cursor = new_cursor;
                        received += 1;
                    } else {
                        spin_loop();
                    }
                }
                done_tx.send(()).unwrap();
            }
            ring_c.release_cursor(cursor_idx);
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
/// Producer and consumer alternate on the same core — lower bound on ring overhead.
///
/// Regression signal: ns/iter increase > 20% suggests overhead added to
/// try_publish or try_receive (unnecessary allocations, extra fences, etc.).
fn ring_single_msg_latency(c: &mut Criterion) {
    const CAPACITY: u32 = 256;
    const SLOT_SIZE: u32 = 256;

    let ring = setup_ring("latency", CAPACITY, SLOT_SIZE);
    let cursor_start = ring.current_tail();
    let cursor_idx = ring.claim_cursor(cursor_start).unwrap();
    let mut cursor = cursor_start;
    let msg = b"ping";
    let mut buf = Vec::with_capacity(256);

    let mut group = c.benchmark_group("ring_single_msg_latency");
    group.throughput(Throughput::Elements(1));

    group.bench_function("sequential_roundtrip", |b| {
        b.iter(|| {
            while !ring.try_publish(msg) {
                spin_loop();
            }
            loop {
                if let Some(new_cursor) = ring.try_receive(cursor_idx, cursor, &mut buf) {
                    cursor = new_cursor;
                    break;
                }
                spin_loop();
            }
        })
    });

    ring.release_cursor(cursor_idx);
    group.finish();
}

criterion_group!(benches, ring_throughput, ring_single_msg_latency);
criterion_main!(benches);
