/// End-to-end benchmarks: Publisher → Unix socket wakeup → Subscriber.
///
/// These catch regressions in the full pipeline: socket signaling overhead,
/// accept-loop latency, or wakeup byte accumulation bugs. Run with:
///
///   cargo bench --bench e2e
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use mmbus::{BusConfig, Publisher, Subscriber};
use std::hint::spin_loop;
use std::path::PathBuf;
use std::time::{Duration, Instant};

const BENCH_BASE: &str = "/tmp/mmbus_bench";

struct E2EBench {
    pub_: Publisher,
    // Channels to drive the consumer thread across Criterion samples.
    go_tx: std::sync::mpsc::SyncSender<usize>,
    done_rx: std::sync::mpsc::Receiver<()>,
}

fn setup_e2e(label: &str, slot_size: u32) -> E2EBench {
    let cfg = BusConfig {
        capacity: 4096,
        slot_size,
        base_dir: PathBuf::from(BENCH_BASE),
        ..Default::default()
    };
    let _ = std::fs::remove_file(cfg.base_dir.join(label).join("signal.sock"));

    let (go_tx, go_rx) = std::sync::mpsc::sync_channel::<usize>(1);
    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();

    let cfg_sub = cfg.clone();
    let label_owned = label.to_owned();
    std::thread::spawn(move || {
        let mut sub =
            Subscriber::connect(&label_owned, &cfg_sub, Duration::from_secs(10)).unwrap();
        loop {
            let n = match go_rx.recv() {
                Ok(n) => n,
                Err(_) => break,
            };
            for _ in 0..n {
                sub.receive().unwrap();
            }
            done_tx.send(()).unwrap();
        }
    });

    let mut pub_ = Publisher::create(label, cfg).unwrap();
    pub_.wait_for_subscribers(1, Duration::from_secs(10)).unwrap();

    E2EBench { pub_, go_tx, done_rx }
}

/// Throughput: messages per second across the full pipeline.
///
/// Regression signal: a drop in msg/s suggests socket write batching broke,
/// the accept loop acquired a lock in the hot path, or wakeup semantics changed.
fn e2e_throughput(c: &mut Criterion) {
    const BATCH: usize = 5_000;

    let mut group = c.benchmark_group("e2e_socket_throughput");
    group.throughput(Throughput::Elements(BATCH as u64));
    // Socket wakeup variance is higher than pure spin; give Criterion more
    // samples to stabilize the measurement.
    group.sample_size(20);

    for msg_size in [32usize, 256] {
        let label = format!("e2e_tput_{msg_size}b");
        let mut bench = setup_e2e(&label, msg_size as u32 + 64);
        let msg = vec![0u8; msg_size];

        group.bench_with_input(
            BenchmarkId::new("msg_size", msg_size),
            &msg_size,
            |b, _| {
                b.iter_custom(|iters| {
                    let n = iters as usize * BATCH;
                    bench.go_tx.send(n).unwrap();

                    let start = Instant::now();
                    let mut sent = 0;
                    while sent < n {
                        match bench.pub_.publish(&msg) {
                            Ok(()) => sent += 1,
                            Err(mmbus::Error::Full) => spin_loop(),
                            Err(e) => panic!("{e}"),
                        }
                    }
                    bench.done_rx.recv().unwrap();
                    start.elapsed()
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, e2e_throughput);
criterion_main!(benches);
