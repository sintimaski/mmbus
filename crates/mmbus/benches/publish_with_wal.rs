//! Publisher hot-path bench: WAL on vs WAL off.
//!
//! Compares `Publisher::publish` throughput for a 32 B payload across:
//!
//!   * baseline — `BusConfig` with WAL disabled (the v0.1.0 path)
//!   * WAL=Each — fsync inline per publish
//!   * WAL=Batched — append + background flusher
//!   * WAL=None — append only, no fsync
//!
//! Run with `cargo bench --bench publish_with_wal`.  Criterion stores
//! baselines in `target/criterion/`; the percentage delta vs the
//! disabled path is the W1-f gate (<10% regression for the chosen
//! default policy).

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use mmbus::wal::{FsyncPolicy, WalConfig};
use mmbus::{BusConfig, Publisher};
use std::path::PathBuf;
use std::time::Duration;

const BASE: &str = "/tmp/mmbus_bench_wal";
const PAYLOAD: &[u8] = &[0xABu8; 32];

fn cfg(name: &str, wal: WalConfig) -> BusConfig {
    let dir = PathBuf::from(BASE).join(name);
    let _ = std::fs::remove_dir_all(&dir);
    BusConfig {
        capacity: 4096,
        slot_size: 64,
        base_dir: dir,
        wal,
        ..Default::default()
    }
}

fn make_publisher(name: &str, wal: WalConfig) -> Publisher {
    Publisher::create("bus", cfg(name, wal)).expect("publisher create")
}

fn publish_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("publish_with_wal");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(3));

    let configs: Vec<(&str, WalConfig)> = vec![
        ("baseline_no_wal", WalConfig::disabled()),
        (
            "wal_none",
            WalConfig {
                enabled: true,
                fsync_policy: FsyncPolicy::None,
                ..Default::default()
            },
        ),
        (
            "wal_batched",
            WalConfig {
                enabled: true,
                fsync_policy: FsyncPolicy::Batched,
                ..Default::default()
            },
        ),
        (
            "wal_each",
            WalConfig {
                enabled: true,
                fsync_policy: FsyncPolicy::Each,
                ..Default::default()
            },
        ),
    ];

    for (label, wal) in configs {
        let mut p = make_publisher(label, wal);
        group.bench_with_input(BenchmarkId::new("policy", label), &(), |b, _| {
            b.iter(|| {
                // DropOldest publish never returns Full — keeps the
                // bench measuring the publish path, not backpressure.
                p.publish(PAYLOAD).unwrap();
            })
        });
    }

    group.finish();
}

criterion_group!(benches, publish_throughput);
criterion_main!(benches);
