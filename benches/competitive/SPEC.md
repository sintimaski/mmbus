# Competitive Benchmark Workload Spec

One workload, four implementations, same numbers — single-host
pub/sub with optional durability.  Numbers go in
`docs/benchmarks-vs-competition.md`.

## Setup

| Dimension              | Value                                          |
|------------------------|------------------------------------------------|
| Host                   | Single machine, no network noise               |
| Producer threads       | 1                                              |
| Consumer threads       | 1 (reading every message)                      |
| Payload size           | 256 bytes (typical small-msg shape)            |
| Message count          | 1,000,000                                      |
| Warmup messages        | 10,000 (discarded from timing)                 |
| Durability             | Run TWICE: durable=on, durable=off             |
| Topic / stream / queue | Fresh per run (no carryover)                   |

## Frameworks

| Framework      | Transport        | Durable mode used                     |
|----------------|------------------|---------------------------------------|
| mmbus          | mmap ring + WAL  | `WalConfig::default()` (Batched)      |
| ZeroMQ         | `ipc://` socket  | No durable mode — durable=off only    |
| Redis Streams  | TCP loopback     | `appendonly yes appendfsync everysec` |
| NATS JetStream | TCP loopback     | `FILE` storage, `WorkQueue` policy    |

ZeroMQ has no built-in durability so its column is N/A for the
durable row.  We list it anyway as the "what's the fastest local
non-durable IPC option" baseline.

## Metrics

Each run reports:

1. **Sustained throughput** (msgs/sec) — `N / wall_time`, where
   `wall_time` is from "first warmup message sent" to "last
   non-warmup message received by the consumer".
2. **Producer-side throughput** (msgs/sec) — `N / publisher wall time`.
   Captures backpressure: if the consumer is slower than the
   producer, the producer rate is bounded by the consumer.
3. **Consumer wall-time** (seconds).
4. **Peak RSS** of both processes (rough, via `getrusage`).

## What we're NOT measuring

* **Multi-publisher fanout** — mmbus is SPMC by design; not a
  fair comparison.
* **Cross-host** — mmbus is local-only.  The bridge (separate
  crate) does cross-host but isn't this benchmark's subject.
* **Latency percentiles** — separate exercise; this is the
  throughput sweep.
* **Multi-tenant noise / cloud network jitter** — we run
  everything on the same idle dev machine for reproducibility.

## Caveats to document in the writeup

* Redis + NATS pay loopback TCP costs that mmbus skips by
  design.  This isn't an apples-to-apples test of "which
  protocol is faster" — it's an apples-to-apples test of "for
  the use case mmbus targets, what's the relative cost of
  picking each tool."
* All frameworks have knobs.  We use their default + their
  recommended-durable config.  Anyone can tune further; the
  point of the table is to show ROUGH magnitudes.

## Reproduction

```
cd benches/competitive
./run_all.sh
# Results land in results.json + are appended to RESULTS.md
```

Requires Docker (Redis + NATS run as containers), Python 3.11+,
and an installed mmbus wheel.
