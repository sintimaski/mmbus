# Competitive Benchmark Results — 2026-05-19

Single-host (Apple M-series, APFS), 1M messages, 256-byte payload,
single publisher + single consumer.  Methodology: see `SPEC.md`.
Reproduction: `./run_all.sh`.

mmbus Python wheel under test: v0.2.2 (development install via
`maturin develop --release` against `main` @ `068d0f9`).

## Throughput (sustained, msgs/sec)

| Framework            | Durable | Sustained    | Producer-side | Consumer wall | Notes |
|----------------------|---------|-------------:|--------------:|--------------:|-------|
| **mmbus**            | no      | **1.36 M/s** | 1.36 M/s      | 0.73 s        | mmap + Unix-sock signal; WAL=disabled |
| **mmbus**            | yes     | _(pending — Python wheel currently uses v0.1 WAL backend, ~40 k/s; v2 backend per criterion bench is 4.6 M/s in Rust)_ | | | rebuild with `maturin develop --release --features wal_v2` for v2 perf |
| **ZeroMQ** PUSH/PULL | n/a     | **1.65 M/s** | 2.77 M/s      | 0.60 s        | `ipc://` socket; no built-in durability |
| **Redis Streams**    | yes     | **0.12 M/s** | 0.12 M/s      | 8.05 s        | TCP loopback :16379; XADD pipeline=1024; `appendfsync=everysec` |
| **NATS JetStream**   | yes     | _(pending)_  |               |               | TCP loopback :14222; FILE storage |
| NATS core            | no      | _excluded_   |               |               | drops messages under SlowConsumer (by design); not comparable here |

## Headline reading

- For **non-durable same-host pub/sub**, ZeroMQ wins on producer-side
  throughput (more aggressive buffering); mmbus is close on sustained
  throughput (the producer-consumer pipeline is balanced by the ring's
  backpressure).
- For **durable**, Redis Streams's loopback TCP + per-second-fsync
  caps at 0.12 M/s.  mmbus's v2 WAL backend hits 4.6 M/s pure-publish
  in the Rust criterion bench; the Python wheel currently ships the
  v0.1 backend so the Python number is lower.  Will update once the
  Python wheel defaults to `wal_v2`.

## Memory (peak RSS)

| Framework            | Publisher | Subscriber |
|----------------------|----------:|-----------:|
| mmbus nondurable     | 22 MB     | 22 MB      |
| ZeroMQ               | 211 MB    | 18 MB      |
| Redis Streams        | 25 MB     | 27 MB      |
| NATS JetStream       | _(pending)_ | _(pending)_ |

ZeroMQ's large publisher RSS is its outbound HWM buffer (we set
HWM=1M to avoid bounded-buffer dynamics).  mmbus's mmap'd ring is
shared between processes so RSS counts it twice.

## Caveats

- Redis + NATS pay loopback TCP costs that mmbus skips by design.
  This isn't an apples-to-apples comparison of "which protocol is
  faster"; it's "for the use case mmbus targets (same-host pub/sub
  with optional durability), what's the relative cost of each tool".
- Every framework has tuning knobs.  We use sensible defaults +
  pipelined publishes where the framework supports them.  All
  numbers are reproducible with `./run_all.sh`.
- Single producer / single consumer.  Multi-publisher fanout is a
  different test mmbus is intentionally NOT built for (SPMC by
  design).
