# Competitive Benchmark Results — 2026-05-19

Single-host (Apple M-series, APFS), 1M messages, 256-byte payload,
single publisher + single consumer.  Methodology: see `SPEC.md`.
Reproduction: `./run_all.sh`.

mmbus Python wheel under test: v0.2.3 (development install via
`maturin develop --release` — wheel ships with `wal_v2` enabled
by default as of v0.2.3).

## Throughput (sustained, msgs/sec)

| Framework            | Durable | Sustained    | Producer-side | Consumer wall | Notes |
|----------------------|---------|-------------:|--------------:|--------------:|-------|
| **mmbus**            | no      | **1.34 M/s** | 1.34 M/s      | 0.74 s        | mmap + Unix-sock signal; WAL disabled |
| **mmbus**            | yes     | **1.06 M/s** | 1.06 M/s      | 0.94 s        | mmap WAL v2; `Batched` fsync; ~26× v0.2.2 wheel (v0.1 backend) |
| **ZeroMQ** PUSH/PULL | n/a     | **1.65 M/s** | 2.77 M/s      | 0.60 s        | `ipc://` socket; no built-in durability |
| **Redis Streams**    | yes     | **0.12 M/s** | 0.12 M/s      | 8.05 s        | TCP loopback :16379; XADD pipeline=1024; `appendfsync=everysec` |
| **NATS JetStream**   | yes     | _(pending)_  |               |               | TCP loopback :14222; FILE storage — macOS Docker too slow to converge |
| NATS core            | no      | _excluded_   |               |               | drops messages under SlowConsumer (by design); not comparable here |

## Headline reading

- For **non-durable same-host pub/sub**, ZeroMQ wins on producer-side
  throughput (more aggressive buffering); mmbus is close on sustained
  throughput.
- For **durable, Python clients**, mmbus is **~8.8× faster than Redis
  Streams** (1.06 M/s vs 0.12 M/s) on the same workload.  Both are
  bounded by their Python clients' per-call overhead, but mmbus skips
  the loopback TCP + fsync stall that caps Redis.
- The Rust criterion bench (`cargo bench --bench publish_with_wal`)
  shows mmbus at **4.6 M/s pure-publish for durable**.  Python +
  PyO3 + GIL-drop costs cap the Python wheel at ~1.06 M/s.  For
  raw throughput-sensitive workloads, use the Rust API directly or
  `publish_many` to amortize PyO3 overhead.

## Memory (peak RSS)

| Framework            | Publisher | Subscriber |
|----------------------|----------:|-----------:|
| mmbus nondurable     | 51 MB     | 51 MB      |
| mmbus durable        | 116 MB    | 51 MB      |
| ZeroMQ               | 211 MB    | 18 MB      |
| Redis Streams        | 25 MB     | 27 MB      |
| NATS JetStream       | _(pending)_ | _(pending)_ |

ZeroMQ's large publisher RSS is its outbound HWM buffer (we set
HWM=1M to avoid bounded-buffer dynamics).  mmbus's mmap'd ring is
shared between processes so RSS counts it on both sides; the
durable publisher's extra ~65 MB is the WAL's first segment mmap'd
into the publisher's address space.

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
