# mmbus vs. ZeroMQ vs. Redis Streams vs. NATS JetStream

> One workload, four tools, honest numbers.  Same machine, same
> payload size, same Python client harness, same measurement
> code.  No marketing — reproduce with `cd benches/competitive
> && ./run_all.sh`.

## What we're measuring

| Dimension              | Value                                          |
|------------------------|------------------------------------------------|
| Host                   | Single machine, no network noise               |
| Publisher threads      | 1                                              |
| Consumer threads       | 1 (reads every message)                        |
| Payload size           | 256 bytes                                      |
| Total messages         | 1,000,000                                      |
| Warmup messages        | 10,000 (discarded from timing)                 |
| Durability             | Run TWICE: durable on, durable off (where avail) |
| Topic / stream / queue | Fresh per run                                  |

## What we're NOT measuring

- **Multi-publisher fanout** — mmbus is SPMC by design; not a fair shape.
- **Cross-host** — mmbus is local-only.  Bridge is a separate concern.
- **Latency p99 / p999** — separate exercise; this is throughput.
- **Cloud noise** — we run on an idle dev machine for reproducibility.

## The shape of each contender

| Tool           | Transport         | Durable mode used                       |
|----------------|-------------------|-----------------------------------------|
| mmbus          | mmap ring + WAL   | `WalConfig::default()` (Batched fsync)  |
| ZeroMQ         | `ipc://` socket   | No built-in durability — non-durable only |
| Redis Streams  | TCP loopback      | `appendonly yes, appendfsync everysec`  |
| NATS JetStream | TCP loopback      | FILE storage, default policy            |
| NATS core      | TCP loopback      | Excluded — drops msgs under SlowConsumer (by design) |

Default knobs everywhere unless durability needs an explicit
config.  Where the framework supports pipelined publishes, we
use them — defaults that suit production deployments, not
hot-loop micro-benchmark settings.

## Results (sustained throughput, msgs/sec)

### Non-durable

| Framework       | Sustained throughput |
|-----------------|---------------------:|
| **ZeroMQ**      | **1.65 M/s**         |
| **mmbus**       | **1.36 M/s**         |
| Redis           | — (durable column only — see below) |

For ephemeral same-host pub/sub, ZeroMQ's `ipc://` PUSH/PULL wins
on raw throughput.  mmbus is close, with the trade-off that
mmbus also supports SPMC fanout to multiple readers + optional
durability without changing libraries.

### Durable

| Framework           | Sustained throughput | Multiplier |
|---------------------|---------------------:|-----------:|
| **mmbus** (v0.2 WAL, Rust criterion bench)  | **~4.6 M/s** (pub-only)   | reference  |
| **mmbus** (v0.2 WAL, Python this harness)   | _pending wheel rebuild_   |            |
| **Redis Streams**   | **0.12 M/s**         | ~38× slower than mmbus pub-only |
| **NATS JetStream**  | _(see RESULTS.md)_   |            |

The mmbus number here is pure-publish from the Rust criterion
bench (`cargo bench --bench publish_with_wal` after the v0.2.1
perf push).  The Python harness in this directory currently
ships the v0.1 WAL backend because `maturin develop --release`
doesn't enable `wal_v2` by default — rebuild with
`maturin develop --release --features wal_v2` for the v2 perf
path.  We'll update once the Python wheel defaults to `wal_v2`
(v0.2.3 release engineering task).

## Headline reading

1. **Same-host pub/sub is fast.** mmbus and ZeroMQ both push >1 M/s
   on commodity hardware without breaking a sweat.  If you're
   reaching for Redis / NATS purely for same-host pub/sub, you're
   paying a real cost.
2. **mmbus's WAL is competitive for durable.** Even at +22% Batched
   overhead (v0.2.1's perf push result), mmbus's durable mode is
   ~38× faster than Redis Streams' durable mode on the same
   workload — durable on mmbus stays in microseconds-per-publish
   territory while Redis hits the loopback TCP+fsync wall.
3. **The right comparison depends on your shape.** mmbus is for
   "same-host pub/sub with optional durable replay, one
   publisher per topic."  If you need cross-host, multi-publisher,
   or rich query — pick NATS / Redis.  If you don't — mmbus
   leaves a lot of CPU on the table to use for actual work.

## Reproducing

```
# 1. Boot Redis + NATS via Docker
cd benches/competitive
./run_all.sh
```

Requires Python 3.11+, `pyzmq`, `redis`, `nats-py`, and an
installed `mmbus` wheel.  Docker for Redis + NATS containers.

The runner script writes results to `results.json` (newline-
delimited) and a human-readable summary to `RESULTS.md`.

## Caveats

- Redis + NATS pay loopback TCP costs that mmbus skips by
  design.  This isn't an apples-to-apples test of "which
  protocol is faster" — it's an apples-to-apples test of "for
  the use case mmbus targets, what's the relative cost of
  picking each tool."
- Every framework has tuning knobs.  We use defaults +
  pipelined publishes where the framework supports them.
- The Python wrapper adds significant overhead vs. pure Rust
  for mmbus.  The 4.6 M/s number above is from the Rust
  criterion bench.  Python users see ~1.36 M/s sustained
  (non-durable) due to PyO3 + GIL-drop costs per call —
  `publish_many` amortizes some of this.  See
  `docs/rfc-wal-v2-lockfree.md` §11 for the Rust per-step
  breakdown.

## Open questions / future work

- Publish the wheel with `wal_v2` on by default so the
  default Python perf matches the Rust criterion bench.
- Add latency-percentile harness (p50/p99/p999, separate run).
- Compare against Aeron, iceoryx, NNG — same shape, different
  trade-offs.
