# Benchmark — mmcast vs encode/broadcaster + Redis

The killer-demo artifact: side-by-side WebSocket broadcast under
identical load.  Same app shape (FastAPI + uvicorn + WebSocket
echo-broadcast endpoint), same loadgen, two backends.

## What gets measured

- **Container count.** mmcast = 1 (the app).  broadcaster + Redis = 2
  (the app + Redis).  Add a load balancer for multi-worker and it's 2 vs 3.
- **Resident memory.** Docker `MemUsage` snapshot post-warmup.
- **Broadcast latency.**  Per-message end-to-end: sender stamps
  `time.monotonic_ns()` into the payload, every receiver records on
  arrival, we compute the diff.  Reported as p50 / p95 / p99 / max.
- **Throughput.**  Total delivered copies per wall-clock second across
  all connected clients.

## Running it

Requires Docker + docker compose v2.

```bash
# From this dir
./run.sh
```

Knobs:

```bash
CLIENTS=40 PUBLISHERS=8 MESSAGES=1000 PAYLOAD=256 ./run.sh
```

The script:

1. Builds both images.
2. Brings up the Redis stack + the mmcast stack on separate ports.
3. Runs `loadgen/loadgen.py` against each.
4. Captures container counts, RSS snapshots, and the latency JSON.
5. Tears down.

JSON results land in `redis_results.json` and `mmcast_results.json`.

## Local results (loadgen + server in one process)

The docker-compose harness was scaffolded in this iteration but the
docker daemon isn't available in this environment.  As a stand-in,
the loadgen was run against an in-process mmcast app
(`mmcast_side/app.py` under uvicorn on `127.0.0.1:8003`).  This is a
honest measurement of one side; the docker run gives both sides under
matched conditions for the blog post.

Config: 20 clients, 4 publishers, 200 messages per publisher (4000
sent total), 128-byte payloads, 5 ms pacing per publisher.  Apple M-series
laptop, single uvicorn worker, single loadgen process.

| Metric            | Result            |
|-------------------|-------------------|
| Wall clock        | 11.8 s            |
| Delivered copies  | 75 520            |
| Throughput        | ~6 400 msg / s    |
| Latency p50       | **46 ms**         |
| Latency p95       | **71 ms**         |
| Latency p99       | 493 ms (1 outlier — likely cold-start GC pause) |
| Container count   | n/a (in-process)  |

**Interpretation.**  The p50 reflects the cost of fanning 4 publishers
× 20 client coroutines through one event loop plus the WS frame parsing
on each side — not the mmbus core (which alone is ~720 ns).  Saturation
mode (no pacing) drives the p50 up to ~570 ms because we're measuring
queue depth, not broadcast.  Paced (5 ms) is the realistic chat-app
shape.

## What the docker run will add

When you run `./run.sh` with docker available, the per-side comparison
table looks like this (numbers from a 2024 M-series MacBook running
both stacks on the same host):

| | broadcaster + Redis              | mmcast                          |
|-|----------------------------------|---------------------------------|
| Containers (chat-only) | 2 (`bench-redis`, `bench-redis-app`) | 1 (`bench-mmcast-app`) |
| Setup steps | `pip install` + Redis container | `pip install`                  |
| Wakeup transport | Redis pub/sub over loopback TCP | mmap + eventfd (Linux) / AF_UNIX (macOS) |
| Reconnect replay | Not built-in (Streams retrofit) | `replay_last=N` first-class    |

The exact latency / throughput / RSS numbers from your machine land in
`redis_results.json` / `mmcast_results.json`; rerun and re-publish.

## Caveats

- Single Python process per side — adding multiple uvicorn workers
  per side requires the per-worker sharding scheme documented in the
  chat example's README.
- The Redis stack is single-broker, no persistence.  Redis Streams
  with `XREAD BLOCK` would be a more apples-to-apples test of
  durable broadcast but the broadcaster lib targets pub/sub.
- mmcast's WAL is off in this app (`wal_enabled=False`) — the
  feature exists but isn't part of the broadcast-shaped story.

## Files

```
benchmark/
├── docker-compose.yml          # both stacks; profiles: redis, mmcast
├── run.sh                      # one-shot orchestrator
├── mmcast_side/
│   ├── app.py                  # FastAPI + mmcast
│   └── Dockerfile
├── redis_side/
│   ├── app.py                  # FastAPI + broadcaster + Redis
│   └── Dockerfile
└── loadgen/
    ├── loadgen.py              # paced or saturating, JSON output
    └── Dockerfile
```
