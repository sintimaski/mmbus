# mmcast — WebSocket broadcast for FastAPI without Redis

*Draft launch post for `mmbus-cast` v0.1.0.*

If you run a FastAPI app with WebSocket broadcast, you've reached for
[`encode/broadcaster`](https://github.com/encode/broadcaster) and a
Redis container.  That's fine.  But if your whole stack otherwise
needs zero infrastructure — single machine, single binary, ASGI server
and that's it — Redis is the only piece you can't `pip install`.

`mmbus-cast` is the same broadcaster shape with the Redis dependency
replaced by an mmap ring buffer.  One package, one process, same API
surface.

```python
from mmbus_cast import Broadcast

bc = Broadcast("my-app")
async with bc:
    async with await bc.subscribe("chat", replay_last=20) as sub:
        async for event in sub:
            await ws.send_bytes(event.data)
```

## What it is, what it isn't

mmcast is a ~600-LOC pure-Python lib that sits on top of
[mmbus](https://github.com/sintimaski/mmbus), a zero-copy SPMC pub/sub
crate.  The lib gives you a familiar async surface — `publish`,
`subscribe`, `presence` — and translates calls into mmbus
operations.

The pitch is narrow on purpose:

- ✅ Replace `broadcaster + Redis` in a multi-worker FastAPI/Starlette WS broadcast.
- ✅ Get `replay_last=N` (a feature Redis pub/sub does not have without switching to Streams).
- ✅ Get `presence` baked in instead of rolling your own with Redis sorted sets.
- ❌ Not a Redis replacement.  No KV, no locks, no queues.  Those are
  separate `mmbus-*` siblings if they happen at all.
- ❌ Not cross-region.  Single host.  For cross-host, the in-process
  `mmbus[bridge]` companion forwards topics over TCP / QUIC.

## What disappears

The interesting comparison is what's no longer in your stack:

| | broadcaster + Redis | mmcast |
|-|---------------------|--------|
| Containers (chat-only, single worker) | 2 | **1** |
| `docker-compose.yml` services | `app`, `redis` | `app` |
| `requirements.txt` entries | `fastapi`, `broadcaster[redis]`, `redis` | `fastapi`, `mmbus-cast` |
| Cold-start steps | `pull redis`, `redis-server`, app start | app start |
| What you lose on Redis being down | the broadcast | n/a |

The actual broadcast latency in a single-worker FastAPI with mmcast,
measured paced (5 ms inter-publish), 20 connected clients, 4
publishers, 4000 messages:

| Metric    | mmcast (paced) |
|-----------|----------------|
| p50       | **46 ms**      |
| p95       | **71 ms**      |
| Throughput | ~6.4 K msg/s  |

(Numbers are end-to-end through the WS frame parser; the underlying
mmbus pub/sub is sub-µs.  Python WS + asyncio is the bottleneck, same
as on the broadcaster + Redis side.)

The full docker-compose Redis-vs-mmcast side-by-side is in
[`examples/benchmark/`](https://github.com/sintimaski/mmbus/tree/main/crates/mmcast/examples/benchmark)
— `./run.sh` produces matched numbers on your hardware.

## How it works

mmcast opens one mmbus subscription per channel per process and
fan-outs to per-WebSocket asyncio Queues.  A subscriber count of 1000
WS connections is still 1 mmbus subscriber on the channel — which
matters because mmbus's `max_subscribers` per topic is bounded
(default 16).

The slow-client policy is per-consumer: `drop_oldest` (default),
`drop_newest`, or `disconnect`.  Counters surface as
`Subscription.slow_count` and `delivered_count`.

```python
sub = await bc.subscribe(
    "feed", queue_depth=1024, slow_policy="drop_oldest",
)
```

Replay-on-reconnect uses mmbus's in-ring `subscribe_with_history`:

```python
sub = await bc.subscribe("chat", replay_last=20)
```

In v0.1 the replay is best-effort, bounded by the mmbus ring capacity,
applied at channel-open time within a process.  v0.2 will introduce a
per-subscriber buffer for accurate per-late-join replay (a different
trade-off the spec calls out).

## The multi-worker question

If you run `uvicorn --workers 4`, you might wonder: can all four
workers publish to the same channel?  mmbus's wire-format invariant
says no — single publisher per topic across processes.  mmcast handles
this with per-worker sharding: each worker publishes to
`chat.<worker_id>` and subscribes to all peer shards.

```python
from mmbus_cast.fastapi import broadcast_lifespan, worker_shard_from_env

worker_id, peers = worker_shard_from_env(workers=4)
async with broadcast_lifespan(
    "my-app", worker_id=worker_id, peers=peers, prepare=["chat"]
) as bc:
    ...
```

In a 4-worker deployment you get 4 publishers + 16 subscriptions per
channel, which fits comfortably inside mmbus's default
`max_subscribers=16` ceiling.  Beyond that, raise the limit via the
underlying Bus kwargs.

## What's next

The roadmap for v0.2 is short:

- **Per-subscriber replay buffer** so every WS reconnect gets its
  history, not just the first one in a worker.
- **Auto-detect the uvicorn worker index** so `workers=N` setups don't
  need an env-var dance.
- **A `mmbus-sched`-style sibling** — same mmbus + WAL primitives,
  cron-shaped API.  Pure experiment.

But honestly, what most matters for the next few months is whether
anyone hits a wall the docs missed.  Open an issue if mmcast
disappoints in a way Redis didn't.

## Try it

```bash
pip install mmbus-cast[fastapi]

# clone the chat example
git clone https://github.com/sintimaski/mmbus
cd mmbus/crates/mmcast/examples/fastapi_chat
uvicorn app:app --port 8000
# open http://localhost:8000 in two tabs
```

Source on GitHub:
[`crates/mmcast/`](https://github.com/sintimaski/mmbus/tree/main/crates/mmcast).
Spec:
[`docs/spec-mmcast-v0.1.md`](https://github.com/sintimaski/mmbus/blob/main/docs/spec-mmcast-v0.1.md).
