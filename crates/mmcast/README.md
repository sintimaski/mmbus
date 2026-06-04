# mmbus-cast

> ASGI WebSocket broadcast on top of [mmbus](https://github.com/sintimaski/mmbus).
> No Redis container, no broker — same shape as
> [`encode/broadcaster`](https://github.com/encode/broadcaster),
> ~720 ns wakeup, replay on reconnect, presence baked in.

```python
from fastapi import FastAPI, WebSocket
from mmbus_cast.fastapi import broadcast_lifespan
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app):
    async with broadcast_lifespan("my-app", prepare=["chat"]) as bc:
        app.state.broadcast = bc
        yield

app = FastAPI(lifespan=lifespan)

@app.websocket("/ws")
async def ws(socket: WebSocket) -> None:
    await socket.accept()
    bc = socket.app.state.broadcast
    async with await bc.subscribe("chat", replay_last=20) as sub:
        async for event in sub:
            await socket.send_bytes(event.data)
```

## Why

Multi-worker FastAPI / Starlette / Django Channels apps that need
WebSocket broadcast across workers historically reach for Redis pub/sub
([`encode/broadcaster`](https://github.com/encode/broadcaster) is the
canonical shim).  That's two containers and a network hop for what is,
on a single machine, a mmap memcpy.

`mmbus-cast` is a thin async-Python wrapper over an mmbus ring buffer
that gives you that same `Broadcast` / `subscribe` / `publish` shape
with:

|                              | broadcaster + Redis | mmbus-cast              |
|------------------------------|---------------------|-------------------------|
| Containers (single-worker)   | 2                   | **1**                   |
| Wakeup transport             | Redis loopback TCP  | mmap + eventfd / AF_UNIX |
| Setup                        | `pip install` + run Redis | `pip install mmbus-cast` |
| Replay on reconnect          | Streams retrofit    | `replay_last=N` first-class |
| Presence                     | Roll your own       | `broadcast.presence(...)` |
| Cross-host                   | Native (cluster)    | `mmbus[bridge]`         |

For comparison numbers — broadcast latency, throughput, RSS, container
footprint — see [`examples/benchmark/`](examples/benchmark/).

## Install

```bash
pip install mmbus-cast            # base lib (pulls mmbus as a dep)
pip install mmbus-cast[fastapi]   # + FastAPI example deps
```

Wheels: Linux + macOS, Python 3.8 – 3.13.  Windows is gated on mmbus
landing its own Windows support.

## Five-minute tour

### Publish + subscribe

```python
from mmbus_cast import Broadcast

bc = Broadcast("my-app")
async with bc:
    await bc.publish("notify", b"hello")

    async with await bc.subscribe("notify") as sub:
        async for event in sub:
            print(event.data)        # b"hello"
            # event.cursor for advanced "resume from here" patterns
```

### Replay last N on reconnect

```python
async with await bc.subscribe("notify", replay_last=20) as sub:
    async for event in sub:
        ...  # the most recent 20 in-ring messages arrive first, then live
```

In-ring history only in v0.1 — best-effort, bounded by the mmbus ring
capacity.  Durable WAL replay lands in v0.2.

### Backpressure for slow WebSocket clients

```python
sub = await bc.subscribe(
    "feed",
    queue_depth=1024,                # per-consumer bound
    slow_policy="drop_oldest",       # default; or "drop_newest" / "disconnect"
)
print(sub.slow_count)                # bump every overflow
print(sub.delivered_count)
```

### Presence

```python
async with bc.presence("chat", member_id="alice",
                       ttl_secs=15.0, heartbeat_secs=5.0) as p:
    print(p.members)                 # snapshot: {"alice", ...}
    async for change in p:
        print(change.member, "joined" if change.joined else "left")
```

### FastAPI lifespan helper

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from mmbus_cast.fastapi import broadcast_lifespan

@asynccontextmanager
async def lifespan(app):
    async with broadcast_lifespan("my-app", prepare=["chat"]) as bc:
        app.state.broadcast = bc
        yield

app = FastAPI(lifespan=lifespan)
```

Full chat app: [`examples/fastapi_chat/`](examples/fastapi_chat/).

## Multi-worker FastAPI

mmbus enforces single-publisher-per-topic *across processes*.  For
`uvicorn --workers 4`, mmcast uses per-worker sharding: each worker
publishes to `chat.<worker_id>` and subscribes to all peers' shards.

```python
from mmbus_cast.fastapi import broadcast_lifespan, worker_shard_from_env

worker_id, peers = worker_shard_from_env(workers=4)
async with broadcast_lifespan(
    "my-app", worker_id=worker_id, peers=peers, prepare=["chat"]
) as bc:
    ...
```

The shard ID can also be set via `MMCAST_WORKER_ID` and `MMCAST_PEERS`
env vars — handy for systemd / supervisor configs.  See the chat
example's `README.md`.

## Cross-host

mmbus-cast is same-host by design — that's where the speed comes
from.  For cross-host fan-out, install
[`mmbus[bridge]`](https://github.com/sintimaski/mmbus/tree/main/crates/mmbus-bridge);
the bridge republishes mmbus topics over TCP (or QUIC, with the
standalone binary), so every channel mmcast publishes to is
automatically federated to peer hosts.

## Status

`v0.1` — pre-release.  See
[`../../docs/spec-mmcast-v0.1.md`](../../docs/spec-mmcast-v0.1.md) for
the contract, including the four locked decisions and the
encode/broadcaster parity audit.

## License

MIT.
