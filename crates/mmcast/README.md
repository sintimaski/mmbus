# mmbus-cast

> ASGI WebSocket broadcast on top of [mmbus](https://github.com/sintimaski/mmbus).
> No Redis container, no broker — same shape as `encode/broadcaster`,
> ~720 ns wakeup latency, replay on reconnect.

```python
from fastapi import FastAPI, WebSocket
from mmbus_cast import Broadcast

app = FastAPI()
broadcast = Broadcast("my-app")

@app.on_event("startup")
async def startup() -> None:
    await broadcast.__aenter__()

@app.on_event("shutdown")
async def shutdown() -> None:
    await broadcast.__aexit__(None, None, None)

@app.websocket("/ws")
async def ws(socket: WebSocket) -> None:
    await socket.accept()
    async with broadcast.subscribe("chat", replay_last=20) as sub:
        async for event in sub:
            await socket.send_bytes(event.data)
```

## Why

If you're running `uvicorn --workers 4` and need to broadcast to
WebSocket connections across workers, today's options are:

| Option                          | Verdict                                             |
|---------------------------------|-----------------------------------------------------|
| `encode/broadcaster` + Redis    | Works.  Now you run Redis.                          |
| Django Channels + Redis layer   | Works.  Now you run Redis *and* Django Channels.    |
| Roll your own with `multiprocessing.Queue` | Doesn't actually broadcast across workers.   |
| **mmbus-cast**                  | One pip install, no daemon, replay on reconnect.    |

## Install

```bash
pip install mmbus-cast              # base
pip install mmbus-cast[fastapi]     # + FastAPI example deps
```

## Status

`v0.1.0` — pre-release, in active development.  See
[`../../docs/spec-mmcast-v0.1.md`](../../docs/spec-mmcast-v0.1.md) for
the contract.

## Cross-host

`mmbus-cast` is same-host by design (that's where the speed comes
from).  For cross-host fan-out, install `mmbus[bridge]` and run the
in-process bridge — every mmbus topic mmcast publishes to is
automatically federated.  See
[`../mmbus-bridge/README.md`](../mmbus-bridge/README.md).

## License

MIT.
