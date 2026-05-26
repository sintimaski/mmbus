"""FastAPI WebSocket broadcast over mmbus.

Each WebSocket connection gets its **own mmbus subscription** — fan-out
happens inside the ring buffer (SPMC cursor table), so the app code does
not maintain a set of active connections.  Publish once, every subscriber
sees it.

Setup::

    pip install fastapi 'uvicorn[standard]'

Run (single worker)::

    uvicorn examples.fastapi_broadcast:app

Demo three terminals:

* WS subscriber A::

      websocat ws://localhost:8000/ws

* WS subscriber B::

      websocat ws://localhost:8000/ws

* Publisher::

      curl -X POST http://localhost:8000/publish -d 'hello'

Both A and B receive ``hello`` simultaneously.
"""
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect

from mmbus import Bus

# `max_subscribers` caps the cursor table inside the mmap header — bump it
# if you expect many concurrent WS clients per process.
bus = Bus("fastapi-broadcast", max_subscribers=64)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # Warm up the publisher so WS subscribers don't have to wait for the
    # first POST to create it.  The empty message is never delivered:
    # subscribers connect with their cursor at the *current* tail.
    bus.publish("broadcast", b"")
    yield


app = FastAPI(lifespan=lifespan)


@app.post("/publish")
async def publish(req: Request) -> dict:
    payload = await req.body()
    bus.publish("broadcast", payload)
    return {"status": "published", "bytes": len(payload)}


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    await ws.accept()
    sub = await bus.subscribe_async("broadcast", timeout_secs=5.0)
    try:
        async with sub:
            async for msg in sub:
                await ws.send_bytes(msg)
    except WebSocketDisconnect:
        pass  # client went away; sub is released via async-with


@app.get("/")
async def index() -> dict:
    return {
        "endpoints": {
            "POST /publish": "publish bytes to the broadcast topic",
            "WS /ws": "receive every broadcast (one subscription per connection)",
        },
        "active_subscribers": bus.stats("broadcast").active_subscribers
        if bus.stats("broadcast")
        else 0,
    }
