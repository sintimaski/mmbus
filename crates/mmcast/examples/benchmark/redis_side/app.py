"""Redis side of the benchmark — same FastAPI app, broadcaster + Redis.

Mirrors mmcast_side/app.py: one ``/ws`` endpoint, echo-broadcast.
"""
from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager

from broadcaster import Broadcast
from fastapi import FastAPI, WebSocket, WebSocketDisconnect


CHANNEL = "bench"
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379")


@asynccontextmanager
async def lifespan(app: FastAPI):
    bc = Broadcast(REDIS_URL)
    await bc.connect()
    app.state.broadcast = bc
    try:
        yield
    finally:
        await bc.disconnect()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health() -> dict:
    return {"ok": True}


@app.websocket("/ws")
async def ws(socket: WebSocket) -> None:
    await socket.accept()
    bc = socket.app.state.broadcast

    async def push_out() -> None:
        try:
            async with bc.subscribe(channel=CHANNEL) as sub:
                async for event in sub:
                    # broadcaster round-trips str.  We carry raw bytes over
                    # the WS as latin-1 (a total, byte-preserving codec) so
                    # the payload is byte-identical to what the loadgen sent
                    # — matching mmcast's bytes path.  Using the default
                    # UTF-8 here would corrupt header bytes >= 0x80.
                    msg = event.message
                    raw = msg.encode("latin-1") if isinstance(msg, str) else msg
                    await socket.send_bytes(raw)
        except (WebSocketDisconnect, RuntimeError):
            pass

    async def pull_in() -> None:
        try:
            while True:
                data = await socket.receive_bytes()
                # latin-1 is a total 1:1 byte<->codepoint map, so this
                # round-trips arbitrary bytes losslessly (see push_out).
                await bc.publish(channel=CHANNEL, message=data.decode("latin-1"))
        except WebSocketDisconnect:
            pass

    out = asyncio.create_task(push_out())
    inn = asyncio.create_task(pull_in())
    done, pending = await asyncio.wait(
        {out, inn}, return_when=asyncio.FIRST_COMPLETED
    )
    for t in pending:
        t.cancel()
