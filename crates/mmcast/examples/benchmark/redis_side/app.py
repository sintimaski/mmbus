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


@app.websocket("/ws")
async def ws(socket: WebSocket) -> None:
    await socket.accept()
    bc = socket.app.state.broadcast

    async def push_out() -> None:
        try:
            async with bc.subscribe(channel=CHANNEL) as sub:
                async for event in sub:
                    # broadcaster delivers Event.message as str — encode
                    # to keep the payload comparable to mmcast's bytes path.
                    await socket.send_bytes(event.message.encode()
                        if isinstance(event.message, str) else event.message)
        except (WebSocketDisconnect, RuntimeError):
            pass

    async def pull_in() -> None:
        try:
            while True:
                data = await socket.receive_bytes()
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
