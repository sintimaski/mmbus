"""mmcast side of the benchmark — single-worker uvicorn FastAPI app.

Pared down from the chat example: just `/ws` echo-broadcast.  No HTML.
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import asyncio

from mmbus_cast.fastapi import broadcast_lifespan


CHANNEL = "bench"
BUS_NAME = os.environ.get("MMCAST_BUS_NAME", "mmcast-bench")
BUS_DIR = os.environ.get("MMCAST_BUS_DIR", "/tmp/mmcast-bench")


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with broadcast_lifespan(
        BUS_NAME,
        base_dir=BUS_DIR,
        prepare=[CHANNEL],
        capacity=4096,         # larger ring for sustained loadgen
        slot_size=4096,
        wal_enabled=False,
    ) as bc:
        app.state.broadcast = bc
        yield


app = FastAPI(lifespan=lifespan)


@app.websocket("/ws")
async def ws(socket: WebSocket) -> None:
    await socket.accept()
    bc = socket.app.state.broadcast

    async def push_out(sub) -> None:
        try:
            async for ev in sub:
                await socket.send_bytes(ev.data)
        except (WebSocketDisconnect, RuntimeError):
            pass

    async def pull_in() -> None:
        try:
            while True:
                data = await socket.receive_bytes()
                await bc.publish(CHANNEL, data)
        except WebSocketDisconnect:
            pass

    async with await bc.subscribe(CHANNEL, queue_depth=4096) as sub:
        out = asyncio.create_task(push_out(sub))
        inn = asyncio.create_task(pull_in())
        done, pending = await asyncio.wait(
            {out, inn}, return_when=asyncio.FIRST_COMPLETED
        )
        for t in pending:
            t.cancel()
