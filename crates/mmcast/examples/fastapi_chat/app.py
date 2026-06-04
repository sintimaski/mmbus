"""FastAPI WebSocket chat — same shape as encode/broadcaster's example,
backed by mmbus instead of Redis.

Run it::

    pip install mmbus-cast[fastapi]
    uvicorn examples.fastapi_chat.app:app --workers 1 --port 8000

Then open http://localhost:8000 in two browser tabs.  Messages typed in
either tab show up in both.

Multi-worker (>1) requires per-worker sharding — set ``MMCAST_PEERS``
to a comma-separated list of worker IDs the deployment runs.  The
shipped example assumes single-worker for simplicity; see the README
for the multi-worker pattern.
"""
from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from mmbus_cast.fastapi import broadcast_lifespan, worker_shard_from_env


CHANNEL = "chat"


@asynccontextmanager
async def lifespan(app: FastAPI):
    worker_id, peers = worker_shard_from_env()
    # Single-process default: one worker, peers=[self].  Multi-worker
    # users set MMCAST_PEERS in the deployment env.
    multi_worker = len(peers) > 1
    async with broadcast_lifespan(
        "mmcast-chat-demo",
        worker_id=worker_id if multi_worker else None,
        peers=peers if multi_worker else None,
        prepare=[CHANNEL],
        # The chat ring needs to be large enough to absorb burst traffic
        # without backpressuring slow WS clients.  256 slots × 4 KiB = 1 MiB.
        capacity=256,
        slot_size=4096,
        # WAL gives durable replay across publisher restarts.  Off here
        # for the demo (we don't need durability for a chat reload toy),
        # but flip to True for production.
        wal_enabled=False,
    ) as bc:
        app.state.broadcast = bc
        yield


app = FastAPI(lifespan=lifespan)


INDEX_HTML = """\
<!doctype html>
<title>mmcast chat</title>
<style>
  body { font-family: system-ui, sans-serif; max-width: 32rem; margin: 2rem auto; }
  #log { list-style: none; padding: 0; }
  #log li { padding: .25rem .5rem; border-bottom: 1px solid #eee; }
  input[type=text] { width: 80%; padding: .5rem; }
  button { padding: .5rem 1rem; }
</style>
<h1>mmcast chat</h1>
<p>Open another tab — messages broadcast across all sessions on this worker.
   Multi-worker: see README.</p>
<ul id="log"></ul>
<form id="form">
  <input type="text" id="msg" placeholder="say something" autofocus />
  <button>send</button>
</form>
<script>
  const ws = new WebSocket(`ws://${location.host}/ws`);
  const log = document.getElementById("log");
  ws.onmessage = (e) => {
    const li = document.createElement("li");
    li.textContent = e.data;
    log.appendChild(li);
  };
  document.getElementById("form").addEventListener("submit", (e) => {
    e.preventDefault();
    const msg = document.getElementById("msg");
    if (msg.value) { ws.send(msg.value); msg.value = ""; }
  });
</script>
"""


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return INDEX_HTML


@app.websocket("/ws")
async def ws(socket: WebSocket) -> None:
    await socket.accept()
    bc = socket.app.state.broadcast

    async def push_outbound(sub) -> None:
        """Forward broadcast messages → this client."""
        try:
            async for event in sub:
                await socket.send_text(event.data.decode())
        except (WebSocketDisconnect, RuntimeError):
            pass

    async def pull_inbound() -> None:
        """Forward this client's typed messages → broadcast."""
        try:
            while True:
                msg = await socket.receive_text()
                await bc.publish(CHANNEL, msg.encode())
        except WebSocketDisconnect:
            pass

    # Replay the last 20 messages on join so the new tab has context.
    async with await bc.subscribe(CHANNEL, replay_last=20) as sub:
        out_task = asyncio.create_task(push_outbound(sub))
        in_task = asyncio.create_task(pull_inbound())
        done, pending = await asyncio.wait(
            {out_task, in_task}, return_when=asyncio.FIRST_COMPLETED
        )
        for t in pending:
            t.cancel()
