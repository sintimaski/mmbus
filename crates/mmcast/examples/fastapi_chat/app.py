"""FastAPI WebSocket chat — same shape as encode/broadcaster's example,
backed by mmbus instead of Redis.

Run it::

    pip install mmbus-cast[fastapi]
    uvicorn examples.fastapi_chat.app:app --workers 1 --port 8000

Then open http://localhost:8000 in two browser tabs.  Messages typed in
either tab show up in both.

Multi-worker (>1) requires per-worker sharding — set ``MMCAST_WORKER_ID``
+ ``MMCAST_PEERS`` per worker (see the README).

Security note: a WebSocket endpoint is a public attack surface even
though the IPC behind it is private to the host.  This example shows two
of the controls a real deployment needs — an Origin allowlist and
per-message error handling — but it has **no authentication**.  Add auth
appropriate to your app before ``socket.accept()``.  Set
``MMCAST_ALLOWED_ORIGINS`` (comma-separated) to enforce the Origin
allowlist; when unset the check is skipped (demo convenience) and a
warning is logged.
"""
from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Optional, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

import mmbus
from mmbus_cast.fastapi import broadcast_lifespan, worker_shard_from_env

logger = logging.getLogger("mmcast-chat")

CHANNEL = "chat"


def _allowed_origins() -> Optional[Set[str]]:
    raw = os.environ.get("MMCAST_ALLOWED_ORIGINS")
    if not raw:
        return None
    return {o.strip() for o in raw.split(",") if o.strip()}


def _origin_ok(socket: WebSocket, allowed: Optional[Set[str]]) -> bool:
    """Browser same-origin policy does NOT apply to WebSockets, so a page
    on any site can open a socket to us.  Enforce an Origin allowlist when
    one is configured.  A missing Origin header (non-browser clients) is
    allowed; an allowlist that's unset disables the check (demo only)."""
    if allowed is None:
        return True
    origin = socket.headers.get("origin")
    if origin is None:
        return True  # non-browser client (curl, native app, tests)
    return origin in allowed


@asynccontextmanager
async def lifespan(app: FastAPI):
    worker_id, peers = worker_shard_from_env()
    # Single-process default: one worker.  Multi-worker users set
    # MMCAST_WORKER_ID + MMCAST_PEERS in the deployment env.
    multi_worker = len(peers) > 1
    if _allowed_origins() is None:
        logger.warning(
            "MMCAST_ALLOWED_ORIGINS is unset — WebSocket Origin checking is "
            "DISABLED. Set it before exposing this beyond localhost."
        )
    async with broadcast_lifespan(
        "mmcast-chat-demo",
        worker_id=worker_id if multi_worker else None,
        peers=peers if multi_worker else None,
        prepare=[CHANNEL],
        # Ring large enough to absorb burst traffic.  256 × 4 KiB = 1 MiB.
        capacity=256,
        slot_size=4096,
        # WAL gives durable replay across publisher restarts; off for this
        # toy.  Flip to True for production durability.
        wal_enabled=False,
    ) as bc:
        app.state.broadcast = bc
        app.state.allowed_origins = _allowed_origins()
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
  // Pick ws:// or wss:// to match the page scheme (avoids mixed-content
  // blocking when served over HTTPS).
  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  const ws = new WebSocket(`${proto}//${location.host}/ws`);
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
    if not _origin_ok(socket, socket.app.state.allowed_origins):
        # 1008 = policy violation.
        await socket.close(code=1008)
        return
    await socket.accept()
    bc = socket.app.state.broadcast

    async def push_outbound(sub) -> None:
        """Forward broadcast messages → this client."""
        try:
            async for event in sub:
                # Peers may publish non-UTF-8 bytes (or via the bridge);
                # replace undecodable bytes rather than killing the task.
                await socket.send_text(event.data.decode("utf-8", "replace"))
        except (WebSocketDisconnect, RuntimeError):
            pass

    async def pull_inbound() -> None:
        """Forward this client's typed messages → broadcast."""
        try:
            while True:
                msg = await socket.receive_text()
                try:
                    await bc.publish(CHANNEL, msg.encode())
                except (mmbus.MessageTooLargeError, mmbus.BusFullError) as e:
                    # One oversized/rejected message must not tear down the
                    # client's connection.
                    logger.warning("mmcast-chat: dropping message: %s", e)
        except WebSocketDisconnect:
            pass

    # Replay the last 20 messages on join so a new tab has context.
    async with bc.subscribe(CHANNEL, replay_last=20) as sub:
        out_task = asyncio.create_task(push_outbound(sub))
        in_task = asyncio.create_task(pull_inbound())
        try:
            await asyncio.wait(
                {out_task, in_task}, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            # Cancel and AWAIT the survivor so no "Task was destroyed but
            # it is pending!" warning escapes on shutdown.
            for t in (out_task, in_task):
                t.cancel()
            for t in (out_task, in_task):
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
