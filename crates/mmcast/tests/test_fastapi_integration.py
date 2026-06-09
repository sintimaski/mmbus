"""FastAPI integration tests for the documented helper surface.

Distinct from ``test_fastapi_chat_e2e.py`` (which drives the shipped
example app): this builds a *fresh* minimal FastAPI app straight from
``broadcast_lifespan`` + ``Broadcast.subscribe``, and exercises the two
real-world wiring patterns:

  1. WS ↔ WS broadcast across two connections.
  2. A plain HTTP handler publishing to WS subscribers (e.g. a REST
     endpoint that pushes a notification) — the common "publish from
     outside the socket" case.

Uses Starlette's TestClient (sync, runs the ASGI app + lifespan in a
background portal), so these are sync tests.
"""
from __future__ import annotations

import asyncio
import uuid
from contextlib import asynccontextmanager

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")  # required by starlette.testclient

from fastapi import FastAPI, WebSocket, WebSocketDisconnect  # noqa: E402
from starlette.testclient import TestClient  # noqa: E402

from mmbus_cast.fastapi import broadcast_lifespan  # noqa: E402

CHANNEL = "chat"


def _make_app(bus_dir: str) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        async with broadcast_lifespan(
            f"fa-{uuid.uuid4().hex[:8]}",
            base_dir=bus_dir,
            wal_enabled=False,
            capacity=64,
            slot_size=4096,
            prepare=[CHANNEL],
        ) as bc:
            app.state.broadcast = bc
            yield

    app = FastAPI(lifespan=lifespan)

    @app.websocket("/ws")
    async def ws(socket: WebSocket) -> None:
        await socket.accept()
        bc = socket.app.state.broadcast

        async def pump(sub) -> None:
            try:
                async for ev in sub:
                    await socket.send_text(ev.data.decode())
            except (WebSocketDisconnect, RuntimeError):
                pass

        async with bc.subscribe(CHANNEL, replay_last=0) as sub:
            task = asyncio.create_task(pump(sub))
            try:
                while True:
                    msg = await socket.receive_text()
                    await bc.publish(CHANNEL, msg.encode())
            except WebSocketDisconnect:
                pass
            finally:
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass

    @app.post("/notify/{text}")
    async def notify(text: str) -> dict:
        # Publish from a normal request handler — proves the Broadcast
        # stashed in app.state is usable outside the WS handler.
        await app.state.broadcast.publish(CHANNEL, text.encode())
        return {"published": text}

    return app


def test_ws_to_ws_broadcast(short_bus_dir):
    app = _make_app(short_bus_dir)
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws1, client.websocket_connect(
            "/ws"
        ) as ws2:
            ws1.send_text("hello-fastapi")
            # Both connections (sender included) receive the broadcast.
            assert ws1.receive_text() == "hello-fastapi"
            assert ws2.receive_text() == "hello-fastapi"


def test_http_publish_reaches_ws(short_bus_dir):
    app = _make_app(short_bus_dir)
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws1:
            resp = client.post("/notify/from-http")
            assert resp.status_code == 200
            assert resp.json() == {"published": "from-http"}
            assert ws1.receive_text() == "from-http"


def test_lifespan_opens_and_closes_broadcast(short_bus_dir):
    """The Broadcast is open during the app's lifetime and closed after."""
    app = _make_app(short_bus_dir)
    with TestClient(app):
        bc = app.state.broadcast
        assert bc is not None
        assert bc._closed is False
    # After the lifespan exits, the broadcast is closed.
    assert app.state.broadcast._closed is True
