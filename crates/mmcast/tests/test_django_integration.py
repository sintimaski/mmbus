"""Django (Channels) integration test.

mmcast has no Django-specific helper (Django Channels support is an
explicit v0.1 non-goal — see docs/spec-mmcast-v0.1.md), but the generic
``Broadcast`` API works inside a Channels ``AsyncWebsocketConsumer``.
This test proves that claim with a real consumer driven by Channels'
``WebsocketCommunicator``: two WS clients on one channel see each other's
messages, fanned out through an mmbus ring.

Skipped if Django / Channels aren't installed.
"""
from __future__ import annotations

import uuid

import pytest

pytest.importorskip("django")
pytest.importorskip("channels")

from django.conf import settings  # noqa: E402

# Channels consumers need a configured Django; a minimal in-memory config
# is enough (no apps, no DB).  Guard so multiple test modules don't
# double-configure.
if not settings.configured:
    settings.configure(
        DEBUG=True,
        ALLOWED_HOSTS=["*"],
        INSTALLED_APPS=[],
        DATABASES={},
        LOGGING_CONFIG=None,
    )
    import django  # noqa: E402

    django.setup()

import asyncio  # noqa: E402

from channels.generic.websocket import AsyncWebsocketConsumer  # noqa: E402
from channels.testing import WebsocketCommunicator  # noqa: E402

from mmbus_cast import Broadcast  # noqa: E402

CHANNEL = "chat"


def _with_broadcast(app, broadcast):
    """Tiny ASGI middleware that injects the live Broadcast into the
    connection scope under ``scope["broadcast"]``.

    This is the idiomatic Channels way to hand a consumer a per-app
    dependency — real apps put ``user`` / ``session`` on the scope the
    same way (via middleware), and a production deployment would inject
    the Broadcast from ``apps.py``'s ``ready()`` or an ASGI lifespan.
    """

    async def wrapped(scope, receive, send):
        scope = dict(scope)
        scope["broadcast"] = broadcast
        await app(scope, receive, send)

    return wrapped


class ChatConsumer(AsyncWebsocketConsumer):
    async def connect(self) -> None:
        self._bc: Broadcast = self.scope["broadcast"]
        await self.accept()
        self._sub_ctx = self._bc.subscribe(CHANNEL)
        self._sub = await self._sub_ctx.__aenter__()
        self._pump_task = asyncio.create_task(self._pump())

    async def _pump(self) -> None:
        try:
            async for ev in self._sub:
                await self.send(text_data=ev.data.decode())
        except (asyncio.CancelledError, Exception):
            pass

    async def receive(self, text_data=None, bytes_data=None) -> None:
        if text_data is not None:
            await self._bc.publish(CHANNEL, text_data.encode())

    async def disconnect(self, code) -> None:
        self._pump_task.cancel()
        try:
            await self._pump_task
        except (asyncio.CancelledError, Exception):
            pass
        await self._sub_ctx.__aexit__(None, None, None)


@pytest.mark.asyncio
async def test_django_channels_broadcast(short_bus_dir):
    bc = Broadcast(
        f"dj-{uuid.uuid4().hex[:8]}",
        base_dir=short_bus_dir,
        wal_enabled=False,
        capacity=64,
        slot_size=4096,
    )
    async with bc:
        await bc.prepare(CHANNEL)
        app = _with_broadcast(ChatConsumer.as_asgi(), bc)
        c1 = WebsocketCommunicator(app, "/ws/")
        c2 = WebsocketCommunicator(app, "/ws/")
        ok1, _ = await c1.connect()
        ok2, _ = await c2.connect()
        assert ok1 and ok2

        await c1.send_to(text_data="hello-django")
        # Both clients (sender included) receive the broadcast.
        assert await c1.receive_from(timeout=5) == "hello-django"
        assert await c2.receive_from(timeout=5) == "hello-django"

        # And the other direction.
        await c2.send_to(text_data="reply")
        assert await c1.receive_from(timeout=5) == "reply"
        assert await c2.receive_from(timeout=5) == "reply"

        await c1.disconnect()
        await c2.disconnect()


@pytest.mark.asyncio
async def test_django_channels_single_client_roundtrip(short_bus_dir):
    """A lone consumer still receives its own published messages (proves
    the subscribe-before-publish wiring in connect())."""
    bc = Broadcast(
        f"dj-{uuid.uuid4().hex[:8]}",
        base_dir=short_bus_dir,
        wal_enabled=False,
    )
    async with bc:
        await bc.prepare(CHANNEL)
        app = _with_broadcast(ChatConsumer.as_asgi(), bc)
        comm = WebsocketCommunicator(app, "/ws/")
        ok, _ = await comm.connect()
        assert ok
        await comm.send_to(text_data="solo")
        assert await comm.receive_from(timeout=5) == "solo"
        await comm.disconnect()
