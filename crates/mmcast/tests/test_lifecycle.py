"""Broadcast lifecycle + idempotency tests.

Covers the "duplicate submission / safe re-run" rows of the corner-case
checklist for the open/close/prepare/publish-bootstrap paths.
"""
from __future__ import annotations

import asyncio
import uuid

import pytest

from mmbus_cast import Broadcast


def _bus(short_root: str) -> dict:
    return {
        "base_dir": short_root,
        "capacity": 32,
        "slot_size": 256,
        "wal_enabled": False,
    }


@pytest.mark.asyncio
async def test_aenter_is_idempotent(short_bus_dir):
    """Re-entering an already-open Broadcast returns the same instance and
    does not recreate the underlying Bus."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_bus(short_bus_dir))
    r1 = await bc.__aenter__()
    bus1 = bc._bus
    r2 = await bc.__aenter__()
    bus2 = bc._bus
    try:
        assert r1 is bc and r2 is bc
        assert bus1 is bus2 is not None
    finally:
        await bc.__aexit__(None, None, None)


@pytest.mark.asyncio
async def test_aexit_is_idempotent(short_bus_dir):
    """A second close is a no-op, not an error."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_bus(short_bus_dir))
    await bc.__aenter__()
    await bc.__aexit__(None, None, None)
    # Second close must not raise.
    await bc.__aexit__(None, None, None)
    assert bc._closed is True


@pytest.mark.asyncio
async def test_prepare_is_idempotent(short_bus_dir):
    """Preparing the same channel repeatedly claims the publisher once and
    leaves exactly one channel registered."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_bus(short_bus_dir))
    async with bc:
        await bc.prepare("chat")
        await bc.prepare("chat")
        await bc.prepare("chat", "chat")
        assert set(bc._channels) == {"chat"}
        assert bc._channels["chat"]._publisher is not None


@pytest.mark.asyncio
async def test_publish_without_prepare_creates_publisher(short_bus_dir):
    """A first publish on a fresh channel lazily claims the publisher —
    no explicit prepare() required."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_bus(short_bus_dir))
    async with bc:
        # No prepare, no prior subscribe.
        await bc.publish("fresh", b"hello")
        assert bc._channels["fresh"]._publisher is not None


@pytest.mark.asyncio
async def test_prepare_then_subscribe_then_publish(short_bus_dir):
    """The documented startup ordering: prepare at boot, subscribe per
    connection, publish — all on the same channel."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_bus(short_bus_dir))
    async with bc:
        await bc.prepare("chat")
        sub = await bc.subscribe("chat")
        async with sub:
            await bc.publish("chat", b"ordered")
            ev = await asyncio.wait_for(sub.__anext__(), 5.0)
            assert ev.data == b"ordered"


@pytest.mark.asyncio
async def test_reuse_after_aexit_via_new_instance(short_bus_dir):
    """A closed Broadcast stays closed; the supported pattern is a new
    instance, which works against the same bus dir."""
    name = f"t-{uuid.uuid4().hex[:8]}"
    bc1 = Broadcast(name, **_bus(short_bus_dir))
    async with bc1:
        await bc1.publish("chat", b"first")
    # Fresh instance on the same bus.
    bc2 = Broadcast(name, **_bus(short_bus_dir))
    async with bc2:
        sub = await bc2.subscribe("chat")
        async with sub:
            await bc2.publish("chat", b"second")
            ev = await asyncio.wait_for(sub.__anext__(), 5.0)
            assert ev.data == b"second"
