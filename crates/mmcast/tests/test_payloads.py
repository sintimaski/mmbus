"""Payload edge cases + error-path propagation.

Covers the input/boundary and dependency-failure rows of the corner-case
checklist: empty payloads, the writable-buffer types `publish` accepts,
oversize → MessageTooLargeError, ring-full → BusFullError (error policy),
and non-serializable publish_json.
"""
from __future__ import annotations

import asyncio
import uuid

import pytest

import mmbus
from mmbus_cast import Broadcast


def _bus(short_root: str, **over) -> dict:
    base = {
        "base_dir": short_root,
        "capacity": 16,
        "slot_size": 256,
        "wal_enabled": False,
    }
    base.update(over)
    return base


@pytest.mark.asyncio
async def test_empty_payload_roundtrips(short_bus_dir):
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_bus(short_bus_dir))
    async with bc:
        sub = await bc.subscribe("x")
        async with sub:
            await bc.publish("x", b"")
            ev = await asyncio.wait_for(sub.__anext__(), 5.0)
            assert ev.data == b""


@pytest.mark.asyncio
async def test_bytearray_accepted(short_bus_dir):
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_bus(short_bus_dir))
    async with bc:
        sub = await bc.subscribe("x")
        async with sub:
            await bc.publish("x", bytearray(b"from-bytearray"))
            ev = await asyncio.wait_for(sub.__anext__(), 5.0)
            assert ev.data == b"from-bytearray"
            assert isinstance(ev.data, bytes)


@pytest.mark.asyncio
async def test_memoryview_accepted(short_bus_dir):
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_bus(short_bus_dir))
    async with bc:
        sub = await bc.subscribe("x")
        async with sub:
            await bc.publish("x", memoryview(b"from-memoryview"))
            ev = await asyncio.wait_for(sub.__anext__(), 5.0)
            assert ev.data == b"from-memoryview"


@pytest.mark.asyncio
async def test_message_too_large_raises(short_bus_dir):
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_bus(short_bus_dir, slot_size=64))
    async with bc:
        await bc.prepare("x")
        with pytest.raises(mmbus.MessageTooLargeError):
            await bc.publish("x", b"q" * 5000)


@pytest.mark.asyncio
async def test_bus_full_raises_in_error_mode(short_bus_dir):
    """With backpressure='error' and a connected (cursor-holding)
    subscriber, a burst that outruns the fanout fills the ring and
    surfaces BusFullError — the mmbus error policy propagates unchanged."""
    bc = Broadcast(
        f"t-{uuid.uuid4().hex[:8]}",
        **_bus(short_bus_dir, backpressure="error", capacity=4, slot_size=64),
    )
    async with bc:
        sub = await bc.subscribe("x")
        async with sub:
            with pytest.raises(mmbus.BusFullError):
                # Tight loop: awaiting a non-suspending coroutine doesn't
                # yield, so the fanout can't drain the ring between
                # publishes — it fills and errors.
                for _ in range(100):
                    await bc.publish("x", b"z")


@pytest.mark.asyncio
async def test_drop_oldest_default_never_raises_bus_full(short_bus_dir):
    """The mmcast default (drop_oldest) absorbs the same burst without
    erroring — the contrast that justifies the default."""
    bc = Broadcast(
        f"t-{uuid.uuid4().hex[:8]}",
        **_bus(short_bus_dir, capacity=4, slot_size=64),  # default backpressure
    )
    async with bc:
        sub = await bc.subscribe("x")
        async with sub:
            for _ in range(100):
                await bc.publish("x", b"z")  # must not raise


@pytest.mark.asyncio
async def test_publish_json_non_serializable_raises(short_bus_dir):
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_bus(short_bus_dir))
    async with bc:
        await bc.prepare("x")
        with pytest.raises(TypeError):
            await bc.publish_json("x", {1, 2, 3})  # a set isn't JSON


@pytest.mark.asyncio
async def test_publish_json_roundtrips_unicode(short_bus_dir):
    """JSON helper preserves non-ASCII content through the round-trip."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_bus(short_bus_dir, slot_size=512))
    async with bc:
        sub = await bc.subscribe("x")
        async with sub:
            payload = {"msg": "héllo 🌍", "n": 3}
            await bc.publish_json("x", payload)
            ev = await asyncio.wait_for(sub.__anext__(), 5.0)
            assert ev.json() == payload
