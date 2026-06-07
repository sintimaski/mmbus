"""T3 — Broadcast core behavioural tests.

Round-trips an in-process publish through an actual mmbus.Bus into an
async subscriber.  These are integration tests by mmbus convention
(``CLAUDE.md`` testing rules: real mmap + real Unix sockets, no
mocks).

Each test gets its own isolated bus dir (the ``short_bus_dir`` fixture
in ``conftest.py``) so they don't collide on the global
``/tmp/mmbus/<name>/`` layout.
"""
from __future__ import annotations

import asyncio
import uuid

import pytest

from mmbus_cast import Broadcast, BroadcastClosedError, Event


def _fresh_bus(short_root: str) -> dict:
    """Kwargs that point a Broadcast at an isolated bus dir."""
    return {
        "base_dir": short_root,
        # Small ring + slot to keep tests fast; not testing the ring
        # itself here.
        "capacity": 64,
        "slot_size": 4096,
        # Disable WAL for these tests — durability is mmbus's contract,
        # not mmcast's, and the WAL adds disk I/O the tests don't need.
        "wal_enabled": False,
    }


@pytest.mark.asyncio
async def test_publish_subscribe_roundtrip(short_bus_dir):
    """One subscriber receives a single publish."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        sub = await bc.subscribe("chat")
        async with sub:
            await bc.publish("chat", b"hello")
            event = await asyncio.wait_for(sub.__anext__(), timeout=5.0)
            assert isinstance(event, Event)
            assert event.data == b"hello"
            assert sub.delivered_count == 1


@pytest.mark.asyncio
async def test_publish_json(short_bus_dir):
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        sub = await bc.subscribe("chat")
        async with sub:
            await bc.publish_json("chat", {"user": "dd", "n": 7})
            event = await asyncio.wait_for(sub.__anext__(), timeout=5.0)
            assert event.json() == {"user": "dd", "n": 7}


@pytest.mark.asyncio
async def test_two_subscribers_both_receive(short_bus_dir):
    """Fan-out: two subscribers on the same channel both see every message."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        sub_a = await bc.subscribe("chat")
        sub_b = await bc.subscribe("chat")
        async with sub_a, sub_b:
            for i in range(5):
                await bc.publish("chat", f"msg-{i}".encode())
            got_a, got_b = [], []
            for _ in range(5):
                got_a.append((await asyncio.wait_for(sub_a.__anext__(), 5.0)).data)
                got_b.append((await asyncio.wait_for(sub_b.__anext__(), 5.0)).data)
            assert got_a == [f"msg-{i}".encode() for i in range(5)]
            assert got_b == got_a


@pytest.mark.asyncio
async def test_channels_are_isolated(short_bus_dir):
    """A subscriber on `chat` does not see messages on `notify`."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        sub = await bc.subscribe("chat")
        async with sub:
            await bc.publish("notify", b"wrong-channel")
            await bc.publish("chat", b"right-channel")
            event = await asyncio.wait_for(sub.__anext__(), timeout=5.0)
            assert event.data == b"right-channel"


@pytest.mark.asyncio
async def test_one_mmbus_subscriber_per_channel(short_bus_dir):
    """Two consumers on `chat` share a single underlying mmbus subscriber.

    Spec AC: the lib must bound mmbus subscriber count by O(channels),
    not O(consumers).  Verified by checking the internal ``_channels``
    map — there's exactly one ``_Channel`` per channel name regardless
    of how many ``Subscription`` instances exist for it.
    """
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        s1 = await bc.subscribe("chat")
        s2 = await bc.subscribe("chat")
        s3 = await bc.subscribe("chat")
        async with s1, s2, s3:
            assert len(bc._channels) == 1
            assert len(bc._channels["chat"]._subs) == 3


@pytest.mark.asyncio
async def test_publish_after_close_raises(short_bus_dir):
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        pass
    with pytest.raises(BroadcastClosedError):
        await bc.publish("chat", b"too-late")


@pytest.mark.asyncio
async def test_publish_rejects_non_bytes(short_bus_dir):
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        with pytest.raises(TypeError):
            await bc.publish("chat", "string-not-bytes")  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_close_wakes_subscribers(short_bus_dir):
    """Closing the Broadcast wakes any in-flight async-iterating consumers
    (their ``async for`` exits, not hangs)."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    await bc.__aenter__()
    sub = await bc.subscribe("chat")

    async def drain():
        out = []
        async for ev in sub:
            out.append(ev.data)
        return out

    drain_task = asyncio.create_task(drain())
    await asyncio.sleep(0.05)  # let drain park on the queue
    await bc.__aexit__(None, None, None)
    out = await asyncio.wait_for(drain_task, timeout=5.0)
    assert out == []  # no messages were published; close ended the iter
