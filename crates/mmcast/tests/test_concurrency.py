"""Concurrency / lifecycle-race tests for the core fan-out.

Covers the races the review flagged:
  * concurrent subscribe() must not open duplicate mmbus subscriptions
  * subscribe() racing a close must not leak an orphan channel
  * a disconnected/closed subscriber's iterator must always terminate,
    even with a full queue (Event-based close, not a queue sentinel)
"""
from __future__ import annotations

import asyncio
import os
import shutil
import uuid

import pytest

from mmbus_cast import Broadcast, BroadcastClosedError


@pytest.fixture
def short_bus_dir():
    root = f"/tmp/mmcast-test-{uuid.uuid4().hex[:8]}"
    os.makedirs(root, exist_ok=True)
    yield root
    shutil.rmtree(root, ignore_errors=True)


def _fresh_bus(short_root: str) -> dict:
    return {
        "base_dir": short_root,
        "capacity": 128,
        "slot_size": 4096,
        "wal_enabled": False,
    }


@pytest.mark.asyncio
async def test_concurrent_subscribe_single_mmbus_sub(short_bus_dir):
    """Many concurrent subscribe() calls on one channel share exactly one
    underlying mmbus subscription per peer."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        await bc.prepare("chat")
        subs = await asyncio.gather(*(bc.subscribe("chat") for _ in range(10)))
        try:
            ch = bc._channels["chat"]
            # Single-publisher mode → exactly one mmbus subscription ("_solo").
            assert len(ch._mmbus_subs) == 1
            assert len(ch._subs) == 10
            # And it actually fans out to all 10.
            await bc.publish("chat", b"broadcast")
            for s in subs:
                ev = await asyncio.wait_for(s.__anext__(), 5.0)
                assert ev.data == b"broadcast"
        finally:
            for s in subs:
                await s.__aexit__(None, None, None)


@pytest.mark.asyncio
async def test_subscribe_after_close_raises(short_bus_dir):
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        pass
    with pytest.raises(BroadcastClosedError):
        # Eager closed-check happens in subscribe() itself.
        bc.subscribe("chat")


@pytest.mark.asyncio
async def test_subscribe_opener_after_close_raises_and_no_leak(short_bus_dir):
    """A subscribe context whose opener runs *after* the Broadcast closed
    must raise (the under-lock closed re-check) and must not resurrect a
    channel into the cleared registry — the M9 race, made deterministic
    by deferring the open until after close."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    await bc.__aenter__()
    await bc.prepare("chat")

    # subscribe() validates + returns a context while still open; the
    # opener (which creates the channel) runs lazily on await.
    ctx = bc.subscribe("newchan", connect_timeout_secs=2.0)
    await bc.__aexit__(None, None, None)

    with pytest.raises(BroadcastClosedError):
        await ctx  # opener runs now, after close → must raise
    assert bc._channels == {}  # no orphan channel resurrected


@pytest.mark.asyncio
async def test_disconnect_policy_full_queue_iterator_terminates(short_bus_dir):
    """The disconnect slow-policy must terminate the iterator even when the
    queue is completely full — the close is signalled out-of-band so it
    can't be starved by a full queue (the M8 race)."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        await bc.prepare("feed")
        sub = await bc.subscribe(
            "feed", queue_depth=2, slow_policy="disconnect"
        )
        async with sub:
            # Overflow the queue to trigger the disconnect policy.
            for i in range(20):
                await bc.publish("feed", f"m{i}".encode())
            for _ in range(30):
                await asyncio.sleep(0.01)
            # Iterator must terminate (drains what's queued, then stops).
            collected = []
            with pytest.raises(StopAsyncIteration):
                for _ in range(100):
                    ev = await asyncio.wait_for(sub.__anext__(), 2.0)
                    collected.append(ev.data)
            assert sub.slow_count >= 1


@pytest.mark.asyncio
async def test_broadcast_close_terminates_parked_iterator(short_bus_dir):
    """Closing the Broadcast wakes a consumer parked on an empty queue."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    await bc.__aenter__()
    await bc.prepare("chat")
    sub = await bc.subscribe("chat")

    async def drain():
        out = []
        async for ev in sub:
            out.append(ev.data)
        return out

    task = asyncio.create_task(drain())
    await asyncio.sleep(0.1)  # park on the empty queue
    await bc.__aexit__(None, None, None)
    out = await asyncio.wait_for(task, timeout=5.0)
    assert out == []
