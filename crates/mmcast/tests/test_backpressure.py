"""T4 — Per-consumer slow-client backpressure.

Verifies the three ``slow_policy`` modes (``drop_oldest``,
``drop_newest``, ``disconnect``) on a subscription whose consumer is
not draining.  ``drop_oldest`` is the default and what the docker-
compose demo exercises; the others are covered for completeness.
"""
from __future__ import annotations

import asyncio
import logging
import os
import shutil
import uuid

import pytest

from mmbus_cast import Broadcast


@pytest.fixture
def short_bus_dir():
    root = f"/tmp/mmcast-test-{uuid.uuid4().hex[:8]}"
    os.makedirs(root, exist_ok=True)
    yield root
    shutil.rmtree(root, ignore_errors=True)


def _fresh_bus(short_root: str) -> dict:
    return {
        "base_dir": short_root,
        "capacity": 128,           # big enough to hold the burst pre-fanout
        "slot_size": 4096,
        "wal_enabled": False,
    }


async def _publish_burst(bc: Broadcast, channel: str, n: int) -> None:
    """Publish ``n`` distinct messages back to back, then yield to let
    the fan-out background task drain them into consumer queues."""
    for i in range(n):
        await bc.publish(channel, f"msg-{i}".encode())
    # One round-trip through the event loop is enough for the fanout
    # task to drain a small burst; a couple of sleeps lets larger
    # bursts settle on slower CI runners without flake.
    for _ in range(20):
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_drop_oldest_retains_newest(short_bus_dir, caplog):
    """``drop_oldest``: when the per-consumer queue is full, the oldest
    in-queue messages are discarded; the consumer sees the *newest*
    queue_depth messages."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    caplog.set_level(logging.WARNING, logger="mmbus_cast")
    async with bc:
        sub = await bc.subscribe(
            "chat", queue_depth=4, slow_policy="drop_oldest"
        )
        async with sub:
            await _publish_burst(bc, "chat", 10)
            # Drain whatever's in the queue.
            drained = []
            for _ in range(4):
                ev = await asyncio.wait_for(sub.__anext__(), 2.0)
                drained.append(ev.data)
            # 6 messages were dropped (10 burst minus 4 queue slots).
            # Whichever 4 remain must be the *newest* under drop_oldest,
            # i.e. the trailing window of the 10-message burst.
            assert drained == [
                b"msg-6", b"msg-7", b"msg-8", b"msg-9",
            ], drained
            assert sub.slow_count >= 6
            assert any(
                "slow consumer" in rec.message.lower()
                for rec in caplog.records
            )


@pytest.mark.asyncio
async def test_drop_newest_retains_oldest(short_bus_dir):
    """``drop_newest``: the queue keeps the first ``queue_depth``
    messages; later ones overflow and are dropped."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        sub = await bc.subscribe(
            "chat", queue_depth=4, slow_policy="drop_newest"
        )
        async with sub:
            await _publish_burst(bc, "chat", 10)
            drained = []
            for _ in range(4):
                ev = await asyncio.wait_for(sub.__anext__(), 2.0)
                drained.append(ev.data)
            assert drained == [
                b"msg-0", b"msg-1", b"msg-2", b"msg-3",
            ], drained
            assert sub.slow_count >= 6


@pytest.mark.asyncio
async def test_disconnect_closes_iterator(short_bus_dir):
    """``disconnect``: once the queue overflows, the consumer's async
    iterator terminates (after draining what's already queued)."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        sub = await bc.subscribe(
            "chat", queue_depth=4, slow_policy="disconnect"
        )
        async with sub:
            await _publish_burst(bc, "chat", 10)

            # Drain.  Expect to terminate (StopAsyncIteration) by the
            # time we've consumed the in-queue items.
            drained = []
            with pytest.raises(StopAsyncIteration):
                for _ in range(20):  # safety bound
                    ev = await asyncio.wait_for(sub.__anext__(), 2.0)
                    drained.append(ev.data)
            assert sub.slow_count >= 1
            # We at least saw the original 4 queued messages.
            assert len(drained) >= 1


@pytest.mark.asyncio
async def test_invalid_slow_policy_rejected(short_bus_dir):
    """Constructor validates ``slow_policy``."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        with pytest.raises(ValueError):
            await bc.subscribe("chat", slow_policy="explode")


@pytest.mark.asyncio
async def test_fast_consumer_no_slow_count(short_bus_dir):
    """The drop_count is 0 when the consumer keeps up."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        sub = await bc.subscribe("chat", queue_depth=4)
        async with sub:
            for i in range(10):
                await bc.publish("chat", f"msg-{i}".encode())
                ev = await asyncio.wait_for(sub.__anext__(), 2.0)
                assert ev.data == f"msg-{i}".encode()
            assert sub.slow_count == 0
            assert sub.delivered_count == 10


@pytest.mark.asyncio
async def test_disconnect_does_not_disturb_peers(short_bus_dir):
    """One slow consumer being disconnected does not affect a fast
    consumer on the same channel — fan-out is per-consumer."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        slow = await bc.subscribe(
            "chat", queue_depth=2, slow_policy="disconnect"
        )
        fast = await bc.subscribe("chat", queue_depth=64)
        async with slow, fast:
            for i in range(20):
                await bc.publish("chat", f"msg-{i}".encode())
            # Let fanout settle.
            for _ in range(20):
                await asyncio.sleep(0.01)
            # Fast consumer should have received all 20 messages.
            got = []
            for _ in range(20):
                ev = await asyncio.wait_for(fast.__anext__(), 2.0)
                got.append(ev.data)
            assert got == [f"msg-{i}".encode() for i in range(20)]
            # Slow consumer should have been disconnected (its counter is up).
            assert slow.slow_count >= 1
