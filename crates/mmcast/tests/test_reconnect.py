"""Peer reconnect-loop tests.

A peer shard that is offline at subscribe time must be picked up
automatically once it comes online — the convergence property that lets
multi-worker deployments start in any order.
"""
from __future__ import annotations

import asyncio
import uuid

import pytest

from mmbus_cast import Broadcast


def _fresh_bus(short_root: str) -> dict:
    return {
        "base_dir": short_root,
        "capacity": 128,
        "slot_size": 4096,
        "wal_enabled": False,
    }


@pytest.mark.asyncio
async def test_late_peer_is_picked_up(short_bus_dir):
    """w0 subscribes while w1 is offline; once w1 starts publishing, w0's
    reconnect loop attaches to it and delivers its messages."""
    bus_name = f"t-{uuid.uuid4().hex[:8]}"
    peers = ["w0", "w1"]

    w0 = Broadcast(bus_name, worker_id="w0", peers=peers, **_fresh_bus(short_bus_dir))
    w1 = Broadcast(bus_name, worker_id="w1", peers=peers, **_fresh_bus(short_bus_dir))

    async with w0, w1:
        # w0 subscribes first — w1's shard is offline at this point, so
        # only w0's own shard connects initially.
        sub0 = await w0.subscribe("chat", connect_timeout_secs=2.0)
        async with sub0:
            ch0 = w0._channels["chat"]
            assert "w1" not in ch0._mmbus_subs  # peer offline at first
            assert ch0._reconnect_task is not None  # reconnect loop armed

            # Now w1 comes online.  Wait for w0's reconnect loop to attach
            # to w1's shard before publishing — subscribe is live-only, so
            # a message published before attachment would legitimately be
            # missed (that's not what this test is about).
            await w1.prepare("chat")
            deadline = asyncio.get_event_loop().time() + 8.0
            while asyncio.get_event_loop().time() < deadline:
                if "w1" in ch0._mmbus_subs:
                    break
                await asyncio.sleep(0.05)
            assert "w1" in ch0._mmbus_subs, "reconnect loop never attached w1"

            # Now that w0 is attached to w1's shard, a publish is delivered.
            await w1.publish("chat", b"from-late-w1")
            ev = await asyncio.wait_for(sub0.__anext__(), 5.0)
            assert ev.data == b"from-late-w1"


@pytest.mark.asyncio
async def test_reconnect_loop_stops_when_converged(short_bus_dir):
    """Once all peers are connected, the reconnect loop exits (no idle
    busy-poll forever)."""
    bus_name = f"t-{uuid.uuid4().hex[:8]}"
    peers = ["w0", "w1"]

    w0 = Broadcast(bus_name, worker_id="w0", peers=peers, **_fresh_bus(short_bus_dir))
    w1 = Broadcast(bus_name, worker_id="w1", peers=peers, **_fresh_bus(short_bus_dir))

    async with w0, w1:
        await w0.prepare("chat")
        await w1.prepare("chat")
        # Both shards exist before subscribe → connects fully on first try.
        sub0 = await w0.subscribe("chat", connect_timeout_secs=2.0)
        async with sub0:
            ch0 = w0._channels["chat"]
            assert set(ch0._mmbus_subs) == {"w0", "w1"}
            # No missing peers → no reconnect loop needed.
            assert ch0._reconnect_task is None


@pytest.mark.asyncio
async def test_single_publisher_mode_no_reconnect_loop(short_bus_dir):
    """Single-publisher mode connects its one topic and arms no reconnect
    loop."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        await bc.prepare("chat")
        sub = await bc.subscribe("chat", connect_timeout_secs=2.0)
        async with sub:
            ch = bc._channels["chat"]
            assert set(ch._mmbus_subs) == {"_solo"}
            assert ch._reconnect_task is None
