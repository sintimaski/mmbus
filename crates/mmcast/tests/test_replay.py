"""T5 — In-ring history replay (``replay_last=N``).

Honours the v0.1 semantic: replay is applied at *channel open* time
inside this Broadcast (not per-Subscription).  The spec's "best-effort,
in-ring only" caveat is documented in ``docs/spec-mmcast-v0.1.md``.
"""
from __future__ import annotations

import asyncio
import logging
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
async def test_replay_delivers_in_ring_history(short_bus_dir):
    """Publish first, subscribe with replay_last — the subscriber
    receives the historical messages before any live ones."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        # Claim publisher first so `subscribe_with_history` doesn't time
        # out waiting for one.
        await bc.prepare("chat")
        for i in range(5):
            await bc.publish("chat", f"hist-{i}".encode())

        sub = await bc.subscribe("chat", replay_last=5)
        async with sub:
            got = []
            for _ in range(5):
                ev = await asyncio.wait_for(sub.__anext__(), 2.0)
                got.append(ev.data)
            assert got == [f"hist-{i}".encode() for i in range(5)]


@pytest.mark.asyncio
async def test_replay_zero_matches_live_only(short_bus_dir):
    """``replay_last=0`` (default) skips history; the subscriber only
    sees messages published after it joins."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        await bc.prepare("chat")
        await bc.publish("chat", b"before")

        sub = await bc.subscribe("chat", replay_last=0)
        async with sub:
            await bc.publish("chat", b"after")
            ev = await asyncio.wait_for(sub.__anext__(), 2.0)
            # First message we see is "after" — "before" was historical.
            assert ev.data == b"after"


@pytest.mark.asyncio
async def test_replay_then_live(short_bus_dir):
    """Replay messages arrive first, live messages follow."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        await bc.prepare("chat")
        await bc.publish("chat", b"history-1")
        await bc.publish("chat", b"history-2")

        sub = await bc.subscribe("chat", replay_last=2)
        async with sub:
            ev1 = await asyncio.wait_for(sub.__anext__(), 2.0)
            ev2 = await asyncio.wait_for(sub.__anext__(), 2.0)
            assert ev1.data == b"history-1"
            assert ev2.data == b"history-2"

            await bc.publish("chat", b"live")
            ev3 = await asyncio.wait_for(sub.__anext__(), 2.0)
            assert ev3.data == b"live"


@pytest.mark.asyncio
async def test_replay_larger_than_ring_clamps(short_bus_dir):
    """``replay_last`` larger than the ring capacity clamps to whatever
    history is available — mmbus's ``subscribe_with_history`` returns
    the in-ring slice silently rather than erroring."""
    short_ring = {**_fresh_bus(short_bus_dir), "capacity": 8}
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **short_ring)
    async with bc:
        await bc.prepare("chat")
        for i in range(3):
            await bc.publish("chat", f"m-{i}".encode())

        # Ask for 100 historical messages on an 8-slot ring with 3 written.
        sub = await bc.subscribe("chat", replay_last=100)
        async with sub:
            got = []
            for _ in range(3):
                ev = await asyncio.wait_for(sub.__anext__(), 2.0)
                got.append(ev.data)
            assert got == [b"m-0", b"m-1", b"m-2"]


@pytest.mark.asyncio
async def test_replay_negative_rejected(short_bus_dir):
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        with pytest.raises(ValueError):
            await bc.subscribe("chat", replay_last=-1)


@pytest.mark.asyncio
async def test_second_subscriber_with_different_replay_warns(
    short_bus_dir, caplog
):
    """First subscriber's ``replay_last`` wins; later subscribers asking
    for a different value get a WARNING.

    v0.1 contract: only the first subscriber is *guaranteed* to see the
    replay.  Subsequent subscribers may incidentally receive some
    historical messages if they attach before the fanout has consumed
    them from mmbus — this is a per-channel race we don't pretend to
    resolve until v0.2 introduces a per-subscriber buffer.  The test
    asserts the warning fires and the live message reaches both.
    """
    caplog.set_level(logging.WARNING, logger="mmbus_cast")
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        await bc.prepare("chat")
        await bc.publish("chat", b"history")

        first = await bc.subscribe("chat", replay_last=1)
        second = await bc.subscribe("chat", replay_last=5)
        async with first, second:
            # Warning fired — the contract for late subscribers.
            assert any(
                "replay_last" in rec.message
                for rec in caplog.records
            ), [r.message for r in caplog.records]

            # First sub is guaranteed to see history.
            ev = await asyncio.wait_for(first.__anext__(), 2.0)
            assert ev.data == b"history"

            # Both must see live messages published from now on.
            await bc.publish("chat", b"live")

            async def first_live() -> bytes:
                while True:
                    ev = await asyncio.wait_for(first.__anext__(), 2.0)
                    if ev.data == b"live":
                        return ev.data

            async def second_live() -> bytes:
                while True:
                    ev = await asyncio.wait_for(second.__anext__(), 2.0)
                    if ev.data == b"live":
                        return ev.data

            assert await first_live() == b"live"
            assert await second_live() == b"live"
