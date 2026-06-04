"""T6 — Presence tracking tests.

Single-process semantics: two ``Presence`` instances on the same
``Broadcast`` see each other via the shared ``_presence:<channel>``
topic.  Multi-process presence (per-member sharding) is out of v0.1.
"""
from __future__ import annotations

import asyncio
import os
import shutil
import uuid

import pytest

from mmbus_cast import Broadcast, Presence, PresenceChange


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


async def _wait_for_member(p: Presence, member: str, timeout: float = 2.0) -> None:
    """Poll until ``p.members`` includes ``member`` or ``timeout`` elapses."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if member in p.members:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(
        f"timed out waiting for {member!r} in members={p.members!r}"
    )


async def _wait_for_leave(p: Presence, member: str, timeout: float = 2.0) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if member not in p.members:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(
        f"timed out waiting for {member!r} to leave; members={p.members!r}"
    )


@pytest.mark.asyncio
async def test_self_is_in_members_on_enter(short_bus_dir):
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        async with bc.presence(
            "chat",
            member_id="alice",
            ttl_secs=1.0,
            heartbeat_secs=0.1,
        ) as p:
            assert "alice" in p.members


@pytest.mark.asyncio
async def test_two_members_see_each_other(short_bus_dir):
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        async with bc.presence(
            "chat",
            member_id="alice",
            ttl_secs=1.0,
            heartbeat_secs=0.1,
        ) as p_alice:
            async with bc.presence(
                "chat",
                member_id="bob",
                ttl_secs=1.0,
                heartbeat_secs=0.1,
            ) as p_bob:
                await _wait_for_member(p_alice, "bob")
                await _wait_for_member(p_bob, "alice")
                assert {"alice", "bob"} <= p_alice.members
                assert {"alice", "bob"} <= p_bob.members


@pytest.mark.asyncio
async def test_graceful_leave_publishes_leave_event(short_bus_dir):
    """When a member exits the ``async with`` cleanly, peers see a leave
    change event within one heartbeat (before TTL expiry)."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        async with bc.presence(
            "chat",
            member_id="alice",
            ttl_secs=10.0,  # long enough that TTL doesn't help us
            heartbeat_secs=0.5,
        ) as p_alice:
            p_bob = bc.presence(
                "chat",
                member_id="bob",
                ttl_secs=10.0,
                heartbeat_secs=0.5,
            )
            await p_bob.__aenter__()
            await _wait_for_member(p_alice, "bob")

            # Drain any pending changes from p_alice so we can wait on
            # the leave deterministically.
            async def next_change(p: Presence) -> PresenceChange:
                return await asyncio.wait_for(p.__anext__(), 3.0)

            # Bob leaves gracefully.
            await p_bob.__aexit__(None, None, None)
            await _wait_for_leave(p_alice, "bob")


@pytest.mark.asyncio
async def test_ttl_eviction_when_no_heartbeats(short_bus_dir):
    """If a member stops heartbeating (simulated by suspending their
    heartbeat loop), peers evict them after ~TTL."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        async with bc.presence(
            "chat",
            member_id="alice",
            ttl_secs=0.3,
            heartbeat_secs=0.05,
        ) as p_alice:
            p_bob = bc.presence(
                "chat",
                member_id="bob",
                ttl_secs=0.3,
                heartbeat_secs=0.05,
            )
            await p_bob.__aenter__()
            try:
                await _wait_for_member(p_alice, "bob")

                # Suspend Bob's heartbeat without cleanly exiting (so no
                # ``leave`` is published — simulates a crash / unclean
                # disconnect).
                assert p_bob._heartbeat_task is not None
                p_bob._heartbeat_task.cancel()

                # Alice should evict bob within ~1.5×TTL.
                await _wait_for_leave(p_alice, "bob", timeout=1.5)
            finally:
                # Tidy up Bob without re-publishing (heartbeat task is
                # already cancelled; just close the rest).
                p_bob._closed = True
                if p_bob._consume_task:
                    p_bob._consume_task.cancel()
                if p_bob._eviction_task:
                    p_bob._eviction_task.cancel()
                if p_bob._sub is not None:
                    try:
                        await p_bob._sub.__aexit__(None, None, None)
                    except Exception:
                        pass


@pytest.mark.asyncio
async def test_invalid_ttl_rejected(short_bus_dir):
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        with pytest.raises(ValueError):
            bc.presence("chat", member_id="alice", ttl_secs=0)
        with pytest.raises(ValueError):
            bc.presence("chat", member_id="alice", ttl_secs=1.0, heartbeat_secs=0)
