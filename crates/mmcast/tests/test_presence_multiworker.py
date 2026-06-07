"""T13 — Multi-process presence under sharded Broadcasts.

Architectural property: ``Presence`` reaches into the Broadcast via
``broadcast.publish`` / ``broadcast.subscribe`` on a
``_presence:<channel>`` topic.  Both calls flow through the same
shard-aware ``_Channel`` machinery that handles the chat fan-in.  So
two Broadcasts in sharded mode (distinct ``worker_id``, shared
``peers``) automatically get cross-shard presence — no extra plumbing.

This test verifies that property end-to-end.  Closes residual risk 3.
"""
from __future__ import annotations

import asyncio
import os
import shutil
import uuid

import pytest

from mmbus_cast import Broadcast, Presence


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


async def _wait_for_member(p: Presence, member: str, timeout: float = 3.0) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if member in p.members:
            return
        await asyncio.sleep(0.02)
    raise AssertionError(
        f"timed out waiting for {member!r} in members={p.members!r}"
    )


async def _wait_for_leave(p: Presence, member: str, timeout: float = 3.0) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if member not in p.members:
            return
        await asyncio.sleep(0.02)
    raise AssertionError(
        f"timed out waiting for {member!r} to leave; members={p.members!r}"
    )


@pytest.mark.asyncio
async def test_presence_across_two_sharded_workers(short_bus_dir):
    """Two Broadcast instances simulating two workers each host one
    Presence member.  Each member must appear in the other worker's
    members snapshot — proving presence works across shards.
    """
    bus_name = f"t-{uuid.uuid4().hex[:8]}"
    peers = ["w0", "w1"]

    w0 = Broadcast(
        bus_name, worker_id="w0", peers=peers, **_fresh_bus(short_bus_dir)
    )
    w1 = Broadcast(
        bus_name, worker_id="w1", peers=peers, **_fresh_bus(short_bus_dir)
    )
    async with w0, w1:
        # No internal-topic poking needed: the channel reconnect loop
        # converges the cross-shard subscriptions once both workers'
        # presence publishers exist (staggered-startup resilience).
        async with w0.presence(
            "chat",
            member_id="alice",
            ttl_secs=5.0,
            heartbeat_secs=0.2,
        ) as p_alice:
            async with w1.presence(
                "chat",
                member_id="bob",
                ttl_secs=5.0,
                heartbeat_secs=0.2,
            ) as p_bob:
                await _wait_for_member(p_alice, "bob", timeout=8.0)
                await _wait_for_member(p_bob, "alice", timeout=8.0)
                assert {"alice", "bob"} <= p_alice.members
                assert {"alice", "bob"} <= p_bob.members


@pytest.mark.asyncio
async def test_presence_graceful_leave_across_shards(short_bus_dir):
    """A member exiting cleanly on one shard publishes a leave that the
    peer on the other shard receives within one heartbeat."""
    bus_name = f"t-{uuid.uuid4().hex[:8]}"
    peers = ["w0", "w1"]

    w0 = Broadcast(
        bus_name, worker_id="w0", peers=peers, **_fresh_bus(short_bus_dir)
    )
    w1 = Broadcast(
        bus_name, worker_id="w1", peers=peers, **_fresh_bus(short_bus_dir)
    )
    async with w0, w1:
        async with w0.presence(
            "chat",
            member_id="alice",
            ttl_secs=10.0,         # long so TTL doesn't help us
            heartbeat_secs=0.5,
        ) as p_alice:
            p_bob = w1.presence(
                "chat",
                member_id="bob",
                ttl_secs=10.0,
                heartbeat_secs=0.5,
            )
            await p_bob.__aenter__()
            await _wait_for_member(p_alice, "bob", timeout=8.0)

            # Bob exits gracefully on the OTHER shard.  Alice on w0 must
            # still see the leave event because the leave was published
            # to `_presence.chat.w1` and Alice subscribes to both peer
            # shards.
            await p_bob.__aexit__(None, None, None)
            await _wait_for_leave(p_alice, "bob", timeout=3.0)


@pytest.mark.asyncio
async def test_presence_ttl_eviction_across_shards(short_bus_dir):
    """Bob's heartbeat dies on w1; Alice on w0 still evicts him after
    TTL — proves the TTL loop fires on remotely-shard-published
    heartbeats."""
    bus_name = f"t-{uuid.uuid4().hex[:8]}"
    peers = ["w0", "w1"]

    w0 = Broadcast(
        bus_name, worker_id="w0", peers=peers, **_fresh_bus(short_bus_dir)
    )
    w1 = Broadcast(
        bus_name, worker_id="w1", peers=peers, **_fresh_bus(short_bus_dir)
    )
    async with w0, w1:
        # ttl must comfortably exceed cross-shard reconnect convergence
        # (~2s) so Alice actually sees Bob before any TTL math; the test
        # then stops Bob's heartbeat and checks eviction.
        async with w0.presence(
            "chat",
            member_id="alice",
            ttl_secs=1.0,
            heartbeat_secs=0.1,
        ) as p_alice:
            p_bob = w1.presence(
                "chat",
                member_id="bob",
                ttl_secs=1.0,
                heartbeat_secs=0.1,
            )
            await p_bob.__aenter__()
            try:
                await _wait_for_member(p_alice, "bob", timeout=8.0)
                # Cancel bob's heartbeat without a graceful leave.
                assert p_bob._heartbeat_task is not None
                p_bob._heartbeat_task.cancel()
                # Alice (on the other shard) evicts bob within ~1.5×TTL.
                await _wait_for_leave(p_alice, "bob", timeout=3.0)
            finally:
                p_bob._closed = True
                for t in (p_bob._consume_task, p_bob._eviction_task):
                    if t:
                        t.cancel()
                if p_bob._sub is not None:
                    try:
                        await p_bob._sub.__aexit__(None, None, None)
                    except Exception:
                        pass
