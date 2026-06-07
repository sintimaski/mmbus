"""Presence robustness tests.

Locks in the hardening: malformed-message tolerance, bounded change
queue, member caps, self-eviction guard, heartbeat floor, and the pure
parser's contract.
"""
from __future__ import annotations

import asyncio
import os
import shutil
import uuid

import pytest

from mmbus_cast import Broadcast
from mmbus_cast._presence import (
    _MAX_MEMBER_ID_LEN,
    _MAX_PRESENCE_PAYLOAD,
    _MIN_HEARTBEAT_SECS,
    _parse_presence_message,
)


@pytest.fixture
def short_bus_dir():
    root = f"/tmp/mmcast-test-{uuid.uuid4().hex[:8]}"
    os.makedirs(root, exist_ok=True)
    yield root
    shutil.rmtree(root, ignore_errors=True)


def _fresh_bus(short_root: str) -> dict:
    return {
        "base_dir": short_root,
        "capacity": 256,
        "slot_size": 4096,
        "wal_enabled": False,
    }


# ── pure parser ────────────────────────────────────────────────────────────


def test_parse_valid():
    assert _parse_presence_message(b'{"member":"a","kind":"join"}') == ("a", "join")
    assert _parse_presence_message(b'{"member":"a","kind":"heartbeat","ts":1}') == (
        "a",
        "heartbeat",
    )
    assert _parse_presence_message(b'{"member":"a","kind":"leave"}') == ("a", "leave")


@pytest.mark.parametrize(
    "payload",
    [
        b"42",                                   # JSON but not a dict (int)
        b'"hello"',                              # JSON string, not dict
        b"\x80\x80\x80",                         # invalid UTF-8
        b"not json at all {",                    # malformed JSON
        b'{"member":{},"kind":"join"}',          # member not a str (unhashable)
        b'{"member":["a"],"kind":"join"}',       # member a list
        b'{"member":"a"}',                        # missing kind
        b'{"kind":"join"}',                       # missing member
        b'{"member":"a","kind":"bogus"}',        # unknown kind
        b'{"member":"","kind":"join"}',          # empty member
        b'{"member":"' + b"x" * (_MAX_MEMBER_ID_LEN + 1) + b'","kind":"join"}',  # too long
        b"x" * (_MAX_PRESENCE_PAYLOAD + 1),      # oversized payload
    ],
)
def test_parse_rejects_malformed(payload):
    assert _parse_presence_message(payload) is None


# ── consume loop survives malformed input ──────────────────────────────────


@pytest.mark.asyncio
async def test_malformed_message_does_not_kill_consume_loop(short_bus_dir):
    """A bad record published to the presence topic must not freeze the
    subsystem: a subsequent legitimate member is still tracked."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        async with bc.presence(
            "chat", member_id="alice", ttl_secs=5.0, heartbeat_secs=0.2
        ) as p:
            # Inject several malformed records onto the *internal* topic
            # via the internal publish path (simulating a buggy/hostile
            # same-host publisher).
            topic = p._presence_topic
            for bad in (b"42", b"\x80\x80", b'{"member":{},"kind":"join"}', b"{"):
                await bc._publish_internal(topic, bad)
            # Now a legitimate join.
            await bc._publish_internal(
                topic, b'{"member":"bob","kind":"join","ts":1}'
            )
            # The consume loop survived the bad records and tracks bob.
            deadline = asyncio.get_event_loop().time() + 3.0
            while asyncio.get_event_loop().time() < deadline:
                if "bob" in p.members:
                    break
                await asyncio.sleep(0.02)
            assert "bob" in p.members
            assert p._consume_task is not None and not p._consume_task.done()


# ── bounded change queue ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_changes_queue_is_bounded(short_bus_dir):
    """A flood of distinct joins, with no consumer draining, must not grow
    the change queue without bound."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        async with bc.presence(
            "chat",
            member_id="alice",
            ttl_secs=30.0,
            heartbeat_secs=1.0,
            # Small cap so the test is fast.
            changes_queue_max=8,
        ) as p:
            topic = p._presence_topic
            for i in range(200):
                await bc._publish_internal(
                    topic, f'{{"member":"m{i}","kind":"join"}}'.encode()
                )
            # Let the consume loop process the flood.
            for _ in range(50):
                await asyncio.sleep(0.01)
            # Never exceeds the configured maxsize.
            assert p._changes.qsize() <= 8


# ── self-eviction guard ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_self_not_evicted_when_heartbeat_publish_fails(short_bus_dir):
    """If this member can't publish heartbeats (e.g. another process owns
    the single-publisher slot), it must not evict *itself* and emit a
    spurious self-leave."""
    # Process A owns the publisher for the presence topic.
    owner = Broadcast(f"shared-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    name = owner._name
    async with owner:
        async with owner.presence(
            "chat", member_id="owner", ttl_secs=0.3, heartbeat_secs=0.05
        ):
            # Process B opens the same bus + presence; it can't claim the
            # publisher (owner holds it), so its heartbeats fail.
            other = Broadcast(name, **_fresh_bus(short_bus_dir))
            async with other:
                p_b = other.presence(
                    "chat", member_id="bob", ttl_secs=0.3, heartbeat_secs=0.05
                )
                async with p_b:
                    # Wait well past TTL.  bob must still consider itself
                    # present (no self-eviction), even though it can't
                    # publish heartbeats.
                    await asyncio.sleep(0.8)
                    assert "bob" in p_b.members
                    # And no self-leave change was emitted.
                    self_leaves = []
                    while not p_b._changes.empty():
                        ch = p_b._changes.get_nowait()
                        if ch.member == "bob" and ch.joined is False:
                            self_leaves.append(ch)
                    assert self_leaves == []


# ── constructor validation ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_heartbeat_floor_enforced(short_bus_dir):
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        with pytest.raises(ValueError):
            bc.presence("chat", member_id="a", heartbeat_secs=0)
        with pytest.raises(ValueError):
            bc.presence(
                "chat", member_id="a", heartbeat_secs=_MIN_HEARTBEAT_SECS / 2
            )


@pytest.mark.asyncio
async def test_bad_member_id_rejected(short_bus_dir):
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        with pytest.raises(ValueError):
            bc.presence("chat", member_id="")
        with pytest.raises(ValueError):
            bc.presence("chat", member_id="x" * (_MAX_MEMBER_ID_LEN + 1))


@pytest.mark.asyncio
async def test_ttl_must_be_positive(short_bus_dir):
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        with pytest.raises(ValueError):
            bc.presence("chat", member_id="a", ttl_secs=0)
