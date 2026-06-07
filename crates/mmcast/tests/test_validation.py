"""Channel-name validation (the _validate module + its wiring).

Covers the reserved-namespace and path-safety rules, and that the public
Broadcast entry points actually enforce them.
"""
from __future__ import annotations

import uuid

import pytest

from mmbus_cast import Broadcast, InvalidChannelError
from mmbus_cast._validate import (
    MAX_CHANNEL_NAME_LEN,
    RESERVED_PREFIX,
    validate_channel_name,
)


def _fresh_bus(short_root: str) -> dict:
    return {
        "base_dir": short_root,
        "capacity": 64,
        "slot_size": 4096,
        "wal_enabled": False,
    }


# ── pure function ──────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "name",
    ["chat", "room.42", "a-b_c", "X", "events.v2", "a" * MAX_CHANNEL_NAME_LEN],
)
def test_valid_names_pass(name):
    assert validate_channel_name(name) == name


@pytest.mark.parametrize(
    "name",
    [
        "_private",            # reserved prefix
        "_presence.chat",      # the internal namespace
        "",                    # empty
        "a" * (MAX_CHANNEL_NAME_LEN + 1),  # too long
        "../etc/passwd",       # traversal (slash + ..)
        "..",                  # bare traversal
        ".",                   # bare dot
        "a/b",                 # slash not allowed
        "a b",                 # space not allowed
        "café",                # non-ASCII
        "a:b",                 # colon (Windows-hostile)
        "a\x00b",              # NUL
    ],
)
def test_invalid_names_rejected(name):
    with pytest.raises(InvalidChannelError):
        validate_channel_name(name)


def test_non_str_rejected():
    with pytest.raises(InvalidChannelError):
        validate_channel_name(123)  # type: ignore[arg-type]


def test_internal_bypasses_reserved_prefix_only():
    # internal=True allows the leading underscore...
    assert validate_channel_name("_presence.chat", internal=True) == "_presence.chat"
    # ...but still enforces path-safety.
    with pytest.raises(InvalidChannelError):
        validate_channel_name("_presence:../evil", internal=True)


def test_invalid_channel_error_is_value_error():
    """Subclassing ValueError keeps existing `except ValueError` working."""
    assert issubclass(InvalidChannelError, ValueError)


# ── enforcement at the Broadcast entry points ──────────────────────────────


@pytest.mark.asyncio
async def test_publish_rejects_reserved_channel(short_bus_dir):
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        with pytest.raises(InvalidChannelError):
            await bc.publish("_presence.chat", b"forged")


@pytest.mark.asyncio
async def test_publish_json_rejects_traversal(short_bus_dir):
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        with pytest.raises(InvalidChannelError):
            await bc.publish_json("../../etc/evil", {"x": 1})


@pytest.mark.asyncio
async def test_subscribe_rejects_reserved_channel_eagerly(short_bus_dir):
    """Validation is eager — raises at the subscribe() call, before await."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        with pytest.raises(InvalidChannelError):
            bc.subscribe("_private")  # no await — must raise synchronously


@pytest.mark.asyncio
async def test_prepare_rejects_bad_channel(short_bus_dir):
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        with pytest.raises(InvalidChannelError):
            await bc.prepare("ok", "_bad")


@pytest.mark.asyncio
async def test_presence_rejects_bad_channel(short_bus_dir):
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        with pytest.raises(InvalidChannelError):
            bc.presence("a/b", member_id="x")


# ── worker_id / peers validation ───────────────────────────────────────────


def test_bad_worker_id_rejected(short_bus_dir):
    with pytest.raises(InvalidChannelError):
        Broadcast(
            "t", worker_id="../escape", peers=["w0"], **_fresh_bus(short_bus_dir)
        )


def test_bad_peer_rejected(short_bus_dir):
    with pytest.raises(InvalidChannelError):
        Broadcast(
            "t", worker_id="w0", peers=["w0", "a:b"], **_fresh_bus(short_bus_dir)
        )


def test_reserved_prefix_constant():
    assert RESERVED_PREFIX == "_"
