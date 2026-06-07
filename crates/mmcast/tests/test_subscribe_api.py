"""subscribe() call-shape + lifecycle tests.

`subscribe()` returns an awaitable async-context-manager so both styles
work:

    sub = await bc.subscribe("chat")
    async with bc.subscribe("chat") as sub:

with eager argument validation at call time.
"""
from __future__ import annotations

import asyncio
import uuid

import pytest

from mmbus_cast import Broadcast, Event


def _fresh_bus(short_root: str) -> dict:
    return {
        "base_dir": short_root,
        "capacity": 64,
        "slot_size": 4096,
        "wal_enabled": False,
    }


@pytest.mark.asyncio
async def test_await_style(short_bus_dir):
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        sub = await bc.subscribe("chat")
        async with sub:
            await bc.publish("chat", b"hi")
            ev = await asyncio.wait_for(sub.__anext__(), 5.0)
            assert ev.data == b"hi"


@pytest.mark.asyncio
async def test_context_manager_style(short_bus_dir):
    """The spec's documented idiom: `async with bc.subscribe(...) as sub`."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        await bc.prepare("chat")
        async with bc.subscribe("chat") as sub:
            assert isinstance(sub, type(sub))
            await bc.publish("chat", b"yo")
            ev = await asyncio.wait_for(sub.__anext__(), 5.0)
            assert isinstance(ev, Event)
            assert ev.data == b"yo"


@pytest.mark.asyncio
async def test_context_manager_closes_subscription(short_bus_dir):
    """Exiting `async with bc.subscribe(...)` closes the underlying sub."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        await bc.prepare("chat")
        ctx = bc.subscribe("chat")
        async with ctx as sub:
            pass
        assert sub._closed is True


@pytest.mark.asyncio
async def test_eager_validation_before_await(short_bus_dir):
    """Bad arguments raise at the call site, not deferred to await."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        with pytest.raises(ValueError):
            bc.subscribe("chat", replay_last=-1)
        with pytest.raises(ValueError):
            bc.subscribe("chat", queue_depth=0)
        with pytest.raises(ValueError):
            bc.subscribe("chat", slow_policy="nonsense")


@pytest.mark.asyncio
async def test_await_returns_same_subscription_each_time(short_bus_dir):
    """Awaiting the context twice returns the same Subscription (idempotent
    open), not two subscriptions."""
    bc = Broadcast(f"t-{uuid.uuid4().hex[:8]}", **_fresh_bus(short_bus_dir))
    async with bc:
        await bc.prepare("chat")
        ctx = bc.subscribe("chat")
        sub1 = await ctx
        sub2 = await ctx
        assert sub1 is sub2
        async with sub1:
            pass
