"""T7 — Tests for the FastAPI helpers (env-driven worker shard,
``broadcast_lifespan``).  The chat example exercises them end-to-end;
these are the unit-level guards.
"""
from __future__ import annotations

import asyncio
import logging
import uuid

import pytest

from mmbus_cast import Broadcast
from mmbus_cast.fastapi import broadcast_lifespan, worker_shard_from_env


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch):
    monkeypatch.delenv("MMCAST_WORKER_ID", raising=False)
    monkeypatch.delenv("MMCAST_PEERS", raising=False)
    monkeypatch.delenv("WEB_CONCURRENCY", raising=False)


def test_shard_from_env_var(monkeypatch):
    monkeypatch.setenv("MMCAST_WORKER_ID", "alpha")
    monkeypatch.setenv("MMCAST_PEERS", "alpha, beta ,gamma")
    wid, peers = worker_shard_from_env()
    assert wid == "alpha"
    assert peers == ["alpha", "beta", "gamma"]


def test_shard_from_workers_arg():
    wid, peers = worker_shard_from_env(workers=3)
    assert wid.startswith("w") and wid[1:].isdigit()  # default = w<pid>
    assert peers == ["w0", "w1", "w2"]


def test_shard_single_process_default():
    """No env, no workers count → single-element peer list with self."""
    wid, peers = worker_shard_from_env()
    assert peers == [wid]


def test_shard_warns_on_multiworker_without_peers(monkeypatch, caplog):
    """WEB_CONCURRENCY>1 but no peer list = silently-broken fan-in — must
    warn loudly so the operator notices."""
    monkeypatch.setenv("WEB_CONCURRENCY", "4")
    caplog.set_level(logging.WARNING, logger="mmbus_cast")
    wid, peers = worker_shard_from_env()
    assert peers == [wid]  # degraded to single-process
    assert any("WEB_CONCURRENCY" in r.message for r in caplog.records)


def test_shard_no_warn_when_peers_set(monkeypatch, caplog):
    monkeypatch.setenv("WEB_CONCURRENCY", "4")
    monkeypatch.setenv("MMCAST_PEERS", "w0,w1,w2,w3")
    caplog.set_level(logging.WARNING, logger="mmbus_cast")
    _, peers = worker_shard_from_env()
    assert peers == ["w0", "w1", "w2", "w3"]
    assert not any("WEB_CONCURRENCY" in r.message for r in caplog.records)


def test_shard_no_warn_single_worker(monkeypatch, caplog):
    monkeypatch.setenv("WEB_CONCURRENCY", "1")
    caplog.set_level(logging.WARNING, logger="mmbus_cast")
    worker_shard_from_env()
    assert not any("WEB_CONCURRENCY" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_broadcast_lifespan_opens_and_closes(short_bus_dir):
    """Lifespan helper opens the Broadcast for the yielded scope and
    closes it on exit."""
    name = f"t-{uuid.uuid4().hex[:8]}"
    async with broadcast_lifespan(
        name,
        base_dir=short_bus_dir,
        capacity=64,
        slot_size=4096,
        wal_enabled=False,
    ) as bc:
        assert isinstance(bc, Broadcast)
        # Bus is opened.
        assert bc._bus is not None
        # Sanity round-trip while we're holding the lifespan.
        await bc.prepare("chat")
        sub = await bc.subscribe("chat")
        async with sub:
            await bc.publish("chat", b"alive")
            ev = await asyncio.wait_for(sub.__anext__(), 2.0)
            assert ev.data == b"alive"
    # After context exit, the Broadcast is closed.
    assert bc._closed is True


@pytest.mark.asyncio
async def test_broadcast_lifespan_prepare_claims_publishers(short_bus_dir):
    name = f"t-{uuid.uuid4().hex[:8]}"
    async with broadcast_lifespan(
        name,
        base_dir=short_bus_dir,
        wal_enabled=False,
        prepare=["chat", "notify"],
    ) as bc:
        # Subscribing now should not time out — publishers exist.
        sub = await bc.subscribe("chat", connect_timeout_secs=2.0)
        async with sub:
            await bc.publish("chat", b"hello")
            ev = await asyncio.wait_for(sub.__anext__(), 2.0)
            assert ev.data == b"hello"
