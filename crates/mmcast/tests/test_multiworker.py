"""T12 — Real multi-worker sharding tests.

Verifies that the per-worker sharding pattern documented in the spec
and exposed via ``Broadcast(worker_id=..., peers=[...])`` actually
delivers cross-worker fan-in.

Two complementary cases:

1. **In-process simulation** — two ``Broadcast`` instances within one
   pytest process, each with its own ``worker_id`` but a shared
   ``peers`` list.  Exercises the topic-sharding code path under real
   mmbus operations (separate publisher slots per shard, real fan-in
   subscriptions).  Fast, deterministic, runs in every CI matrix cell.
2. **Real subprocess uvicorn workers** — spawn two `uvicorn` processes
   on different ports with `MMCAST_WORKER_ID` / `MMCAST_PEERS` set,
   open WS clients to each, prove messages publish-from-A reach
   subscribers on B and vice versa.  Slow; skipped when
   uvicorn isn't importable.

The spec previously flagged "multi-worker sharding documented but not
exercised under real `uvicorn --workers N`" as a residual risk; these
tests close it.
"""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import socket
import struct
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

from mmbus_cast import Broadcast


# ──────────────────────────────────────────────────────────────────────────
# Shared fixtures
# ──────────────────────────────────────────────────────────────────────────


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


def _free_port() -> int:
    """Grab an ephemeral port the OS hands back, then close — race-y
    in principle but adequate for the bench-style integration test."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ──────────────────────────────────────────────────────────────────────────
# 1. In-process sharding
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_two_workers_fan_in_via_sharding(short_bus_dir):
    """Two Broadcast instances ("workers") with distinct worker_id +
    shared peers list see each other's publishes."""
    bus_name = f"t-{uuid.uuid4().hex[:8]}"
    peers = ["w0", "w1"]

    w0 = Broadcast(
        bus_name, worker_id="w0", peers=peers, **_fresh_bus(short_bus_dir)
    )
    w1 = Broadcast(
        bus_name, worker_id="w1", peers=peers, **_fresh_bus(short_bus_dir)
    )

    async with w0, w1:
        # Each worker must claim its own publisher shard before peers
        # can subscribe — ``prepare`` does this without sending data.
        await w0.prepare("chat")
        await w1.prepare("chat")

        sub0 = await w0.subscribe("chat", connect_timeout_secs=5.0)
        sub1 = await w1.subscribe("chat", connect_timeout_secs=5.0)

        async with sub0, sub1:
            # w0 publishes (lands on chat.w0); w1 receives via its
            # subscription to peer "w0"'s shard.
            await w0.publish("chat", b"from-w0")
            # Symmetric: w1 publishes (chat.w1); w0 picks it up.
            await w1.publish("chat", b"from-w1")

            # Each subscriber sees BOTH messages, in some order
            # (interleaving from two shards isn't ordered globally).
            collected_0: list[bytes] = []
            collected_1: list[bytes] = []
            for _ in range(2):
                ev0 = await asyncio.wait_for(sub0.__anext__(), 5.0)
                ev1 = await asyncio.wait_for(sub1.__anext__(), 5.0)
                collected_0.append(ev0.data)
                collected_1.append(ev1.data)

            assert sorted(collected_0) == [b"from-w0", b"from-w1"]
            assert sorted(collected_1) == [b"from-w0", b"from-w1"]


@pytest.mark.asyncio
async def test_publish_only_uses_own_shard(short_bus_dir):
    """Sanity: a publish from worker N lands on ``chat.N`` (verified by
    a separate single-publisher subscription on that physical topic)."""
    bus_name = f"t-{uuid.uuid4().hex[:8]}"
    peers = ["alpha", "beta"]

    alpha = Broadcast(
        bus_name, worker_id="alpha", peers=peers, **_fresh_bus(short_bus_dir)
    )
    async with alpha:
        await alpha.prepare("notes")

        # Direct mmbus.Bus subscription to the physical shard topic
        # to confirm the sharding pattern from outside mmcast.
        import mmbus

        # Reuse alpha's bus instance — exposed for tests only.
        bus = alpha._bus
        assert bus is not None
        async_sub = await bus.subscribe_async(
            "notes.alpha", timeout_secs=2.0
        )
        async with async_sub:
            await alpha.publish("notes", b"hello from alpha")
            msg = await asyncio.wait_for(async_sub.recv_timeout(2.0), 5.0)
            assert msg == b"hello from alpha"


# ──────────────────────────────────────────────────────────────────────────
# 2. Real subprocess uvicorn workers (skipped if uvicorn isn't installed)
# ──────────────────────────────────────────────────────────────────────────


pytest.importorskip("uvicorn")
pytest.importorskip("websockets")


def _spawn_worker(
    worker_id: str,
    peers: str,
    port: int,
    bus_dir: str,
    bus_name: str,
) -> subprocess.Popen:
    """Launch one uvicorn process running the benchmark mmcast app."""
    example_root = Path(__file__).resolve().parent.parent / "examples" / "benchmark" / "mmcast_side"
    env = {
        **os.environ,
        "MMCAST_WORKER_ID": worker_id,
        "MMCAST_PEERS": peers,
        "MMCAST_BUS_DIR": bus_dir,
        "MMCAST_BUS_NAME": bus_name,
        "PYTHONPATH": str(example_root),
    }
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


def _wait_for_port(port: int, timeout_s: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.2)
            try:
                s.connect(("127.0.0.1", port))
                return
            except OSError:
                time.sleep(0.1)
    raise RuntimeError(f"port {port} did not open within {timeout_s}s")


@pytest.mark.asyncio
async def test_real_subprocess_uvicorn_workers_cross_publish(short_bus_dir):
    """Two real uvicorn worker processes run the bench mmcast app with
    `MMCAST_WORKER_ID` + `MMCAST_PEERS` configured.  Sending a message
    via a WS connection to worker A must be delivered to a WS client
    connected to worker B.

    This is the deployment-shaped sharding test — exactly what a
    multi-worker FastAPI app does in production.
    """
    # `app.py` from `examples/benchmark/mmcast_side/` honours
    # MMCAST_BUS_DIR / MMCAST_BUS_NAME / MMCAST_WORKER_ID / MMCAST_PEERS.
    # We point both workers at the same bus dir so they share the
    # mmap files (required for sharded fan-in).
    bus_name = f"mw-{uuid.uuid4().hex[:8]}"
    peers = "w0,w1"
    port_a = _free_port()
    port_b = _free_port()

    proc_a = _spawn_worker("w0", peers, port_a, short_bus_dir, bus_name)
    proc_b = _spawn_worker("w1", peers, port_b, short_bus_dir, bus_name)
    try:
        try:
            _wait_for_port(port_a)
            _wait_for_port(port_b)
        except RuntimeError:
            # Dump the subprocess output so failures are diagnosable.
            for label, proc in (("A", proc_a), ("B", proc_b)):
                proc.terminate()
                try:
                    out, _ = proc.communicate(timeout=2)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    out, _ = proc.communicate()
                print(f"\n--- worker {label} log ---\n{out.decode(errors='replace')}")
            raise
        # Give the lifespan a moment to claim publishers + open
        # subscriptions across peers.
        await asyncio.sleep(1.0)

        import websockets

        url_a = f"ws://127.0.0.1:{port_a}/ws"
        url_b = f"ws://127.0.0.1:{port_b}/ws"
        async with websockets.connect(url_a) as ws_a:
            async with websockets.connect(url_b) as ws_b:
                # Let WS subscriptions settle before publishing.
                await asyncio.sleep(0.3)

                # Send from A; expect delivery to B (and back to A — both
                # workers fan-in from every peer including their own).
                payload = b"hello-from-worker-a"
                await ws_a.send(payload)

                async def _recv(ws, target: bytes) -> bytes:
                    while True:
                        msg = await asyncio.wait_for(ws.recv(), 5.0)
                        if isinstance(msg, str):
                            msg = msg.encode()
                        if msg == target:
                            return msg

                got_b = await _recv(ws_b, payload)
                got_a = await _recv(ws_a, payload)
                assert got_b == payload
                assert got_a == payload

                # And the reverse direction.
                payload2 = b"hello-from-worker-b"
                await ws_b.send(payload2)
                assert await _recv(ws_a, payload2) == payload2
                assert await _recv(ws_b, payload2) == payload2
    finally:
        for p in (proc_a, proc_b):
            p.terminate()
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()
