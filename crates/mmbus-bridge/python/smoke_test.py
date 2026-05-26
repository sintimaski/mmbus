"""Sequential smoke test for the mmbus_bridge Python SDK.

Mirrors the core crate's python/smoke_test.py convention: a plain
``__main__`` script (no pytest dependency) so it runs in any CI that has
the two wheels installed.  Exits non-zero on the first failed check.

    pip install mmbus mmbus-bridge   # or maturin develop in both crates
    python bridge/python/smoke_test.py
"""
from __future__ import annotations

import asyncio
import sys
import tempfile
import time

from mmbus import Bus
from mmbus_bridge import (
    Bridge,
    BridgeConfigError,
    BridgeQuicError,
)

_failures = 0


def check(name: str, ok: bool, detail: str = "") -> None:
    global _failures
    status = "PASS" if ok else "FAIL"
    line = f"[{status}] {name}"
    if detail:
        line += f"  — {detail}"
    print(line)
    if not ok:
        _failures += 1


def expect_raises(name: str, exc_type, fn) -> None:
    try:
        fn()
    except exc_type:
        check(name, True)
    except Exception as e:  # noqa: BLE001
        check(name, False, f"raised {type(e).__name__}, expected {exc_type.__name__}")
    else:
        check(name, False, f"no exception, expected {exc_type.__name__}")


# ── Config validation (deterministic) ─────────────────────────────────────────

def test_bad_config_raises():
    # Missing required `bus` field → ConfigError.
    expect_raises(
        "bad config dict raises BridgeConfigError",
        BridgeConfigError,
        lambda: Bridge({"topics": [{"name": "x"}]}),
    )


def test_empty_bus_raises():
    expect_raises(
        "empty bus name raises BridgeConfigError",
        BridgeConfigError,
        lambda: Bridge({"bus": ""}),
    )


def test_quic_config_rejected():
    # The TCP-only wheel rejects a QUIC peer at start() time.
    cfg = {
        "bus": "app",
        "peers": [
            {
                "name": "q",
                "endpoint": "h:1",
                "preshared_key": "k",
                "transport": "quic",
                "peer_cert_fingerprint": "sha256:DEADBEEF",
            }
        ],
    }
    # Construction validates the fingerprint format (passes); start()
    # is where the TCP-only build refuses QUIC.
    bridge = Bridge(cfg)
    expect_raises(
        "QUIC peer rejected at start() by TCP-only wheel",
        BridgeQuicError,
        bridge.start,
    )


# ── Lifecycle (deterministic) ─────────────────────────────────────────────────

def test_start_stop():
    d = tempfile.mkdtemp(prefix="mmbus-bridge-smoke-")
    bridge = Bridge({"bus": "app", "base_dir": d, "listen": "127.0.0.1:0"})
    check("not running before start", not bridge.is_running())
    bridge.start()
    check("running after start", bridge.is_running())
    check("origin_id populated after start", bridge.origin_id is not None,
          f"origin_id={bridge.origin_id}")
    addr = bridge.listen_addr
    check("listen_addr resolves ephemeral port", addr is not None and addr[1] != 0,
          f"listen_addr={addr}")
    bridge.shutdown()
    check("not running after shutdown", not bridge.is_running())


def test_double_start_and_shutdown_idempotent():
    d = tempfile.mkdtemp(prefix="mmbus-bridge-smoke-")
    bridge = Bridge({"bus": "app", "base_dir": d, "listen": "127.0.0.1:0"})
    bridge.start()
    oid = bridge.origin_id
    bridge.start()  # no-op
    check("double start is a no-op (origin_id stable)", bridge.origin_id == oid)
    bridge.shutdown()
    try:
        bridge.shutdown()  # idempotent
        check("double shutdown is idempotent", True)
    except Exception as e:  # noqa: BLE001
        check("double shutdown is idempotent", False, str(e))


def test_context_manager():
    d = tempfile.mkdtemp(prefix="mmbus-bridge-smoke-")
    with Bridge({"bus": "app", "base_dir": d, "listen": "127.0.0.1:0"}) as bridge:
        check("running inside with-block", bridge.is_running())
    check("joined after with-block", not bridge.is_running())


def test_explicit_origin_id_preserved():
    d = tempfile.mkdtemp(prefix="mmbus-bridge-smoke-")
    with Bridge({"bus": "app", "base_dir": d, "listen": "127.0.0.1:0",
                 "origin_id": 4242}) as bridge:
        check("explicit origin_id preserved", bridge.origin_id == 4242,
              f"origin_id={bridge.origin_id}")


# ── Loopback round-trip (integration; timing-tolerant) ─────────────────────────

def test_loopback_roundtrip():
    """Bridge A forwards a local topic to Bridge B over 127.0.0.1; a
    message published on A's bus must appear on B's bus."""
    psk = "shared-smoke-secret"
    dir_a = tempfile.mkdtemp(prefix="mmbus-bridge-smoke-A-")
    dir_b = tempfile.mkdtemp(prefix="mmbus-bridge-smoke-B-")

    # B is the receiver: it listens and republishes inbound "events".
    # It needs a peer entry so A's PSK is in B's accepted set; the
    # endpoint is a throwaway (B never needs to dial A here).
    bridge_b = Bridge({
        "bus": "app",
        "base_dir": dir_b,
        "listen": "127.0.0.1:0",
        "topics": [{"name": "events", "forward": False, "receive": True}],
        "peers": [{"name": "A", "endpoint": "127.0.0.1:1", "preshared_key": psk}],
    })
    bridge_b.start()
    b_host, b_port = bridge_b.listen_addr

    # A is the sender: it subscribes to local "events" and forwards to B.
    bridge_a = Bridge({
        "bus": "app",
        "base_dir": dir_a,
        "topics": [{"name": "events", "forward": True, "receive": False}],
        "peers": [{"name": "B", "endpoint": f"{b_host}:{b_port}",
                   "preshared_key": psk}],
    })
    bridge_a.start()

    pub_a = Bus("app", base_dir=dir_a)
    bus_b = Bus("app", base_dir=dir_b)

    deadline = time.time() + 25.0
    payload = b"hello-across-the-wire"

    # B's bridge eagerly creates the producer for receive topic
    # "events" at startup, so this subscribe attaches deterministically
    # to a stable ring — no retry loop needed on the receive side.
    try:
        sub_b = bus_b.subscribe("events", timeout_secs=10.0)
    except Exception as e:  # noqa: BLE001
        check("B subscriber attached to eager producer", False, str(e))
        bridge_a.shutdown(); bridge_b.shutdown()
        return
    check("B subscriber attached to eager producer", True)

    # A's bridge subscriber connects to pub_a's topic once a producer
    # exists; publish a priming message to become the producer, then
    # wait for the bridge subscriber to attach.
    pub_a.publish("events", b"__priming__")
    try:
        pub_a.wait_for_subscribers("events", n=1, timeout_secs=10.0)
    except Exception as e:  # noqa: BLE001
        check("A's bridge subscriber attached", False, str(e))
        bridge_a.shutdown(); bridge_b.shutdown()
        return
    check("A's bridge subscriber attached", True)

    # The one inherently-async step left is the A->B TCP connect +
    # PeerHello auth (the forwarder dials with backoff).  Publish the
    # payload on A until it lands on B; identical payloads mean order
    # doesn't matter, so the assertion stays deterministic.  On
    # loopback this converges in well under a second; the deadline is
    # only a safety net.
    got = None
    while time.time() < deadline and got is None:
        pub_a.publish("events", payload)
        msg = sub_b.recv_timeout(0.5)
        if msg == payload:
            got = msg

    check("loopback round-trip delivered payload", got == payload,
          f"got={got!r}")

    bridge_a.shutdown()
    bridge_b.shutdown()


def test_async_wait_and_shutdown() -> None:
    """`wait_async` polling concurrently with `shutdown_async` must not
    deadlock (regression: shutdown used to hold the state mutex across the
    GIL-releasing join while the wait poll held the GIL waiting for it).
    The asyncio.wait_for timeout turns a regression into a failed check
    instead of a hung CI job.
    """
    cfg = {
        "bus": "smoke-async",
        "listen": "127.0.0.1:0",
        "topics": [{"name": "events", "forward": True, "receive": True}],
        "peers": [],
    }

    async def scenario() -> bool:
        async with Bridge(cfg) as bridge:
            if not bridge.is_running():
                return False

            async def stop() -> None:
                await asyncio.sleep(0.2)
                await bridge.shutdown_async()

            await asyncio.gather(bridge.wait_async(poll_interval=0.02), stop())
            return not bridge.is_running()

    try:
        ok = asyncio.run(asyncio.wait_for(scenario(), timeout=5.0))
        check("async wait_async + shutdown_async (no deadlock)", ok)
    except asyncio.TimeoutError:
        check("async wait_async + shutdown_async (no deadlock)", False, "timed out (deadlock)")


def main() -> int:
    print("mmbus_bridge Python SDK smoke test\n")
    test_bad_config_raises()
    test_empty_bus_raises()
    test_quic_config_rejected()
    test_start_stop()
    test_double_start_and_shutdown_idempotent()
    test_context_manager()
    test_explicit_origin_id_preserved()
    test_loopback_roundtrip()
    test_async_wait_and_shutdown()
    print()
    if _failures:
        print(f"{_failures} check(s) FAILED")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
