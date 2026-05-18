"""Linux smoke test — exercises the eventfd sync path, the asyncio
add_reader path, and the np_pipeline example end-to-end.  Used as the
post-build sanity check in Dockerfile + docker-compose.

Each section uses its own bus name so they can run sequentially without
interfering.  Subscribers run in a separate thread or event-loop because
PyO3 enforces single-borrower access to each `Bus` instance.
"""
import asyncio
import os
import subprocess
import sys
import threading

# Strip the script's directory (`python/`) from sys.path so `import mmbus`
# picks up the installed wheel (which has the compiled `_mmbus.so`)
# instead of the in-tree source `python/mmbus/__init__.py` — the latter's
# `from mmbus._mmbus import ...` line fails because no `.so` lives next
# to it.
if sys.path and os.path.basename(sys.path[0]) == "python":
    sys.path.pop(0)

import mmbus  # noqa: E402  (must follow the sys.path fix above)


def smoke_sync() -> None:
    """eventfd / AF_UNIX wakeup + sync subscribe + iterator path."""
    received: list[bytes] = []

    def subscriber() -> None:
        bus = mmbus.Bus("docker-test-sync")
        sub = bus.subscribe("ch", timeout_secs=10.0)
        received.append(sub.recv())

    t = threading.Thread(target=subscriber, daemon=True)
    t.start()

    pub = mmbus.Bus("docker-test-sync")
    pub.wait_for_subscribers("ch", n=1, timeout_secs=10.0)
    pub.publish("ch", b"hello-sync")
    t.join(timeout=3.0)
    assert received == [b"hello-sync"], f"sync: got {received}"
    print("  sync wakeup PASSED")


def smoke_async() -> None:
    """asyncio loop.add_reader path — proves the async surface works
    end-to-end without falling back to a thread pool.

    Windows uses ProactorEventLoop by default in Python 3.8+ and does
    not support `add_reader` on file descriptors — Windows async on
    mmbus is planned via an IOCP-backed path; for now the smoke skips
    on Windows.
    """
    if sys.platform == "win32":
        print("  async add_reader SKIPPED (Windows: asyncio uses IOCP, not add_reader)")
        return
    n_messages = 5

    async def subscriber_main(seen: list[bytes]) -> None:
        bus = mmbus.Bus("docker-test-async")
        sub = await bus.subscribe_async("ch", timeout_secs=10.0)
        async with sub:
            for _ in range(n_messages):
                seen.append(await sub.recv())

    seen: list[bytes] = []
    sub_done = threading.Event()

    def runner() -> None:
        asyncio.run(subscriber_main(seen))
        sub_done.set()

    t = threading.Thread(target=runner, daemon=True)
    t.start()

    pub = mmbus.Bus("docker-test-async")
    pub.wait_for_subscribers("ch", n=1, timeout_secs=10.0)
    for i in range(n_messages):
        pub.publish("ch", f"async-{i}".encode())
    sub_done.wait(timeout=5.0)
    assert sub_done.is_set(), "async subscriber thread did not finish"
    assert seen == [f"async-{i}".encode() for i in range(n_messages)], (
        f"async: got {seen}"
    )
    print("  async add_reader PASSED")


def smoke_backpressure_kwarg() -> None:
    """`backpressure=` kwarg validation + drop_oldest semantics."""
    # Invalid string is rejected at construction with ValueError.
    try:
        mmbus.Bus("bp-validate", backpressure="banana")
    except ValueError:
        pass
    else:
        raise SystemExit("backpressure='banana' should have raised ValueError")

    # drop_oldest lets the publisher outrun a slow reader without raising
    # BusFullError; the reader receives some prefix and the publisher
    # completes the full send.
    import shutil
    import tempfile
    base = os.path.join(tempfile.gettempdir(), "mmbus_smoke_bp")
    shutil.rmtree(base, ignore_errors=True)
    bus = mmbus.Bus(
        "drop-smoke", base_dir=base,
        capacity=4, slot_size=8, backpressure="drop_oldest",
    )
    bus.clean_topic("ch")
    received: list[bytes] = []

    def reader() -> None:
        sub_bus = mmbus.Bus(
            "drop-smoke", base_dir=base,
            capacity=4, slot_size=8, backpressure="drop_oldest",
        )
        sub = sub_bus.subscribe("ch", timeout_secs=5.0)
        for _ in range(20):
            m = sub.recv_timeout(0.5)
            if m is None:
                break
            received.append(m)

    t = threading.Thread(target=reader, daemon=True)
    t.start()
    bus.wait_for_subscribers("ch", n=1, timeout_secs=5.0)
    # 500 publishes into a 4-slot ring must not raise BusFullError under
    # drop_oldest; subscriber sees a prefix of the stream.
    for i in range(500):
        bus.publish("ch", f"{i:08}".encode())
    t.join(timeout=5.0)
    shutil.rmtree(base, ignore_errors=True)
    assert len(received) < 500, "drop_oldest reader must have been skipped"
    assert len(received) > 0, "drop_oldest reader must have received something"
    print(f"  backpressure kwarg PASSED (drop_oldest: {len(received)}/500 frames seen)")


def smoke_example_np_pipeline() -> None:
    """Run the numpy round-trip example as a subprocess so its assertions
    are exercised in CI.  Skipped if numpy is not installed."""
    try:
        import numpy  # noqa: F401
    except ImportError:
        print("  np_pipeline SKIPPED (numpy not installed)")
        return
    here = os.path.dirname(__file__)
    example = os.path.normpath(os.path.join(here, "..", "examples", "np_pipeline.py"))
    result = subprocess.run(
        [sys.executable, example],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr, file=sys.stderr)
        raise SystemExit(f"np_pipeline example failed (exit {result.returncode})")
    print("  np_pipeline example PASSED")


def smoke_bridge_module() -> None:
    """`mmbus.bridge` module imports cleanly and raises the documented
    error when the bridge binary isn't on PATH.  This stays as a
    structural test: we never actually launch a real bridge here
    because that requires a full TCP topology + peer."""
    from mmbus import bridge as _bridge

    # An impossible explicit binary path must raise BridgeNotFoundError.
    try:
        _bridge.run("/dev/null/config.toml", binary="/this/does/not/exist")
    except _bridge.BridgeNotFoundError:
        pass
    else:
        raise SystemExit("BridgeNotFoundError expected for missing binary")

    # Foreground vs background entry points are both exposed.
    assert callable(_bridge.run)
    assert callable(_bridge.spawn)
    print("  bridge module PASSED")


def smoke_example_fastapi_broadcast() -> None:
    """Drive examples/fastapi_broadcast:app via Starlette's TestClient
    end-to-end: POST /publish on the HTTP side, recv on a WS connection
    on the subscriber side, assert byte-for-byte equality.  Skipped if
    fastapi or httpx (required by TestClient) is not installed."""
    try:
        import fastapi  # noqa: F401
        from fastapi.testclient import TestClient
    except (ImportError, RuntimeError):
        # RuntimeError happens when fastapi is present but httpx is not —
        # starlette.testclient raises at import.
        print("  fastapi_broadcast SKIPPED (fastapi+httpx not installed)")
        return

    here = os.path.dirname(__file__)
    repo_root = os.path.normpath(os.path.join(here, ".."))
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

    # The example creates Bus("fastapi-broadcast") at import time and
    # publishes an empty warmup byte under its lifespan.  Clean any
    # leftover topic state from a prior run so the smoke is reproducible.
    mmbus.Bus("fastapi-broadcast").clean_topic("broadcast")

    from examples.fastapi_broadcast import app  # noqa: WPS433

    with TestClient(app) as client:
        with client.websocket_connect("/ws") as ws:
            r = client.post("/publish", content=b"smoke-payload")
            assert r.status_code == 200, r.text
            msg = ws.receive_bytes()
            assert msg == b"smoke-payload", f"unexpected WS payload: {msg!r}"
            root = client.get("/").json()
            assert root["active_subscribers"] >= 1, root
    print("  fastapi_broadcast example PASSED")


def smoke_recv_batch() -> None:
    """Subscription.recv_batch(n, timeout) amortises GIL+PyO3 dispatch
    across N messages.  Verifies:
      1. empty list on timeout when nothing's published,
      2. single recv_batch drains a pre-populated burst,
      3. batch caps at `n` even when more are available.

    `recv_batch` blocks for the FIRST message and then drains
    non-blockingly via try_recv_into.  In a racing publisher
    scenario the drain loop can break the moment the ring is
    momentarily empty between two adjacent publishes — so each
    burst is published in full *before* the subscriber is allowed
    to start draining.  This mirrors the realistic burst pattern
    the batch API is for.
    """
    subscriber_ready = threading.Event()
    burst1_ready = threading.Event()
    burst2_ready = threading.Event()
    received_burst: list[bytes] = []
    received_capped: list[bytes] = []

    def subscriber() -> None:
        bus = mmbus.Bus("docker-test-batch")
        sub = bus.subscribe("ch", timeout_secs=10.0)
        # (1) empty list on timeout — no publishes yet.
        empty = sub.recv_batch(n=5, timeout_secs=0.05)
        assert empty == [], f"expected empty list on timeout, got {empty!r}"

        subscriber_ready.set()  # publisher may now push burst 1
        if not burst1_ready.wait(timeout=5.0):
            raise SystemExit("subscriber: burst1 was never published")
        # (2) burst 1 is fully in the ring — one recv_batch drains it.
        received_burst.extend(sub.recv_batch(n=20, timeout_secs=2.0))

        if not burst2_ready.wait(timeout=5.0):
            raise SystemExit("subscriber: burst2 was never published")
        # (3) burst 2 is fully in the ring — drain in two n=5 calls.
        received_capped.extend(sub.recv_batch(n=5, timeout_secs=2.0))
        received_capped.extend(sub.recv_batch(n=5, timeout_secs=2.0))

    t = threading.Thread(target=subscriber, daemon=True)
    t.start()
    pub = mmbus.Bus("docker-test-batch")
    pub.wait_for_subscribers("ch", n=1, timeout_secs=10.0)
    if not subscriber_ready.wait(timeout=5.0):
        raise SystemExit("recv_batch subscriber never finished empty-list check")

    for i in range(10):
        pub.publish("ch", f"msg-{i:02}".encode())
    burst1_ready.set()

    # Wait long enough for the subscriber to drain burst 1 before
    # we push burst 2 — without this the two bursts can interleave
    # in the ring and the n=5 cap assertion becomes nondeterministic.
    while len(received_burst) < 10:
        if not t.is_alive():
            break
        threading.Event().wait(0.01)

    for i in range(10, 20):
        pub.publish("ch", f"msg-{i:02}".encode())
    burst2_ready.set()

    t.join(timeout=5.0)

    assert len(received_burst) == 10, (
        f"recv_batch burst: expected 10, got {len(received_burst)}"
    )
    assert received_burst[0] == b"msg-00" and received_burst[-1] == b"msg-09"
    assert len(received_capped) == 10, (
        f"recv_batch capped: expected 10 total across two n=5 calls, "
        f"got {len(received_capped)}"
    )
    assert received_capped[0] == b"msg-10" and received_capped[-1] == b"msg-19"
    print(f"  recv_batch PASSED ({len(received_burst)} burst + "
          f"{len(received_capped)} capped)")


def smoke_publish_many() -> None:
    """Bus.publish_many fires ONE wakeup per subscriber for the batch.
    Verifies (a) every record arrives in order, (b) returned count
    matches the input length when ring has room.
    """
    received: list[bytes] = []
    burst_ready = threading.Event()

    def subscriber() -> None:
        bus = mmbus.Bus("docker-test-pubmany")
        sub = bus.subscribe("ch", timeout_secs=10.0)
        burst_ready.wait(timeout=5.0)
        # One recv_batch drains the entire publish_many burst on a
        # single wakeup.
        received.extend(sub.recv_batch(n=64, timeout_secs=2.0))

    t = threading.Thread(target=subscriber, daemon=True)
    t.start()
    pub = mmbus.Bus("docker-test-pubmany")
    pub.wait_for_subscribers("ch", n=1, timeout_secs=10.0)
    payloads = [f"item-{i:02}".encode() for i in range(20)]
    n = pub.publish_many("ch", payloads)
    burst_ready.set()
    t.join(timeout=5.0)
    assert n == 20, f"publish_many: expected 20 written, got {n}"
    assert len(received) == 20, f"recv_batch saw {len(received)}/20"
    assert received[0] == b"item-00" and received[-1] == b"item-19"
    print(f"  publish_many PASSED ({n} written, drained in one recv_batch)")


def smoke_recv_into_buffer() -> None:
    """Subscription.recv_into_buffer drains fixed-size payloads
    directly into a bytearray (no PyBytes alloc per message).  If
    numpy is installed, also verify the same path works with a 2D
    ndarray.
    """
    PAYLOAD_SIZE = 8
    N = 16
    burst_ready = threading.Event()
    result_bytearray = {"count": 0, "buf": None}

    def subscriber_bytearray() -> None:
        bus = mmbus.Bus("docker-test-rcvbuf-ba")
        sub = bus.subscribe("ch", timeout_secs=10.0)
        burst_ready.wait(timeout=5.0)
        buf = bytearray(N * PAYLOAD_SIZE)
        n = sub.recv_into_buffer(buf, payload_size=PAYLOAD_SIZE, timeout_secs=2.0)
        result_bytearray["count"] = n
        result_bytearray["buf"] = bytes(buf[: n * PAYLOAD_SIZE])

    t = threading.Thread(target=subscriber_bytearray, daemon=True)
    t.start()
    pub = mmbus.Bus("docker-test-rcvbuf-ba")
    pub.wait_for_subscribers("ch", n=1, timeout_secs=10.0)
    payloads = [i.to_bytes(PAYLOAD_SIZE, "little") for i in range(N)]
    pub.publish_many("ch", payloads)
    burst_ready.set()
    t.join(timeout=5.0)
    assert result_bytearray["count"] == N, (
        f"recv_into_buffer/bytearray: got {result_bytearray['count']}/{N}"
    )
    expected = b"".join(payloads)
    assert result_bytearray["buf"] == expected
    print(f"  recv_into_buffer/bytearray PASSED ({N}×{PAYLOAD_SIZE}B)")

    # numpy variant
    try:
        import numpy as np
    except ImportError:
        print("  recv_into_buffer/numpy SKIPPED (numpy not installed)")
        return

    result_numpy = {"count": 0, "buf": None}
    burst_ready2 = threading.Event()

    def subscriber_numpy() -> None:
        bus = mmbus.Bus("docker-test-rcvbuf-np")
        sub = bus.subscribe("ch", timeout_secs=10.0)
        burst_ready2.wait(timeout=5.0)
        buf = np.empty((N, PAYLOAD_SIZE), dtype=np.uint8)
        n = sub.recv_into_buffer(buf, payload_size=PAYLOAD_SIZE, timeout_secs=2.0)
        result_numpy["count"] = n
        result_numpy["buf"] = bytes(buf[:n].tobytes())

    t = threading.Thread(target=subscriber_numpy, daemon=True)
    t.start()
    pub_np = mmbus.Bus("docker-test-rcvbuf-np")
    pub_np.wait_for_subscribers("ch", n=1, timeout_secs=10.0)
    pub_np.publish_many("ch", payloads)
    burst_ready2.set()
    t.join(timeout=5.0)
    assert result_numpy["count"] == N, (
        f"recv_into_buffer/numpy: got {result_numpy['count']}/{N}"
    )
    assert result_numpy["buf"] == expected
    print(f"  recv_into_buffer/numpy PASSED ({N}×{PAYLOAD_SIZE}B uint8 ndarray)")


def main() -> None:
    smoke_sync()
    smoke_async()
    smoke_backpressure_kwarg()
    smoke_recv_batch()
    smoke_publish_many()
    smoke_recv_into_buffer()
    smoke_bridge_module()
    smoke_example_np_pipeline()
    smoke_example_fastapi_broadcast()
    print("eventfd smoke test PASSED")


if __name__ == "__main__":
    main()
