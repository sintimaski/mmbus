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

import mmbus


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
    end-to-end without falling back to a thread pool."""
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
    base = "/tmp/mmbus_smoke_bp"
    import shutil
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


def main() -> None:
    smoke_sync()
    smoke_async()
    smoke_backpressure_kwarg()
    smoke_example_np_pipeline()
    smoke_example_fastapi_broadcast()
    print("eventfd smoke test PASSED")


if __name__ == "__main__":
    main()
