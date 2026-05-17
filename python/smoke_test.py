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


def main() -> None:
    smoke_sync()
    smoke_async()
    smoke_example_np_pipeline()
    print("eventfd smoke test PASSED")


if __name__ == "__main__":
    main()
