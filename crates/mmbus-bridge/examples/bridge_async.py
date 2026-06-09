"""Embedded mmbus bridge inside an asyncio service.

The bridge runs on its own background threads; `await bridge.wait_async()`
suspends the coroutine until the bridge stops without blocking the event
loop, so it composes with the rest of your async application.

    python examples/bridge_async.py

Requires the companion wheels installed:

    pip install mmbus mmbus-bridge      # or: pip install mmbus[bridge]
"""
from __future__ import annotations

import asyncio

from mmbus_bridge import Bridge

cfg = {
    "bus": "demo",
    "listen": "127.0.0.1:0",  # ephemeral port; resolved value printed below
    "topics": [{"name": "events", "forward": True, "receive": True}],
    "peers": [],  # add a peer block to forward; see bridge_python.py
}


async def main() -> None:
    # `async with` starts the bridge on entry and shuts it down (off the
    # event loop) on exit — the async mirror of the sync `with Bridge(...)`.
    async with Bridge(cfg) as bridge:
        host, port = bridge.listen_addr
        print(f"bridge origin_id={bridge.origin_id} listening on {host}:{port}")

        # Run the bridge alongside other async work.  Here we just stop it
        # after 5 s; in a real service you'd `await bridge.wait_async()` for
        # the lifetime of the app (or until a shutdown signal).
        async def stop_after(delay: float) -> None:
            await asyncio.sleep(delay)
            await bridge.shutdown_async()

        await asyncio.gather(bridge.wait_async(), stop_after(5.0))
    print("bridge stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nshutting down")
