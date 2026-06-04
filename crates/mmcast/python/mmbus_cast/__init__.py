"""mmbus_cast — ASGI WebSocket broadcast on top of mmbus.

The pitch::

    from mmbus_cast import Broadcast

    broadcast = Broadcast("my-app")
    async with broadcast:
        await broadcast.publish("chat", b"hello")
        async with broadcast.subscribe("chat", replay_last=20) as sub:
            async for event in sub:
                print(event.data)

Same shape as ``encode/broadcaster``, but the transport is mmbus (mmap
ring buffer, no broker) and new subscribers can replay the last N
in-ring messages on connect.

See ``docs/spec-mmcast-v0.1.md`` for the contract.
"""
from __future__ import annotations

from ._broadcast import Broadcast, BroadcastClosedError, SlowConsumer, Subscription
from ._event import Event, PresenceChange
from ._presence import Presence

__all__ = [
    "Broadcast",
    "BroadcastClosedError",
    "Event",
    "Presence",
    "PresenceChange",
    "SlowConsumer",
    "Subscription",
]

__version__ = "0.1.0"
