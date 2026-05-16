"""mmbus — zero-copy pub/sub over mmap.

Quick start
-----------
Publisher process::

    from mmbus import Bus

    bus = Bus("my-app")
    bus.wait_for_subscribers("events", n=1)
    bus.publish("events", b"hello")

Subscriber process::

    from mmbus import Bus

    bus = Bus("my-app")
    for msg in bus.subscribe("events"):
        print(msg)

Async subscriber::

    from mmbus import Bus

    async def main():
        bus = Bus("my-app")
        async with bus.subscribe_async("events") as sub:
            async for msg in sub:
                print(msg)
"""
import asyncio as _asyncio

from mmbus._mmbus import (  # noqa: F401
    _RustBus,
    Subscription,
    TopicStats,
    AlreadyPublishingError,
    BusFullError,
    ConnectTimeoutError,
    MessageTooLargeError,
    TooManySubscribersError,
)


class Bus:
    """High-level pub/sub bus.  Wraps ``_RustBus`` and adds async support."""

    def __init__(self, name: str, **kwargs):
        self._bus = _RustBus(name, **kwargs)

    # ── Publishing ────────────────────────────────────────────────────────────

    def publish(self, topic: str, data: bytes) -> None:
        """Publish *data* to *topic*.

        Raises :exc:`BusFullError` under the default ``Error`` backpressure policy.
        """
        self._bus.publish(topic, data)

    # ── Synchronous subscribe ─────────────────────────────────────────────────

    def subscribe(self, topic: str, timeout_secs: float = 30.0) -> Subscription:
        """Return a synchronous :class:`Subscription` to *topic*.

        Blocks (GIL released) until the publisher is ready or *timeout_secs* elapses.
        Raises :exc:`ConnectTimeoutError` on timeout.

        Supports context-manager and iterator protocol::

            with bus.subscribe("events") as sub:
                for msg in sub:
                    process(msg)
        """
        return self._bus.subscribe(topic, timeout_secs=timeout_secs)

    # ── Async subscribe ───────────────────────────────────────────────────────

    async def subscribe_async(self, topic: str, timeout_secs: float = 30.0) -> "AsyncSubscription":
        """Coroutine that returns an :class:`AsyncSubscription` for use with asyncio.

        The blocking connect runs in the default executor so the event loop stays
        unblocked while waiting for the publisher to appear.
        """
        loop = _asyncio.get_running_loop()
        sync_sub = await loop.run_in_executor(
            None, lambda: self._bus.subscribe(topic, timeout_secs=timeout_secs)
        )
        return AsyncSubscription(sync_sub)

    # ── Coordination ──────────────────────────────────────────────────────────

    def wait_for_subscribers(
        self, topic: str, n: int = 1, timeout_secs: float = 30.0
    ) -> None:
        """Block until at least *n* subscribers are connected to *topic*.

        Raises :exc:`ConnectTimeoutError` if *timeout_secs* elapses first.
        """
        self._bus.wait_for_subscribers(topic, n=n, timeout_secs=timeout_secs)

    # ── Introspection ─────────────────────────────────────────────────────────

    def stats(self, topic: str):
        """Return a :class:`TopicStats` snapshot, or ``None`` if no publisher."""
        return self._bus.stats(topic)

    # ── Context-manager protocol ──────────────────────────────────────────────

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass


class AsyncSubscription:
    """asyncio-compatible subscription.  Wraps a synchronous :class:`Subscription`
    and offloads blocking calls to the running loop's default executor."""

    def __init__(self, sub: Subscription):
        self._sub = sub

    # ── Single-message receive ────────────────────────────────────────────────

    async def recv(self) -> bytes:
        """Await the next message.  Never returns ``None``."""
        loop = _asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._sub.recv)

    async def recv_timeout(self, timeout_secs: float = 1.0):
        """Await the next message up to *timeout_secs*.  Returns ``None`` on timeout."""
        loop = _asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, self._sub.recv_timeout, timeout_secs
        )

    def try_recv(self):
        """Non-blocking poll.  Returns ``None`` if no message is ready."""
        return self._sub.try_recv()

    # ── Properties forwarded from the inner Subscription ─────────────────────

    @property
    def lag(self) -> int:
        return self._sub.lag

    @property
    def cursor(self) -> int:
        return self._sub.cursor

    # ── Async iterator protocol ───────────────────────────────────────────────

    def __aiter__(self):
        return self

    async def __anext__(self) -> bytes:
        try:
            return await self.recv()
        except OSError:
            raise StopAsyncIteration

    # ── Async context-manager protocol ───────────────────────────────────────

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass


__all__ = [
    "Bus",
    "AsyncSubscription",
    "Subscription",
    "TopicStats",
    "AlreadyPublishingError",
    "BusFullError",
    "ConnectTimeoutError",
    "MessageTooLargeError",
    "TooManySubscribersError",
]
__version__ = "0.1.0"
