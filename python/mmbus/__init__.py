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
    CursorTooOldError,
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

    def subscribe_with_history(
        self, topic: str, n_messages_back: int, timeout_secs: float = 30.0
    ) -> Subscription:
        """Subscribe and replay up to *n_messages_back* messages from before
        the connect moment.  Best effort — capped at the ring's capacity.

        Useful for late-joining workers / aggregators that want to see what
        happened in the last few moments without standing up a durable log.
        """
        return self._bus.subscribe_with_history(
            topic, n_messages_back, timeout_secs=timeout_secs
        )

    def subscribe_from(
        self, topic: str, cursor: int, timeout_secs: float = 30.0
    ) -> Subscription:
        """Subscribe starting at an explicit *cursor* value (e.g. one obtained
        from a previous :attr:`Subscription.cursor` checkpoint).

        Raises :exc:`CursorTooOldError` if *cursor* is older than the oldest
        in-ring slot.  Cursor stability is per-publisher-generation: a
        checkpoint taken before a publisher restart is invalid afterwards.
        """
        return self._bus.subscribe_from(topic, cursor, timeout_secs=timeout_secs)

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

    async def subscribe_anyio(
        self, topic: str, timeout_secs: float = 30.0
    ) -> "AnyioSubscription":
        """Coroutine that returns an :class:`AnyioSubscription` for use with
        any anyio-supported backend (asyncio, trio, …).

        Requires ``anyio``: ``pip install anyio``.  Uses ``anyio.to_thread``
        for blocking calls — costs one thread-pool slot per concurrent recv.
        On asyncio prefer :meth:`subscribe_async`, which uses
        ``loop.add_reader`` directly and avoids the thread pool.
        """
        from anyio import to_thread

        sync_sub = await to_thread.run_sync(
            lambda: self._bus.subscribe(topic, timeout_secs=timeout_secs)
        )
        return AnyioSubscription(sync_sub)

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

    def clean_topic(self, topic: str) -> None:
        """Remove all on-disk state for *topic* (ring file, signal socket,
        producer lock).  Raises :exc:`AlreadyPublishingError` if a publisher
        is currently active.  For test setup and dev tooling only."""
        self._bus.clean_topic(topic)

    # ── Context-manager protocol ──────────────────────────────────────────────

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass


class AsyncSubscription:
    """asyncio-compatible subscription using ``loop.add_reader`` — no thread
    pool, no blocking on the event-loop thread.

    The subscriber's wakeup fd (eventfd on Linux, socket on macOS) is
    registered with the running event loop.  When it becomes readable, a
    callback drains one wakeup signal and reads one message from the ring,
    then resolves the awaiting future.  On Linux a second reader is
    registered on the handshake socket so publisher disconnect (POLLHUP)
    is detected even while idle.
    """

    def __init__(self, sub: Subscription):
        self._sub = sub

    # ── Single-message receive ────────────────────────────────────────────────

    async def recv(self) -> bytes:
        """Await the next message.  Raises :exc:`EOFError` if the publisher
        disconnects while waiting."""
        loop = _asyncio.get_running_loop()
        fut = loop.create_future()
        wfd = self._sub.fileno()
        sfd = self._sub.socket_fileno()
        # On macOS these are the same fd; on Linux wfd is the eventfd and
        # sfd is the handshake socket (POLLHUP signals publisher death).
        same_fd = wfd == sfd

        def on_data() -> None:
            try:
                msg = self._sub.poll_recv()
            except OSError as e:
                if not fut.done():
                    fut.set_exception(e)
                return
            if msg is not None and not fut.done():
                fut.set_result(msg)
            # else: spurious wakeup — reader stays armed.

        def on_disconnect() -> None:
            if not fut.done():
                fut.set_exception(EOFError("publisher disconnected"))

        loop.add_reader(wfd, on_data)
        if not same_fd:
            loop.add_reader(sfd, on_disconnect)
        try:
            return await fut
        finally:
            loop.remove_reader(wfd)
            if not same_fd:
                loop.remove_reader(sfd)

    async def recv_timeout(self, timeout_secs: float = 1.0):
        """Await the next message up to *timeout_secs*.  Returns ``None`` on timeout."""
        try:
            return await _asyncio.wait_for(self.recv(), timeout=timeout_secs)
        except _asyncio.TimeoutError:
            return None

    def try_recv(self):
        """Non-blocking poll of the ring (does not drain the wakeup fd).
        Returns ``None`` if no message is ready."""
        return self._sub.try_recv()

    # ── Properties forwarded from the inner Subscription ─────────────────────

    @property
    def lag(self) -> int:
        return self._sub.lag

    @property
    def cursor(self) -> int:
        return self._sub.cursor

    def fileno(self) -> int:
        return self._sub.fileno()

    # ── Async iterator protocol ───────────────────────────────────────────────

    def __aiter__(self):
        return self

    async def __anext__(self) -> bytes:
        try:
            return await self.recv()
        except (OSError, EOFError):
            raise StopAsyncIteration

    # ── Async context-manager protocol ───────────────────────────────────────

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass


class AnyioSubscription:
    """Cross-backend async subscription (asyncio, trio, curio).

    Wraps a synchronous :class:`Subscription` and offloads blocking calls to
    ``anyio.to_thread`` — costs one worker-thread slot per concurrent recv.
    If you're on asyncio and care about thread-pool pressure, prefer
    :class:`AsyncSubscription` which uses ``loop.add_reader`` directly.

    Construct via :meth:`Bus.subscribe_anyio`; raises :exc:`ImportError`
    on instantiation if ``anyio`` is not installed.
    """

    def __init__(self, sub: Subscription):
        try:
            from anyio import to_thread as _to_thread
        except ImportError as e:  # pragma: no cover
            raise ImportError(
                "AnyioSubscription requires anyio: pip install anyio"
            ) from e
        self._to_thread = _to_thread
        self._sub = sub

    # ── Single-message receive ────────────────────────────────────────────────

    async def recv(self) -> bytes:
        """Await the next message."""
        return await self._to_thread.run_sync(self._sub.recv)

    async def recv_timeout(self, timeout_secs: float = 1.0):
        """Await the next message up to *timeout_secs*.  Returns ``None`` on timeout."""
        return await self._to_thread.run_sync(self._sub.recv_timeout, timeout_secs)

    def try_recv(self):
        """Non-blocking poll of the ring (does not drain the wakeup fd)."""
        return self._sub.try_recv()

    # ── Properties forwarded from the inner Subscription ─────────────────────

    @property
    def lag(self) -> int:
        return self._sub.lag

    @property
    def cursor(self) -> int:
        return self._sub.cursor

    def fileno(self) -> int:
        return self._sub.fileno()

    # ── Async iterator protocol ───────────────────────────────────────────────

    def __aiter__(self):
        return self

    async def __anext__(self) -> bytes:
        try:
            return await self.recv()
        except (OSError, EOFError):
            raise StopAsyncIteration

    # ── Async context-manager protocol ───────────────────────────────────────

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass


__all__ = [
    "Bus",
    "AsyncSubscription",
    "AnyioSubscription",
    "Subscription",
    "TopicStats",
    "AlreadyPublishingError",
    "BusFullError",
    "ConnectTimeoutError",
    "CursorTooOldError",
    "MessageTooLargeError",
    "TooManySubscribersError",
]
__version__ = "0.1.0"
