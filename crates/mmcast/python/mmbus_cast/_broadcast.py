"""Broadcast — the public surface.

Architecture (see ``docs/spec-mmcast-v0.1.md``):

    Broadcast
      ├── mmbus.Bus                                 (one per Broadcast)
      └── _Channel(channel_name)                    (one per channel)
            ├── mmbus.TopicPublisher                (claims pub slot once)
            ├── mmbus.AsyncSubscription per peer    (fan-in across shards)
            ├── fan-out background task per peer
            └── N × Subscription                    (one per consumer)
                  └── asyncio.Queue                 (bounded)

Per-channel single-mmbus-subscriber per peer matters: mmbus has a
``max_subscribers`` ceiling per topic (default 16), so the number of
WebSocket connections in a worker cannot drive the mmbus slot count.

The single-publisher-per-topic-across-processes rule (mmbus
``CLAUDE.md:11``) is handled by per-worker sharding when ``worker_id``
+ ``peers`` are supplied.  Without them, the lib runs in single-publisher
mode: one process owns the topic, others may subscribe but not publish.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Dict, Iterable, List, Optional

import mmbus

from ._event import Event

logger = logging.getLogger("mmbus_cast")


# Sentinel placed in a consumer's queue to wake it for shutdown.  A
# private object instance is unforgeable — no payload can collide with it.
_CLOSE_SENTINEL: bytes = object()  # type: ignore[assignment]


class BroadcastClosedError(RuntimeError):
    """Raised when publish/subscribe is called on a closed Broadcast."""


class SlowConsumer(RuntimeWarning):
    """Emitted (via ``logging`` + per-Subscription ``slow_count``) when a
    consumer's outbound queue overflows.  Whether the oldest is dropped,
    the newest is dropped, or the consumer is disconnected is governed by
    the ``slow_policy`` argument to :meth:`Broadcast.subscribe`."""


# ──────────────────────────────────────────────────────────────────────────
# Subscription — the per-consumer handle returned by Broadcast.subscribe()
# ──────────────────────────────────────────────────────────────────────────


class Subscription:
    """One consumer's view of a channel.  Async context manager + async
    iterator over :class:`Event`.

    Not constructed directly — get one from :meth:`Broadcast.subscribe`.
    """

    def __init__(
        self,
        channel: "_Channel",
        *,
        queue_depth: int,
        slow_policy: str,
    ) -> None:
        if slow_policy not in ("drop_oldest", "drop_newest", "disconnect"):
            raise ValueError(
                f"slow_policy must be one of "
                f"'drop_oldest', 'drop_newest', 'disconnect'; got {slow_policy!r}"
            )
        self._channel = channel
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=queue_depth)
        self._slow_policy = slow_policy
        self._closed = False
        # Public counters (spec § Observability)
        self.slow_count = 0
        self.delivered_count = 0

    # Fan-out path (called from _Channel's background task)
    def _enqueue(self, data: bytes) -> None:
        if self._closed:
            return
        try:
            self._queue.put_nowait(data)
        except asyncio.QueueFull:
            self.slow_count += 1
            self._on_full(data)

    def _on_full(self, data: bytes) -> None:
        # T4 lands the full policy matrix; T3 ships ``drop_oldest`` since
        # that's the default and what the demo needs.
        if self._slow_policy == "drop_oldest":
            try:
                self._queue.get_nowait()
                self._queue.put_nowait(data)
            except (asyncio.QueueEmpty, asyncio.QueueFull):
                pass
            logger.warning(
                "mmcast: slow consumer on %r — dropped oldest "
                "(slow_count=%d)",
                self._channel.name,
                self.slow_count,
            )
        elif self._slow_policy == "drop_newest":
            logger.warning(
                "mmcast: slow consumer on %r — dropped newest "
                "(slow_count=%d)",
                self._channel.name,
                self.slow_count,
            )
        elif self._slow_policy == "disconnect":
            logger.warning(
                "mmcast: slow consumer on %r — disconnecting "
                "(slow_count=%d)",
                self._channel.name,
                self.slow_count,
            )
            self._close_from_fanout()

    def _close_from_fanout(self) -> None:
        """Wake the consumer's iterator with the close sentinel.

        Called from the fan-out background task — must not raise.  The
        sentinel must reach the consumer (otherwise its ``__anext__``
        blocks forever on an empty closed queue), so if the queue is
        full we evict the oldest in-queue message to make room.  For
        the ``disconnect`` slow-policy this is acceptable — the
        consumer is being kicked anyway.
        """
        self._closed = True
        # Loop because we may race with the consumer popping items.
        for _ in range(self._queue.maxsize + 1):
            try:
                self._queue.put_nowait(_CLOSE_SENTINEL)
                return
            except asyncio.QueueFull:
                try:
                    self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    continue

    # Async iterator
    def __aiter__(self) -> "Subscription":
        return self

    async def __anext__(self) -> Event:
        data = await self._queue.get()
        if data is _CLOSE_SENTINEL:
            raise StopAsyncIteration
        self.delivered_count += 1
        return Event(data=data)

    # Async context manager
    async def __aenter__(self) -> "Subscription":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self._closed = True
        self._channel._detach(self)


# ──────────────────────────────────────────────────────────────────────────
# _Channel — per-channel fan-out hub (internal)
# ──────────────────────────────────────────────────────────────────────────


class _Channel:
    """Internal: holds the publisher slot + one mmbus subscription per
    peer shard + the fan-out tasks.

    Lifetime: created on first publish/subscribe to a channel name within
    a Broadcast; torn down only when the Broadcast closes.  Keeping it
    alive across "no consumers" gaps is intentional — it preserves
    in-ring history that ``replay_last`` will rely on (T5) and avoids
    rebuilding mmbus subscriptions on burst churn.
    """

    def __init__(self, broadcast: "Broadcast", name: str) -> None:
        self._broadcast = broadcast
        self.name = name  # logical name; physical topics are `name.<peer>`
        self._subs: set[Subscription] = set()
        self._publisher: Optional[mmbus.TopicPublisher] = None
        # One mmbus subscription per peer shard, keyed by peer id.
        self._mmbus_subs: Dict[str, Any] = {}
        self._fanout_tasks: List[asyncio.Task] = []
        self._closed = False
        # ``replay_last`` is per-channel (not per-Subscription) — the in-
        # ring history snapshot happens at channel-open time.  Set by
        # the first subscriber; subsequent subscribers with a different
        # value get a warning (see ``Broadcast.subscribe``).  v0.2 may
        # add a small in-process buffer to give every subscriber its own
        # replay, but that's a separate design.
        self._replay_last: int = 0

    def _topic_for(self, peer: Optional[str]) -> str:
        """Physical mmbus topic name for ``peer`` shard.

        Single-publisher mode (``peer is None``) uses the unsharded
        ``name``; sharded mode uses ``name.<peer>``.
        """
        return self.name if peer is None else f"{self.name}.{peer}"

    def ensure_publisher(self) -> None:
        """Claim this process's publisher slot.

        In sharded mode the slot is ``name.<worker_id>``.  In single-
        publisher mode it's just ``name``.  Either way it's idempotent
        within this Broadcast (the ``mmbus.Bus`` caches the
        ``TopicPublisher`` so reclaim is a no-op).
        """
        if self._publisher is not None:
            return
        topic = self._topic_for(self._broadcast._worker_id)
        self._publisher = self._broadcast._bus.topic(topic)

    def publish(self, data: bytes) -> None:
        self.ensure_publisher()
        assert self._publisher is not None  # for type checkers
        self._publisher.publish(data)

    async def start_subscriptions(
        self,
        *,
        connect_timeout_secs: float,
        replay_last: int = 0,
    ) -> None:
        """Open one mmbus subscription per peer shard (or one for the
        single-publisher topic).

        Idempotent: returns immediately if already started.  ``replay_last``
        only honoured on the first call (subsequent calls log a warning
        if their value differs — see :meth:`Broadcast.subscribe` for the
        full caveat).
        """
        if self._mmbus_subs:
            return
        self._replay_last = replay_last

        # Always try to claim the local publisher slot — best-effort.
        # In single-publisher mode this is the only publisher; in
        # sharded mode it's our shard.  If another process already
        # owns the slot (single-publisher mode, multi-process), this
        # process becomes subscriber-only.
        try:
            self.ensure_publisher()
        except mmbus.AlreadyPublishingError:
            logger.info(
                "mmcast: %r already has a publisher in another process; "
                "this Broadcast is subscriber-only for %r",
                self.name,
                self.name,
            )

        peers = self._broadcast._peers_or_default()
        for peer in peers:
            topic = self._topic_for(peer)
            mmbus_sub = await self._open_mmbus_sub(
                topic,
                replay_last=replay_last,
                connect_timeout_secs=connect_timeout_secs,
            )
            if mmbus_sub is None:
                continue  # peer was offline; warning already logged
            self._mmbus_subs[peer or "_solo"] = mmbus_sub
            self._fanout_tasks.append(
                asyncio.create_task(
                    self._fanout(mmbus_sub),
                    name=f"mmcast-fanout:{topic}",
                )
            )

    async def _open_mmbus_sub(
        self,
        topic: str,
        *,
        replay_last: int,
        connect_timeout_secs: float,
    ):
        """Open one mmbus subscription on ``topic``, with optional in-ring
        history replay.

        Returns ``None`` if the topic has no publisher yet (logged at
        WARNING; the caller proceeds with whatever peers are online).
        """
        bus = self._broadcast._bus
        loop = asyncio.get_event_loop()
        try:
            if replay_last > 0:
                # `subscribe_with_history` is sync — wrap it like
                # mmbus.subscribe_async wraps the bare subscribe.
                sync_sub = await loop.run_in_executor(
                    None,
                    lambda: bus.subscribe_with_history(
                        topic,
                        n_messages_back=replay_last,
                        timeout_secs=connect_timeout_secs,
                    ),
                )
                return mmbus.AsyncSubscription(sync_sub)
            return await bus.subscribe_async(
                topic, timeout_secs=connect_timeout_secs
            )
        except mmbus.ConnectTimeoutError:
            logger.warning(
                "mmcast: peer for topic %r not online; skipping",
                topic,
            )
            return None

    async def _fanout(self, mmbus_sub) -> None:
        try:
            async for data in mmbus_sub:
                # Snapshot to allow detach during iteration.
                for sub in tuple(self._subs):
                    sub._enqueue(data)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("mmcast: fanout loop on %r failed", self.name)
            for sub in tuple(self._subs):
                sub._close_from_fanout()

    def _attach(self, sub: Subscription) -> None:
        self._subs.add(sub)

    def _detach(self, sub: Subscription) -> None:
        self._subs.discard(sub)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for task in self._fanout_tasks:
            task.cancel()
        for task in self._fanout_tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        for mmbus_sub in self._mmbus_subs.values():
            try:
                await mmbus_sub.__aexit__(None, None, None)
            except Exception:
                logger.exception(
                    "mmcast: error closing mmbus sub for %r", self.name
                )
        for sub in tuple(self._subs):
            sub._close_from_fanout()
        # TopicPublisher releases on Bus drop — no explicit close.
        self._publisher = None
        self._mmbus_subs.clear()
        self._fanout_tasks.clear()


# ──────────────────────────────────────────────────────────────────────────
# Broadcast — public API
# ──────────────────────────────────────────────────────────────────────────


class Broadcast:
    """ASGI WebSocket-shaped broadcast over mmbus.

    See ``docs/spec-mmcast-v0.1.md`` for the contract.

    Args:
        name: mmbus bus name (process-shared namespace).
        worker_id: This process's shard ID, used as the publisher topic
            suffix (publishes go to ``<channel>.<worker_id>``).  ``None``
            (default) = single-publisher mode (one process owns the topic).
        peers: All shard IDs to fan in from on subscribe.  When set,
            ``subscribe("chat")`` opens one mmbus subscription per peer
            and merges them.  ``None`` = subscribe to the unsharded topic
            in single-publisher mode, or just our own shard in sharded
            mode without peer fan-in (rarely useful — set it for the
            multi-worker WS broadcast pattern).
        backpressure: Forwarded to ``mmbus.Bus``.  Defaults to
            ``"drop_oldest"`` — the right policy for broadcast: a
            disconnected period shouldn't error out producers.
        **bus_kwargs: Remaining kwargs forwarded to ``mmbus.Bus``.
    """

    def __init__(
        self,
        name: str,
        *,
        worker_id: Optional[str] = None,
        peers: Optional[Iterable[str]] = None,
        backpressure: str = "drop_oldest",
        **bus_kwargs: Any,
    ) -> None:
        self._name = name
        self._worker_id = worker_id
        self._peers: Optional[List[str]] = (
            list(peers) if peers is not None else None
        )
        self._bus_kwargs = {"backpressure": backpressure, **bus_kwargs}
        self._bus: Optional[mmbus.Bus] = None
        self._channels: Dict[str, _Channel] = {}
        # Single lock guarding the channel registry — creation is rare
        # relative to publish/recv, so contention is not a concern.
        self._channels_lock = asyncio.Lock()
        self._closed = False

    def _peers_or_default(self) -> List[Optional[str]]:
        """Peers to subscribe to.  In single-publisher mode (no
        worker_id), there's exactly one logical "peer" — the unsharded
        topic, represented as ``None``."""
        if self._worker_id is None and self._peers is None:
            return [None]
        return list(self._peers) if self._peers is not None else []

    # ── lifecycle ────────────────────────────────────────────────────────
    async def __aenter__(self) -> "Broadcast":
        if self._bus is not None:
            return self  # idempotent re-entry
        self._bus = mmbus.Bus(self._name, **self._bus_kwargs)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._closed:
            return
        self._closed = True
        async with self._channels_lock:
            channels = list(self._channels.values())
            self._channels.clear()
        for ch in channels:
            await ch.close()
        # `mmbus.Bus` has no explicit close — it drops on GC.
        self._bus = None

    # ── publish ──────────────────────────────────────────────────────────
    async def publish(self, channel: str, data: bytes) -> None:
        if self._closed or self._bus is None:
            raise BroadcastClosedError("Broadcast is not open")
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError(
                f"data must be bytes/bytearray/memoryview, not {type(data).__name__}"
            )
        ch = await self._get_or_create_channel(channel)
        # Sync call — `TopicPublisher.publish` releases the GIL around
        # the wakeup syscall, but the ring write itself is sub-µs.
        # Calling from the event loop is correct.
        ch.publish(bytes(data))

    async def publish_json(self, channel: str, obj: Any) -> None:
        await self.publish(
            channel, json.dumps(obj, separators=(",", ":")).encode()
        )

    # ── subscribe ────────────────────────────────────────────────────────
    async def subscribe(
        self,
        channel: str,
        *,
        replay_last: int = 0,  # honoured by T5
        slow_policy: str = "drop_oldest",
        queue_depth: int = 1024,
        connect_timeout_secs: float = 30.0,
    ) -> Subscription:
        """Open a consumer subscription on ``channel``.

        Returns a :class:`Subscription` — use as ``async with`` + ``async for``.
        """
        if self._closed or self._bus is None:
            raise BroadcastClosedError("Broadcast is not open")
        if replay_last < 0:
            raise ValueError("replay_last must be >= 0")
        if queue_depth < 1:
            raise ValueError("queue_depth must be >= 1")

        ch = await self._get_or_create_channel(channel)
        # ``replay_last`` is honoured at channel-open time — the first
        # subscriber to a channel within this Broadcast determines the
        # in-ring snapshot.  Subsequent subscribers see live-only
        # delivery.  If a later subscriber asks for a different
        # replay_last we warn but don't reopen — keeping the v0.1
        # semantics simple.  (Per-subscriber replay needs an in-process
        # buffer; tracked for v0.2.)
        if ch._mmbus_subs and replay_last != ch._replay_last:
            logger.warning(
                "mmcast: subscribe(%r, replay_last=%d) on a channel already "
                "opened with replay_last=%d; this subscriber sees live only",
                channel,
                replay_last,
                ch._replay_last,
            )
        await ch.start_subscriptions(
            connect_timeout_secs=connect_timeout_secs,
            replay_last=replay_last,
        )
        sub = Subscription(ch, queue_depth=queue_depth, slow_policy=slow_policy)
        ch._attach(sub)
        return sub

    async def prepare(self, *channels: str) -> None:
        """Claim publisher slots for ``channels`` at app startup.

        Optional but useful: ensures subscriptions opened later (e.g.
        the first WebSocket connection) don't time out waiting for a
        publisher to exist.  Idempotent.
        """
        for ch_name in channels:
            ch = await self._get_or_create_channel(ch_name)
            ch.ensure_publisher()

    async def _get_or_create_channel(self, channel: str) -> _Channel:
        async with self._channels_lock:
            ch = self._channels.get(channel)
            if ch is None:
                ch = _Channel(self, channel)
                self._channels[channel] = ch
            return ch

    # ── presence (T6) ────────────────────────────────────────────────────
    def presence(
        self,
        channel: str,
        *,
        member_id: str,
        ttl_secs: float = 15.0,
        heartbeat_secs: float = 5.0,
    ):
        raise NotImplementedError("Broadcast.presence lands in T6")
