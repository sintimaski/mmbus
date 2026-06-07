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
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Set,
)

import mmbus

from ._event import Event
from ._validate import validate_channel_name

if TYPE_CHECKING:
    from ._presence import Presence

logger = logging.getLogger("mmbus_cast")


class BroadcastClosedError(RuntimeError):
    """Raised when publish/subscribe is called on a closed Broadcast."""


class SlowConsumer(RuntimeWarning):
    """Emitted (via ``logging`` + per-Subscription ``slow_count``) when a
    consumer's outbound queue overflows.  Whether the oldest is dropped,
    the newest is dropped, or the consumer is disconnected is governed by
    the ``slow_policy`` argument to :meth:`Broadcast.subscribe`."""


# Valid slow-consumer policies, defined once so the constructor and the
# enforcement switch can't drift.
_SLOW_POLICIES = ("drop_oldest", "drop_newest", "disconnect")

# How often a channel retries peer shards that were offline at subscribe
# time.  Lets a multi-worker deployment converge when workers start at
# staggered times, without the caller having to coordinate startup order.
_PEER_RECONNECT_INTERVAL_SECS = 1.0

# Per-attempt connect timeout for a *remote* peer shard.  Kept short: a
# remote peer that isn't up yet shouldn't block the local subscribe —
# the reconnect loop picks it up later.  The caller's full
# ``connect_timeout_secs`` still applies to the local shard and to
# single-publisher mode, where waiting for the one publisher is the
# intended behaviour.
_PEER_CONNECT_TIMEOUT_SECS = 1.0


# ──────────────────────────────────────────────────────────────────────────
# Subscription — the per-consumer handle returned by Broadcast.subscribe()
# ──────────────────────────────────────────────────────────────────────────


class Subscription:
    """One consumer's view of a channel.  Async context manager + async
    iterator over :class:`Event`.

    Not constructed directly — get one from :meth:`Broadcast.subscribe`.

    Closure is signalled via a dedicated :class:`asyncio.Event` rather
    than a queue sentinel: the iterator races a queue ``get`` against the
    close event, so a full queue can never strand the consumer (the close
    signal does not have to fit in the queue).
    """

    def __init__(
        self,
        channel: "_Channel",
        *,
        queue_depth: int,
        slow_policy: str,
    ) -> None:
        if slow_policy not in _SLOW_POLICIES:
            raise ValueError(
                f"slow_policy must be one of {_SLOW_POLICIES}; got {slow_policy!r}"
            )
        self._channel = channel
        self._queue: "asyncio.Queue[bytes]" = asyncio.Queue(maxsize=queue_depth)
        self._slow_policy = slow_policy
        self._closed = False
        self._close_event = asyncio.Event()
        # Public counters (spec § Observability).
        self.slow_count = 0
        self.delivered_count = 0

    # ── fan-out path (called synchronously from _Channel's fanout task) ──
    def _enqueue(self, data: bytes) -> None:
        if self._closed:
            return
        try:
            self._queue.put_nowait(data)
        except asyncio.QueueFull:
            self.slow_count += 1
            self._on_full(data)

    def _on_full(self, data: bytes) -> None:
        if self._slow_policy == "drop_oldest":
            # Evict one, then enqueue the newcomer.  Both can race a
            # concurrent consumer; swallow the resulting transient errors.
            try:
                self._queue.get_nowait()
                self._queue.put_nowait(data)
            except (asyncio.QueueEmpty, asyncio.QueueFull):
                pass
            logger.warning(
                "mmcast: slow consumer on %r — dropped oldest (slow_count=%d)",
                self._channel.name,
                self.slow_count,
            )
        elif self._slow_policy == "drop_newest":
            # Newcomer is simply not enqueued.
            logger.warning(
                "mmcast: slow consumer on %r — dropped newest (slow_count=%d)",
                self._channel.name,
                self.slow_count,
            )
        else:  # "disconnect" — validated in __init__, so no other case
            logger.warning(
                "mmcast: slow consumer on %r — disconnecting (slow_count=%d)",
                self._channel.name,
                self.slow_count,
            )
            self._close_from_fanout()

    def _close_from_fanout(self) -> None:
        """Signal close from the fan-out task (sync context, must not raise).

        Sets the close event; the iterator observes it and stops after
        draining whatever is already queued.  No queue manipulation, so
        there is no race in which the close signal fails to land.
        """
        self._closed = True
        self._close_event.set()

    # ── async iterator ──────────────────────────────────────────────────
    def __aiter__(self) -> "Subscription":
        return self

    async def __anext__(self) -> Event:
        # Drain-then-close: always deliver anything already queued before
        # honouring a close, so a graceful close doesn't drop buffered
        # messages.
        try:
            data = self._queue.get_nowait()
            self.delivered_count += 1
            return Event(data=data)
        except asyncio.QueueEmpty:
            pass
        if self._closed:
            raise StopAsyncIteration

        # Race the next item against the close signal.  Whichever fires
        # first wins; we never lose a delivered item to the close path.
        get_task: "asyncio.Task[bytes]" = asyncio.ensure_future(self._queue.get())
        close_wait: "asyncio.Task[bool]" = asyncio.ensure_future(
            self._close_event.wait()
        )
        try:
            await asyncio.wait(
                {get_task, close_wait}, return_when=asyncio.FIRST_COMPLETED
            )
        except asyncio.CancelledError:
            get_task.cancel()
            close_wait.cancel()
            raise
        finally:
            close_wait.cancel()

        if get_task.done() and not get_task.cancelled():
            data = get_task.result()
            self.delivered_count += 1
            return Event(data=data)

        # Close won.  Cancel the pending get, but recover an item if the
        # get had already dequeued one during cancellation (the known
        # asyncio.Queue get-cancel/put item-loss window).
        get_task.cancel()
        try:
            data = await get_task
            self.delivered_count += 1
            return Event(data=data)
        except (asyncio.CancelledError, Exception):
            pass
        try:
            data = self._queue.get_nowait()
            self.delivered_count += 1
            return Event(data=data)
        except asyncio.QueueEmpty:
            raise StopAsyncIteration

    # ── async context manager ────────────────────────────────────────────
    async def __aenter__(self) -> "Subscription":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self._closed = True
        self._close_event.set()
        self._channel._detach(self)


class _SubscriptionContext:
    """Return type of :meth:`Broadcast.subscribe`.

    Supports both call styles, like ``aiohttp``'s request objects::

        sub = await bc.subscribe("chat")            # manual lifecycle
        async with bc.subscribe("chat") as sub:     # auto-close on exit

    Argument validation happens eagerly when ``subscribe`` is *called*
    (before this object is returned), so invalid arguments raise
    immediately regardless of which style the caller uses.  The actual
    mmbus subscription is opened lazily on first ``await`` / ``__aenter__``.
    """

    __slots__ = ("_opener", "_sub")

    def __init__(self, opener: Callable[[], Awaitable[Subscription]]) -> None:
        self._opener = opener
        self._sub: Optional[Subscription] = None

    async def _open(self) -> Subscription:
        if self._sub is None:
            self._sub = await self._opener()
        return self._sub

    def __await__(self) -> Generator[Any, None, Subscription]:
        return self._open().__await__()

    async def __aenter__(self) -> Subscription:
        return await self._open()

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._sub is not None:
            await self._sub.__aexit__(exc_type, exc, tb)


# ──────────────────────────────────────────────────────────────────────────
# _Channel — per-channel fan-out hub (internal)
# ──────────────────────────────────────────────────────────────────────────


class _Channel:
    """Internal: holds the publisher slot + one mmbus subscription per
    peer shard + the fan-out tasks.

    Lifetime: created on first publish/subscribe to a channel name within
    a Broadcast; torn down only when the Broadcast closes.  Keeping it
    alive across "no consumers" gaps is intentional — it preserves
    in-ring history that ``replay_last`` relies on and avoids rebuilding
    mmbus subscriptions on burst churn.
    """

    def __init__(self, broadcast: "Broadcast", name: str) -> None:
        self._broadcast = broadcast
        self.name = name  # logical name; physical topics are `name.<peer>`
        self._subs: Set[Subscription] = set()
        self._publisher: Optional[mmbus.TopicPublisher] = None
        self._publisher_attempted = False
        # One mmbus subscription per peer shard, keyed by peer id
        # ("_solo" for single-publisher mode).
        self._mmbus_subs: Dict[str, Any] = {}
        self._fanout_tasks: List[asyncio.Task] = []
        # Serialises start_subscriptions so concurrent subscribe() calls
        # can't open duplicate mmbus subscriptions for the same peer.
        self._start_lock = asyncio.Lock()
        self._closed = False
        # ``replay_last`` is per-channel (not per-Subscription) — the in-
        # ring history snapshot happens at channel-open time.  ``None``
        # until the first subscribe; subsequent subscribers with a
        # different value get a warning (see ``Broadcast.subscribe``).
        self._replay_last: Optional[int] = None
        # Background loop that retries peer shards offline at subscribe
        # time, so a staggered multi-worker deployment converges without
        # caller-side startup coordination.  Started lazily.
        self._reconnect_task: Optional[asyncio.Task] = None
        self._connect_timeout_secs: float = 30.0

    def _topic_for(self, peer: Optional[str]) -> str:
        """Physical mmbus topic name for ``peer`` shard.

        Single-publisher mode (``peer is None``) uses the unsharded
        ``name``; sharded mode uses ``name.<peer>``.
        """
        return self.name if peer is None else f"{self.name}.{peer}"

    def ensure_publisher(self) -> None:
        """Claim this process's publisher slot (idempotent).

        In sharded mode the slot is ``name.<worker_id>``; in single-
        publisher mode it's just ``name``.  ``mmbus.Bus`` caches the
        ``TopicPublisher`` so reclaim is cheap.
        """
        if self._publisher is not None:
            return
        topic = self._topic_for(self._broadcast._worker_id)
        self._publisher = self._broadcast._bus.topic(topic)

    def publish(self, data: bytes) -> None:
        self.ensure_publisher()
        assert self._publisher is not None  # for type checkers
        self._publisher.publish(data)

    def _missing_peers(self) -> List[Optional[str]]:
        """Expected peers that don't yet have a live mmbus subscription."""
        expected = self._broadcast._peers_or_default()
        return [
            p
            for p in expected
            if (p if p is not None else "_solo") not in self._mmbus_subs
        ]

    async def start_subscriptions(
        self,
        *,
        connect_timeout_secs: float,
        replay_last: int,
    ) -> None:
        """Open an mmbus subscription per not-yet-connected peer shard.

        Idempotent for already-connected peers, and **retries** peers
        that were offline on a previous call — so a worker that starts
        late is picked up by the next ``subscribe`` instead of being
        dropped forever.  Pending peers are connected concurrently, so
        the first ``subscribe`` of a cold multi-worker app doesn't pay
        ``connect_timeout_secs`` once per offline peer sequentially.
        """
        async with self._start_lock:
            if self._closed:
                return
            if self._replay_last is None:
                self._replay_last = replay_last
            self._connect_timeout_secs = connect_timeout_secs
            # Best-effort publisher claim, once.
            if not self._publisher_attempted:
                self._publisher_attempted = True
                try:
                    self.ensure_publisher()
                except mmbus.AlreadyPublishingError:
                    logger.info(
                        "mmcast: %r already has a publisher in another "
                        "process; this Broadcast is subscriber-only for %r",
                        self.name,
                        self.name,
                    )

            peers = self._broadcast._peers_or_default()
            pending = [
                p for p in peers if (p if p is not None else "_solo") not in self._mmbus_subs
            ]
            if not pending:
                return

            # Connect all pending peers concurrently.  _open_mmbus_sub
            # catches its own ConnectTimeoutError and returns None, so
            # gather never raises for an offline peer.  Remote peer shards
            # use a short timeout (reconnect loop handles convergence);
            # the local shard and single-publisher topic use the caller's
            # full timeout, where waiting for the publisher is intended.
            local = self._broadcast._worker_id
            results = await asyncio.gather(
                *(
                    self._open_mmbus_sub(
                        self._topic_for(p),
                        replay_last=self._replay_last,
                        connect_timeout_secs=(
                            connect_timeout_secs
                            if (p is None or p == local)
                            else min(connect_timeout_secs, _PEER_CONNECT_TIMEOUT_SECS)
                        ),
                    )
                    for p in pending
                )
            )
            for peer, mmbus_sub in zip(pending, results):
                if mmbus_sub is None:
                    continue  # peer offline; the reconnect loop retries it
                key = peer if peer is not None else "_solo"
                self._mmbus_subs[key] = mmbus_sub
                self._fanout_tasks.append(
                    asyncio.create_task(
                        self._fanout(mmbus_sub),
                        name=f"mmcast-fanout:{self._topic_for(peer)}",
                    )
                )

            # If any peer is still missing, ensure the reconnect loop is
            # running so it gets picked up when it comes online.
            if self._missing_peers() and self._reconnect_task is None:
                self._reconnect_task = asyncio.create_task(
                    self._reconnect_loop(),
                    name=f"mmcast-reconnect:{self.name}",
                )

    async def _reconnect_loop(self) -> None:
        """Periodically retry peer shards that were offline at subscribe.

        Runs until every expected peer is connected (the convergence
        case for staggered multi-worker startup) or the channel closes.
        A peer that never appears keeps being retried at the reconnect
        interval — cheap, and correct for "the worker will start later".
        """
        try:
            while not self._closed and self._missing_peers():
                await asyncio.sleep(_PEER_RECONNECT_INTERVAL_SECS)
                if self._closed:
                    return
                await self.start_subscriptions(
                    connect_timeout_secs=self._connect_timeout_secs,
                    replay_last=self._replay_last or 0,
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("mmcast: reconnect loop on %r failed", self.name)
        finally:
            self._reconnect_task = None

    async def _open_mmbus_sub(
        self,
        topic: str,
        *,
        replay_last: int,
        connect_timeout_secs: float,
    ):
        """Open one mmbus subscription on ``topic``, optional in-ring replay.

        Returns ``None`` if the topic has no publisher yet (logged at
        WARNING; caller proceeds with whatever peers are online and
        retries the rest on the next ``subscribe``).
        """
        bus = self._broadcast._bus
        loop = asyncio.get_running_loop()
        try:
            if replay_last > 0:
                # `subscribe_with_history` is sync — offload like
                # mmbus.subscribe_async does for the bare subscribe.
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
                "mmcast: peer for topic %r not online; will retry on next "
                "subscribe",
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
        tasks = list(self._fanout_tasks)
        if self._reconnect_task is not None:
            tasks.append(self._reconnect_task)
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("mmcast: task error on close for %r", self.name)
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
            in single-publisher mode.
        backpressure: Forwarded to ``mmbus.Bus``.  Defaults to
            ``"drop_oldest"`` — the right policy for broadcast: a
            disconnected period shouldn't error out producers.
        **bus_kwargs: Remaining kwargs forwarded to ``mmbus.Bus``.

    Raises:
        InvalidChannelError: if ``worker_id`` or any ``peers`` entry is
            not a path-safe identifier (they become mmbus topic suffixes
            and hence on-disk path components).
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
        # worker_id / peers become topic suffixes → path components.
        # Validate with the same allowlist as channel names (internal=True
        # since they're operator-supplied, not end-user-supplied).
        if worker_id is not None:
            validate_channel_name(worker_id, internal=True)
        self._name = name
        self._worker_id = worker_id
        if peers is not None:
            peers = [validate_channel_name(p, internal=True) for p in peers]
            self._peers: Optional[List[str]] = list(peers)
        else:
            self._peers = None
        self._bus_kwargs = {"backpressure": backpressure, **bus_kwargs}
        self._bus: Optional[mmbus.Bus] = None
        self._channels: Dict[str, _Channel] = {}
        # Guards the channel registry.  Creation is rare relative to
        # publish/recv, so contention is not a concern.
        self._channels_lock = asyncio.Lock()
        self._closed = False

    def _peers_or_default(self) -> List[Optional[str]]:
        """Peers to subscribe to.  In single-publisher mode (no
        worker_id, no peers), there's one logical "peer": the unsharded
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
        """Publish ``data`` (bytes) to ``channel``.

        Raises :class:`~mmbus_cast._validate.InvalidChannelError` for a
        reserved or malformed channel name, :class:`BroadcastClosedError`
        if the Broadcast is closed, and propagates mmbus errors
        (``BusFullError``, ``MessageTooLargeError``).
        """
        validate_channel_name(channel)
        await self._publish_internal(channel, data)

    async def publish_json(self, channel: str, obj: Any) -> None:
        """Publish ``obj`` as compact UTF-8 JSON to ``channel``."""
        validate_channel_name(channel)
        await self._publish_internal(
            channel, json.dumps(obj, separators=(",", ":")).encode()
        )

    async def _publish_internal(self, channel: str, data: bytes) -> None:
        """Publish without the reserved-prefix check — for mmcast
        subsystems (presence) that legitimately use ``_…`` topics."""
        if self._closed or self._bus is None:
            raise BroadcastClosedError("Broadcast is not open")
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError(
                f"data must be bytes/bytearray/memoryview, not {type(data).__name__}"
            )
        ch = await self._get_or_create_channel(channel)
        # Avoid a copy when we already hold an immutable bytes object;
        # mmbus needs an owned buffer for bytearray/memoryview.
        payload = data if type(data) is bytes else bytes(data)
        ch.publish(payload)

    # ── subscribe ────────────────────────────────────────────────────────
    def subscribe(
        self,
        channel: str,
        *,
        replay_last: int = 0,
        slow_policy: str = "drop_oldest",
        queue_depth: int = 1024,
        connect_timeout_secs: float = 30.0,
    ) -> _SubscriptionContext:
        """Open a consumer subscription on ``channel``.

        Returns an awaitable async-context-manager, usable either way::

            sub = await bc.subscribe("chat")
            async with bc.subscribe("chat") as sub:
                async for event in sub:
                    ...

        Arguments are validated eagerly (this call), so a bad channel
        name or argument raises here, before any ``await``.
        """
        validate_channel_name(channel)
        return self._subscribe_internal(
            channel,
            replay_last=replay_last,
            slow_policy=slow_policy,
            queue_depth=queue_depth,
            connect_timeout_secs=connect_timeout_secs,
        )

    def _subscribe_internal(
        self,
        channel: str,
        *,
        replay_last: int = 0,
        slow_policy: str = "drop_oldest",
        queue_depth: int = 1024,
        connect_timeout_secs: float = 30.0,
    ) -> _SubscriptionContext:
        """Subscribe without the reserved-prefix check (presence uses it)."""
        if self._closed or self._bus is None:
            raise BroadcastClosedError("Broadcast is not open")
        if replay_last < 0:
            raise ValueError("replay_last must be >= 0")
        if queue_depth < 1:
            raise ValueError("queue_depth must be >= 1")
        if slow_policy not in _SLOW_POLICIES:
            raise ValueError(
                f"slow_policy must be one of {_SLOW_POLICIES}; got {slow_policy!r}"
            )

        async def _opener() -> Subscription:
            ch = await self._get_or_create_channel(channel)
            # ``replay_last`` is honoured at channel-open time — the first
            # subscriber to a channel within this Broadcast determines the
            # in-ring snapshot.  A later subscriber asking for a different
            # value gets a warning but live-only delivery (per-subscriber
            # replay needs an in-process buffer; tracked for v0.2).
            if ch._replay_last is not None and replay_last != ch._replay_last:
                logger.warning(
                    "mmcast: subscribe(%r, replay_last=%d) on a channel "
                    "already opened with replay_last=%d; this subscriber "
                    "sees live only",
                    channel,
                    replay_last,
                    ch._replay_last,
                )
            await ch.start_subscriptions(
                connect_timeout_secs=connect_timeout_secs,
                replay_last=replay_last,
            )
            sub = Subscription(
                ch, queue_depth=queue_depth, slow_policy=slow_policy
            )
            ch._attach(sub)
            return sub

        return _SubscriptionContext(_opener)

    async def prepare(self, *channels: str) -> None:
        """Claim publisher slots for ``channels`` at app startup.

        Ensures subscriptions opened later (e.g. the first WebSocket
        connection) don't time out waiting for a publisher to exist.
        Idempotent.
        """
        for ch_name in channels:
            validate_channel_name(ch_name)
            ch = await self._get_or_create_channel(ch_name)
            ch.ensure_publisher()

    async def _get_or_create_channel(self, channel: str) -> _Channel:
        async with self._channels_lock:
            # Re-check under the lock: a concurrent __aexit__ may have
            # closed us between the caller's check and acquiring the lock.
            if self._closed or self._bus is None:
                raise BroadcastClosedError("Broadcast is not open")
            ch = self._channels.get(channel)
            if ch is None:
                ch = _Channel(self, channel)
                self._channels[channel] = ch
            return ch

    # ── presence ─────────────────────────────────────────────────────────
    def presence(
        self,
        channel: str,
        *,
        member_id: str,
        ttl_secs: float = 15.0,
        heartbeat_secs: float = 5.0,
        changes_queue_max: int = 4096,
    ) -> "Presence":
        """Open a presence handle for ``channel`` as ``member_id``.

        Async context manager + async iterator over :class:`PresenceChange`.
        ``changes_queue_max`` bounds the buffered join/leave events (drop-
        oldest on overflow) so a caller that never iterates the handle
        can't be made to grow memory without bound.
        See :class:`~mmbus_cast._presence.Presence` for details.
        """
        validate_channel_name(channel)
        if self._closed or self._bus is None:
            raise BroadcastClosedError("Broadcast is not open")
        # Lazy import to avoid a load-time cycle between _broadcast and
        # _presence.
        from ._presence import Presence

        return Presence(
            self,
            channel,
            member_id=member_id,
            ttl_secs=ttl_secs,
            heartbeat_secs=heartbeat_secs,
            changes_queue_max=changes_queue_max,
        )
