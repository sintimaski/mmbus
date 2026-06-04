"""T6 — Presence tracking.

Implements the opt-in ``Broadcast.presence`` surface.  Single-process
semantics: multiple members within one Broadcast see each other via the
shared ``_presence:<channel>`` mmbus topic.  Multi-process presence
(per-member sharding) is out of scope for v0.1 — same constraint as
broadcast.publish per the spec.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import TYPE_CHECKING, Dict, Optional

from ._event import PresenceChange

if TYPE_CHECKING:
    from ._broadcast import Broadcast

logger = logging.getLogger("mmbus_cast")


class Presence:
    """Per-member presence handle for a channel.

    On enter: publishes a join heartbeat, starts a heartbeat loop, opens
    a subscription on the presence topic to track peer joins/leaves.

    On exit: publishes a leave, stops heartbeating, closes the
    subscription.

    Async context manager + async iterator over :class:`PresenceChange`.
    """

    def __init__(
        self,
        broadcast: "Broadcast",
        channel: str,
        *,
        member_id: str,
        ttl_secs: float,
        heartbeat_secs: float,
    ) -> None:
        if ttl_secs <= 0:
            raise ValueError("ttl_secs must be > 0")
        if heartbeat_secs <= 0:
            raise ValueError("heartbeat_secs must be > 0")
        if heartbeat_secs >= ttl_secs:
            # Standard "heartbeat at ~1/3 TTL" rule of thumb: too close
            # to TTL and a single dropped heartbeat triggers a false
            # eviction.
            logger.warning(
                "mmcast: heartbeat_secs=%.3f >= ttl_secs=%.3f — false "
                "evictions likely; recommend heartbeat_secs <= ttl_secs/3",
                heartbeat_secs,
                ttl_secs,
            )
        self._broadcast = broadcast
        self._channel = channel
        self._member_id = member_id
        self._ttl = ttl_secs
        self._heartbeat = heartbeat_secs
        self._presence_topic = f"_presence:{channel}"

        # member_id -> last heartbeat ts (monotonic)
        self._members: Dict[str, float] = {}
        self._changes: asyncio.Queue[PresenceChange] = asyncio.Queue()

        self._sub = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._consume_task: Optional[asyncio.Task] = None
        self._eviction_task: Optional[asyncio.Task] = None
        self._closed = False

    @property
    def members(self) -> set:
        """Snapshot of currently-active members (includes self).

        Note: this returns a fresh set; the underlying state can update
        concurrently with the heartbeat / consume tasks.
        """
        return set(self._members.keys())

    # ── lifecycle ────────────────────────────────────────────────────────
    async def __aenter__(self) -> "Presence":
        # Open the subscription FIRST so the consume loop catches our
        # own initial heartbeat (and peers' messages already arriving).
        self._sub = await self._broadcast.subscribe(self._presence_topic)
        # Track self eagerly — the round-trip through mmbus+fanout is
        # async, and ``members`` should reflect self immediately on entry.
        self._members[self._member_id] = time.monotonic()
        self._consume_task = asyncio.create_task(
            self._consume_loop(), name=f"mmcast-presence-consume:{self._channel}"
        )
        self._eviction_task = asyncio.create_task(
            self._eviction_loop(), name=f"mmcast-presence-evict:{self._channel}"
        )
        await self._publish("join")
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name=f"mmcast-presence-hb:{self._channel}"
        )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._closed:
            return
        self._closed = True
        # Best-effort leave so peers see us go before TTL expiry.
        try:
            await self._publish("leave")
        except Exception:
            logger.exception("mmcast: presence leave publish failed")

        tasks = [
            self._heartbeat_task,
            self._consume_task,
            self._eviction_task,
        ]
        for t in tasks:
            if t is not None:
                t.cancel()
        for t in tasks:
            if t is not None:
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass

        if self._sub is not None:
            try:
                await self._sub.__aexit__(None, None, None)
            except Exception:
                logger.exception("mmcast: presence sub close failed")

    # ── publish helpers ─────────────────────────────────────────────────
    async def _publish(self, kind: str) -> None:
        """``kind`` is ``"join"``, ``"heartbeat"``, or ``"leave"``."""
        payload = json.dumps(
            {"member": self._member_id, "kind": kind, "ts": time.time()},
            separators=(",", ":"),
        ).encode()
        await self._broadcast.publish(self._presence_topic, payload)

    # ── background loops ────────────────────────────────────────────────
    async def _heartbeat_loop(self) -> None:
        try:
            while not self._closed:
                await asyncio.sleep(self._heartbeat)
                if self._closed:
                    return
                try:
                    await self._publish("heartbeat")
                except Exception:
                    logger.exception("mmcast: heartbeat publish failed")
        except asyncio.CancelledError:
            raise

    async def _consume_loop(self) -> None:
        try:
            assert self._sub is not None
            async for event in self._sub:
                try:
                    msg = json.loads(event.data)
                    member = msg["member"]
                    kind = msg["kind"]
                except (KeyError, ValueError) as e:
                    logger.warning(
                        "mmcast: malformed presence message on %r: %s",
                        self._presence_topic,
                        e,
                    )
                    continue
                if kind in ("join", "heartbeat"):
                    is_new = member not in self._members
                    self._members[member] = time.monotonic()
                    if is_new:
                        await self._changes.put(
                            PresenceChange(member=member, joined=True)
                        )
                elif kind == "leave":
                    if self._members.pop(member, None) is not None:
                        await self._changes.put(
                            PresenceChange(member=member, joined=False)
                        )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("mmcast: presence consume loop failed")

    async def _eviction_loop(self) -> None:
        """Periodically scan for members whose last heartbeat is older
        than TTL and emit a leave event for them."""
        try:
            # Scan twice per TTL so eviction fires within ~1.5×TTL of
            # the missed heartbeat — matches the standard eventually-
            # consistent presence pattern.
            scan_interval = max(self._ttl / 2, 0.05)
            while not self._closed:
                await asyncio.sleep(scan_interval)
                if self._closed:
                    return
                now = time.monotonic()
                expired = [
                    m
                    for m, ts in self._members.items()
                    if now - ts > self._ttl
                ]
                for m in expired:
                    del self._members[m]
                    await self._changes.put(
                        PresenceChange(member=m, joined=False)
                    )
        except asyncio.CancelledError:
            raise

    # ── async iterator ──────────────────────────────────────────────────
    def __aiter__(self) -> "Presence":
        return self

    async def __anext__(self) -> PresenceChange:
        if self._closed and self._changes.empty():
            raise StopAsyncIteration
        return await self._changes.get()
