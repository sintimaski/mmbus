"""Presence tracking.

Implements the opt-in ``Broadcast.presence`` surface.  Multiple members
within one Broadcast see each other via a shared internal presence
topic.  Cross-process presence works transparently when the surrounding
``Broadcast`` is in sharded mode (``worker_id`` + ``peers`` set): the
presence topic rides the same ``_Channel`` machinery as chat broadcasts,
so each member's joins, heartbeats, and leaves fan out to every peer
worker.  Verified by ``tests/test_presence_multiworker.py``.

Hardening (same-host trust model still applies, but a single bad actor
or bug inside the boundary must not be able to take the subsystem down):

* Wire messages are parsed by a single pure function that never raises —
  malformed input (non-JSON, non-dict, wrong types, oversized, unknown
  kind) is dropped, not propagated, so one bad message can't kill the
  consume loop.
* The change queue is bounded (drop-oldest on overflow) so a consumer
  that never iterates ``async for change in p`` can't be made to OOM.
* The member table is capped and member-id length is bounded, so a flood
  of distinct ids can't exhaust memory before TTL eviction fires.
* A member never evicts *itself* on TTL — see ``_eviction_loop``.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import TYPE_CHECKING, Dict, Optional, Tuple

from ._event import PresenceChange
from ._validate import RESERVED_PREFIX

if TYPE_CHECKING:
    from ._broadcast import Broadcast

logger = logging.getLogger("mmbus_cast")


# ── hardening limits ───────────────────────────────────────────────────────
# A presence record is a tiny JSON object; anything larger is junk.
_MAX_PRESENCE_PAYLOAD = 4096
# Bound member-id length (also the validated max channel-name length).
_MAX_MEMBER_ID_LEN = 256
# Cap distinct tracked members; beyond this, new ids are ignored (existing
# live members are never evicted to make room for a flood).
_MAX_MEMBERS = 10_000
# Default bound on the change-event queue (drop-oldest on overflow).
_DEFAULT_CHANGES_QUEUE_MAX = 4096
# Floor on heartbeat interval — guards against a misconfiguration that
# would flood the ring (e.g. heartbeat_secs=0).
_MIN_HEARTBEAT_SECS = 0.01

_VALID_KINDS = ("join", "heartbeat", "leave")


def _parse_presence_message(data: bytes) -> Optional[Tuple[str, str]]:
    """Parse a presence wire message into ``(member, kind)`` or ``None``.

    Pure and total: returns ``None`` for *anything* malformed — oversized,
    non-JSON, invalid UTF-8, non-dict, missing/!str fields, over-length
    member, or unknown kind — instead of raising.  This is the single
    point that makes the consume loop robust to arbitrary bytes on the
    presence topic.
    """
    if len(data) > _MAX_PRESENCE_PAYLOAD:
        return None
    try:
        msg = json.loads(data)
    except (ValueError, UnicodeDecodeError):
        # ValueError covers json.JSONDecodeError; UnicodeDecodeError
        # covers non-UTF-8 bytes.
        return None
    if not isinstance(msg, dict):
        return None
    member = msg.get("member")
    kind = msg.get("kind")
    if not isinstance(member, str) or not isinstance(kind, str):
        return None
    if not member or len(member) > _MAX_MEMBER_ID_LEN:
        return None
    if kind not in _VALID_KINDS:
        return None
    return member, kind


class Presence:
    """Per-member presence handle for a channel.

    On enter: opens a subscription on the presence topic, publishes a
    join, and starts the heartbeat + eviction loops.

    On exit: publishes a leave, stops the loops, closes the subscription.

    Async context manager + async iterator over :class:`PresenceChange`.
    Not constructed directly — get one from :meth:`Broadcast.presence`.
    """

    def __init__(
        self,
        broadcast: "Broadcast",
        channel: str,
        *,
        member_id: str,
        ttl_secs: float,
        heartbeat_secs: float,
        changes_queue_max: int = _DEFAULT_CHANGES_QUEUE_MAX,
    ) -> None:
        if ttl_secs <= 0:
            raise ValueError("ttl_secs must be > 0")
        if heartbeat_secs < _MIN_HEARTBEAT_SECS:
            raise ValueError(
                f"heartbeat_secs must be >= {_MIN_HEARTBEAT_SECS}; "
                f"got {heartbeat_secs}"
            )
        if not isinstance(member_id, str) or not member_id:
            raise ValueError("member_id must be a non-empty str")
        if len(member_id) > _MAX_MEMBER_ID_LEN:
            raise ValueError(
                f"member_id too long ({len(member_id)} > {_MAX_MEMBER_ID_LEN})"
            )
        if heartbeat_secs >= ttl_secs:
            # "heartbeat at ~1/3 TTL" rule of thumb: too close to TTL and a
            # single dropped heartbeat triggers a false eviction.
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
        # Path-safe internal topic name.  The channel was already validated
        # by Broadcast.presence; the `_` prefix marks it reserved and a
        # '.' separator keeps it inside the path-safe allowlist (':' would
        # be hostile to a future Windows file-backed ring).
        self._presence_topic = f"{RESERVED_PREFIX}presence.{channel}"

        # member_id -> last heartbeat ts (monotonic seconds).
        self._members: Dict[str, float] = {}
        self._changes: "asyncio.Queue[PresenceChange]" = asyncio.Queue(
            maxsize=max(1, changes_queue_max)
        )

        self._sub = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._consume_task: Optional[asyncio.Task] = None
        self._eviction_task: Optional[asyncio.Task] = None
        self._closed = False
        self._closed_event = asyncio.Event()
        # Dedupe the heartbeat-publish failure log so a persistent failure
        # (e.g. another process owns the single-publisher slot) logs once,
        # not once per heartbeat.
        self._heartbeat_publish_ok = True

    @property
    def members(self) -> set:
        """Snapshot of currently-active members (includes self).

        Returns a fresh set; the underlying state updates concurrently
        with the heartbeat / consume / eviction tasks.
        """
        return set(self._members.keys())

    # ── lifecycle ────────────────────────────────────────────────────────
    async def __aenter__(self) -> "Presence":
        # Open the subscription FIRST so the consume loop catches our own
        # initial join (and peers' messages already arriving).
        self._sub = await self._broadcast._subscribe_internal(self._presence_topic)
        # Track self eagerly — the round-trip through mmbus + fanout is
        # async, and ``members`` should reflect self immediately.
        self._members[self._member_id] = time.monotonic()
        self._consume_task = asyncio.create_task(
            self._consume_loop(),
            name=f"mmcast-presence-consume:{self._channel}",
        )
        self._eviction_task = asyncio.create_task(
            self._eviction_loop(),
            name=f"mmcast-presence-evict:{self._channel}",
        )
        # Best-effort join: in single-publisher multi-process mode another
        # process may own the presence publisher, so this can fail.  That
        # leaves us subscriber-only (we still see peers; peers just won't
        # see us).  We never block presence startup on it.
        try:
            await self._publish("join")
        except Exception:
            self._heartbeat_publish_ok = False
            logger.warning(
                "mmcast: presence join publish failed on %r — subscriber-only "
                "(another process likely owns the publisher)",
                self._presence_topic,
            )
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(),
            name=f"mmcast-presence-hb:{self._channel}",
        )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._closed:
            return
        self._closed = True
        self._closed_event.set()
        # Best-effort leave so peers see us go before TTL expiry.
        try:
            await self._publish("leave")
        except Exception:
            logger.debug("mmcast: presence leave publish failed (closing anyway)")

        for t in (self._heartbeat_task, self._consume_task, self._eviction_task):
            if t is not None:
                t.cancel()
        for t in (self._heartbeat_task, self._consume_task, self._eviction_task):
            if t is not None:
                try:
                    await t
                except asyncio.CancelledError:
                    pass
                except Exception:
                    logger.exception("mmcast: presence task error on close")

        if self._sub is not None:
            try:
                await self._sub.__aexit__(None, None, None)
            except Exception:
                logger.exception("mmcast: presence sub close failed")

    # ── publish + change emission ────────────────────────────────────────
    async def _publish(self, kind: str) -> None:
        """Publish a presence record (``join`` / ``heartbeat`` / ``leave``)."""
        payload = json.dumps(
            {"member": self._member_id, "kind": kind, "ts": time.time()},
            separators=(",", ":"),
        ).encode()
        await self._broadcast._publish_internal(self._presence_topic, payload)

    def _emit_change(self, change: PresenceChange) -> None:
        """Enqueue a change for the consumer, bounded (drop-oldest)."""
        try:
            self._changes.put_nowait(change)
        except asyncio.QueueFull:
            try:
                self._changes.get_nowait()
                self._changes.put_nowait(change)
            except (asyncio.QueueEmpty, asyncio.QueueFull):
                pass
            logger.warning(
                "mmcast: presence change queue full on %r — dropped oldest",
                self._presence_topic,
            )

    # ── background loops ─────────────────────────────────────────────────
    async def _heartbeat_loop(self) -> None:
        try:
            while not self._closed:
                await asyncio.sleep(self._heartbeat)
                if self._closed:
                    return
                try:
                    await self._publish("heartbeat")
                    if not self._heartbeat_publish_ok:
                        logger.info(
                            "mmcast: presence heartbeat publishing recovered "
                            "on %r",
                            self._presence_topic,
                        )
                        self._heartbeat_publish_ok = True
                except Exception:
                    # Log once on the failing edge (e.g. another process
                    # owns the single-publisher slot), then stay quiet.
                    if self._heartbeat_publish_ok:
                        logger.warning(
                            "mmcast: presence heartbeat publish failing on %r "
                            "(suppressing further warnings until it recovers)",
                            self._presence_topic,
                        )
                        self._heartbeat_publish_ok = False
        except asyncio.CancelledError:
            raise

    async def _consume_loop(self) -> None:
        try:
            assert self._sub is not None
            async for event in self._sub:
                parsed = _parse_presence_message(event.data)
                if parsed is None:
                    logger.debug(
                        "mmcast: ignoring malformed presence message on %r",
                        self._presence_topic,
                    )
                    continue
                member, kind = parsed
                try:
                    self._apply(member, kind)
                except Exception:
                    # Defence in depth: a bug in _apply must not kill the
                    # loop and freeze the subsystem.
                    logger.exception(
                        "mmcast: error applying presence record on %r",
                        self._presence_topic,
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("mmcast: presence consume loop crashed on %r", self._presence_topic)

    def _apply(self, member: str, kind: str) -> None:
        """Apply one validated presence record to member state."""
        if kind in ("join", "heartbeat"):
            is_new = member not in self._members
            if is_new and len(self._members) >= _MAX_MEMBERS:
                logger.warning(
                    "mmcast: presence member cap (%d) reached on %r; "
                    "ignoring new member",
                    _MAX_MEMBERS,
                    self._presence_topic,
                )
                return
            self._members[member] = time.monotonic()
            if is_new:
                self._emit_change(PresenceChange(member=member, joined=True))
        elif kind == "leave":
            if self._members.pop(member, None) is not None:
                self._emit_change(PresenceChange(member=member, joined=False))

    async def _eviction_loop(self) -> None:
        """Evict members whose last heartbeat is older than TTL.

        Never evicts *self*: in single-publisher multi-process mode this
        member may be unable to publish its own heartbeats (another
        process owns the slot), and self-eviction would emit a spurious
        self-leave.  Peers still evict us via their own loops if our
        heartbeats genuinely stop reaching them.
        """
        try:
            # Scan twice per TTL so eviction fires within ~1.5×TTL of a
            # missed heartbeat — standard eventually-consistent presence.
            scan_interval = max(self._ttl / 2, 0.05)
            while not self._closed:
                await asyncio.sleep(scan_interval)
                if self._closed:
                    return
                now = time.monotonic()
                expired = [
                    m
                    for m, ts in self._members.items()
                    if m != self._member_id and now - ts > self._ttl
                ]
                for m in expired:
                    del self._members[m]
                    self._emit_change(PresenceChange(member=m, joined=False))
        except asyncio.CancelledError:
            raise

    # ── async iterator ───────────────────────────────────────────────────
    def __aiter__(self) -> "Presence":
        return self

    async def __anext__(self) -> PresenceChange:
        # Deliver anything already queued first.
        try:
            return self._changes.get_nowait()
        except asyncio.QueueEmpty:
            pass
        if self._closed:
            raise StopAsyncIteration

        # Race the next change against close so a closed Presence with an
        # empty queue can't strand an awaiting consumer.
        get_task: "asyncio.Task[PresenceChange]" = asyncio.ensure_future(
            self._changes.get()
        )
        close_wait: "asyncio.Task[bool]" = asyncio.ensure_future(
            self._closed_event.wait()
        )
        try:
            await asyncio.wait(
                {get_task, close_wait}, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            close_wait.cancel()

        if get_task.done() and not get_task.cancelled():
            return get_task.result()
        get_task.cancel()
        try:
            return await get_task
        except (asyncio.CancelledError, Exception):
            pass
        try:
            return self._changes.get_nowait()
        except asyncio.QueueEmpty:
            raise StopAsyncIteration
