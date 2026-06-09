"""Event/PresenceChange records exposed to consumers.

Kept tiny on purpose — mmcast's whole value is "thin shim over mmbus".
Anything richer lives in user code.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Event:
    """One message delivered to a subscriber.

    ``data`` is the raw bytes published.  ``cursor`` is the underlying
    mmbus ring cursor — useful for "resume from here" patterns in future
    versions; opaque otherwise.
    """

    data: bytes
    cursor: int = -1

    def json(self) -> Any:
        """Decode ``data`` as UTF-8 JSON.  Raises the standard
        ``json.JSONDecodeError`` if the payload isn't JSON."""
        return json.loads(self.data)


@dataclass(frozen=True)
class PresenceChange:
    """A presence-topic event: a member joined or left ``channel``."""

    member: str
    joined: bool  # False == left (TTL expiry or graceful leave)
