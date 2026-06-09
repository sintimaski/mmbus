"""Channel-name validation.

A single, reusable validation gate for every public entry point that
takes a user-supplied channel name (``publish``, ``subscribe``,
``prepare``).  Centralised here — not inlined at each call site — so the
rules are defined once and can be tightened in one place.

Two concerns are addressed:

1. **Reserved namespace.**  mmcast uses the ``_`` prefix for internal
   subsystem topics (e.g. ``_presence:<channel>``).  Public callers are
   forbidden from naming a channel ``_…`` so an app that derives the
   channel from untrusted input (a URL path param, say) can't be tricked
   into publishing forged records onto a subsystem topic.  Internal
   callers bypass the check via ``validate_channel_name(..., internal=True)``.

2. **Path-safety.**  A channel name becomes part of an on-disk mmap file
   path inside mmbus (``base_dir / <name>``).  Without validation, a name
   like ``../../etc/evil`` escapes the bus directory.  We restrict names
   to a conservative allowlist and reject traversal sequences outright.

The allowlist (``[A-Za-z0-9_.-]``) matches the topic-name convention used
throughout the mmbus examples; the sharding suffix mmcast appends
(``<name>.<worker_id>``) stays inside it.
"""
from __future__ import annotations

import re

# Reserved prefix for mmcast-internal subsystem topics (presence, and
# any future subsystem).  Public channel names may not start with it.
RESERVED_PREFIX = "_"

# Hard ceiling on channel-name length.  Generous for real channels, but
# bounds the on-disk path length and the per-name memory an attacker
# could pin by spamming distinct names.
MAX_CHANNEL_NAME_LEN = 256

# Conservative allowlist: ASCII letters, digits, and the three
# separators that are safe in both filenames and the sharding suffix.
_ALLOWED_CHANNEL_RE = re.compile(r"\A[A-Za-z0-9_.-]+\Z")


class InvalidChannelError(ValueError):
    """Raised when a channel name fails validation.

    Subclasses :class:`ValueError` so existing ``except ValueError``
    handlers keep working, while callers that want to distinguish a bad
    channel from other value errors can catch this specifically.
    """


def validate_channel_name(channel: str, *, internal: bool = False) -> str:
    """Validate and return ``channel``, or raise :class:`InvalidChannelError`.

    Args:
        channel: the user-supplied channel name.
        internal: when ``True``, the reserved-prefix rule is skipped so
            mmcast's own subsystems (presence) can name ``_…`` topics.
            All other rules (type, length, charset, traversal) still
            apply — internal callers are trusted not to be malicious,
            not trusted to be bug-free.

    Returns:
        The validated channel name (unchanged), so callers can write
        ``channel = validate_channel_name(channel)``.
    """
    if not isinstance(channel, str):
        raise InvalidChannelError(
            f"channel must be str, not {type(channel).__name__}"
        )
    if not channel:
        raise InvalidChannelError("channel must not be empty")
    if len(channel) > MAX_CHANNEL_NAME_LEN:
        raise InvalidChannelError(
            f"channel name too long ({len(channel)} > {MAX_CHANNEL_NAME_LEN})"
        )
    if not internal and channel.startswith(RESERVED_PREFIX):
        raise InvalidChannelError(
            f"channel names starting with {RESERVED_PREFIX!r} are reserved "
            f"for mmcast internal topics; got {channel!r}"
        )
    # Defence in depth: the allowlist already excludes '/' and would
    # reject '..' only if a '/' were present, but '..' as a bare name is
    # still a traversal risk on path join, so reject it explicitly.
    if channel == "." or channel == ".." or ".." in channel:
        raise InvalidChannelError(
            f"channel name must not contain path traversal; got {channel!r}"
        )
    if not _ALLOWED_CHANNEL_RE.match(channel):
        raise InvalidChannelError(
            f"channel name must match [A-Za-z0-9_.-]+; got {channel!r}"
        )
    return channel
