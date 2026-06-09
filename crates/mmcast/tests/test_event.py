"""Event / PresenceChange value-type tests (pure, no bus needed)."""
from __future__ import annotations

import json

import pytest

from mmbus_cast import Event, PresenceChange


def test_event_cursor_defaults_to_minus_one():
    assert Event(data=b"x").cursor == -1
    assert Event(data=b"x", cursor=7).cursor == 7


def test_event_json_valid():
    assert Event(data=b'{"a":1,"b":[2,3]}').json() == {"a": 1, "b": [2, 3]}


def test_event_json_invalid_raises_jsondecodeerror():
    with pytest.raises(json.JSONDecodeError):
        Event(data=b"not json {").json()


def test_event_json_decodeerror_is_value_error():
    """JSONDecodeError subclasses ValueError, so a caller can catch
    either."""
    with pytest.raises(ValueError):
        Event(data=b"\xff\xfe").json()


def test_event_is_frozen():
    ev = Event(data=b"x")
    with pytest.raises(Exception):  # FrozenInstanceError (a dataclass error)
        ev.data = b"y"  # type: ignore[misc]


def test_event_equality():
    assert Event(data=b"x", cursor=1) == Event(data=b"x", cursor=1)
    assert Event(data=b"x", cursor=1) != Event(data=b"x", cursor=2)


def test_presence_change_fields():
    joined = PresenceChange(member="alice", joined=True)
    left = PresenceChange(member="bob", joined=False)
    assert joined.member == "alice" and joined.joined is True
    assert left.member == "bob" and left.joined is False


def test_presence_change_is_frozen():
    pc = PresenceChange(member="alice", joined=True)
    with pytest.raises(Exception):
        pc.member = "eve"  # type: ignore[misc]
