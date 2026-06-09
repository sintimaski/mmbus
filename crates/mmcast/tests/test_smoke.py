"""Smoke tests for the mmcast scaffold (T2).

Verifies:
  1. The package imports.
  2. The public surface is what the spec says it is.
  3. The dependency-direction rule from the spec ("mmbus_cast may only
     touch the public mmbus API, never `_mmbus`") holds — T1/T2 acceptance.

Behavioural tests for publish/subscribe/replay/presence land alongside
T3–T6.
"""
from __future__ import annotations


def test_imports() -> None:
    import mmbus_cast  # noqa: F401

    assert mmbus_cast.__version__ == "0.1.0"


def test_public_surface() -> None:
    import mmbus_cast

    expected = {
        "Broadcast",
        "BroadcastClosedError",
        "Event",
        "PresenceChange",
        "SlowConsumer",
    }
    assert expected.issubset(set(mmbus_cast.__all__))
    for name in expected:
        assert hasattr(mmbus_cast, name), name


def test_no_private_mmbus_imports() -> None:
    """Dependency-direction rule: mmcast may not import from `mmbus._mmbus`.

    The rule exists because `_mmbus` is the private PyO3 extension; only
    `mmbus`'s own Python wrapper is part of the contract mmcast pins.
    Reaching past it would couple mmcast to a binary surface that
    mmbus is free to break inside a minor version.
    """
    import pathlib

    root = pathlib.Path(__file__).resolve().parent.parent / "python" / "mmbus_cast"
    offenders = []
    for path in root.rglob("*.py"):
        text = path.read_text()
        # Crude but sufficient: any literal import that touches the
        # private module is a violation.  Update this guard if mmbus
        # ever promotes part of `_mmbus` into the public surface.
        if "mmbus._mmbus" in text or "from mmbus import _mmbus" in text:
            offenders.append(str(path))
    assert not offenders, f"mmcast must not import mmbus._mmbus: {offenders}"


def test_broadcast_constructor_stores_kwargs() -> None:
    """The constructor stores kwargs (plus the broadcast-default
    ``backpressure="drop_oldest"``) for ``__aenter__`` to feed into
    ``mmbus.Bus``."""
    from mmbus_cast import Broadcast

    bc = Broadcast("test-bus", capacity=128, slot_size=4096)
    assert bc._name == "test-bus"
    # `backpressure="drop_oldest"` is the broadcast-shaped default — a
    # missed message under no-subscriber backpressure is the right thing
    # to drop, not error out.  Caller can override via the kwarg.
    assert bc._bus_kwargs == {
        "backpressure": "drop_oldest",
        "capacity": 128,
        "slot_size": 4096,
    }
    assert bc._bus is None
    assert bc._closed is False
