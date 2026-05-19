"""In-process cross-machine bridge for mmbus topics.

This package embeds the ``mmbus-bridge`` runtime in your Python process —
no separate binary install, no ``subprocess`` lifecycle.  It forwards
locally-published mmbus topics to peer machines over TCP and republishes
inbound peer traffic onto the local bus.

    from mmbus_bridge import Bridge

    cfg = {
        "bus": "my-app",
        "listen": "0.0.0.0:4443",
        "topics": [{"name": "events"}],
        "peers": [
            {
                "name": "machine-b",
                "endpoint": "machine-b.internal:4443",
                "preshared_key": "shared-secret",
            },
        ],
    }

    with Bridge(cfg) as bridge:
        print(f"listening on {bridge.listen_addr}")
        bridge.wait()          # blocks until Ctrl-C / shutdown()

The config schema mirrors the bridge TOML 1:1 — see
``bridge/sample-config.toml`` and ``docs/rfc-bridge-python-sdk.md``.

TCP only: QUIC peers (``transport = "quic"``) are rejected by this
wheel with :class:`BridgeQuicError`.  For QUIC, install the standalone
``mmbus-bridge`` binary (``cargo install --path bridge --features quic``).
"""
from __future__ import annotations

from ._mmbus_bridge import (
    BridgeConfigError,
    BridgeError,
    BridgeListenError,
    BridgeQuicError,
    _RustBridge,
)


class Bridge(_RustBridge):
    """In-process mmbus bridge.

    Construct from a dict (preferred), a TOML string
    (:meth:`from_toml`), or a TOML file (:meth:`from_path`).  Call
    :meth:`start` to spawn the bridge threads and :meth:`shutdown` to
    join them, or use the context-manager form which does both::

        with Bridge(cfg) as bridge:
            ...   # bridge is running here

    Validation runs eagerly at construction time and raises
    :class:`BridgeConfigError` (a subclass of ``ValueError``).
    """

    __slots__ = ()

    def __enter__(self) -> "Bridge":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.shutdown()
        return False  # never suppress exceptions from the with-body

    def __repr__(self) -> str:
        if self.is_running():
            return f"<mmbus_bridge.Bridge running origin_id={self.origin_id} listen_addr={self.listen_addr}>"
        return "<mmbus_bridge.Bridge (not running)>"


__all__ = [
    "Bridge",
    "BridgeError",
    "BridgeConfigError",
    "BridgeListenError",
    "BridgeQuicError",
]
