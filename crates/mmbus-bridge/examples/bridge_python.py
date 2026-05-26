"""Embedded mmbus bridge — runs a forwarder + listener inside a Python service.

Run two copies on the same host to see a loopback round-trip, or point
``peers[].endpoint`` at another machine for a real cross-host relay.

    python examples/bridge_python.py

Requires the companion wheels installed:

    pip install mmbus mmbus-bridge      # or: pip install mmbus[bridge]
"""
from __future__ import annotations

from mmbus_bridge import Bridge

cfg = {
    "bus": "demo",
    # Bind an ephemeral port (":0") so this example never collides with
    # a port already in use; the resolved port is printed below.
    "listen": "127.0.0.1:0",
    "topics": [{"name": "events", "forward": True, "receive": True}],
    # No peers configured = receive-only.  Add a peer block to forward:
    #   "peers": [{"name": "b", "endpoint": "b.host:4443", "preshared_key": "x"}],
    "peers": [],
}

with Bridge(cfg) as bridge:
    host, port = bridge.listen_addr
    print(f"bridge origin_id={bridge.origin_id} listening on {host}:{port}")
    print("Ctrl-C to exit")
    try:
        bridge.wait()
    except KeyboardInterrupt:
        print("\nshutting down")
