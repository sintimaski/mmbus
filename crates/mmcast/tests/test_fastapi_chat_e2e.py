"""T7 — End-to-end test of the FastAPI chat example.

Runs the actual ``examples/fastapi_chat/app.py`` ASGI app in-process via
Starlette's ``TestClient`` and verifies the WebSocket roundtrip works.

Skipped if FastAPI / starlette aren't installed (they're an opt-in
``mmbus-cast[fastapi]`` extra).
"""
from __future__ import annotations

import os
import shutil
import sys
import uuid
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("starlette")


@pytest.fixture
def short_bus_dir(monkeypatch):
    root = f"/tmp/mmcast-test-{uuid.uuid4().hex[:8]}"
    os.makedirs(root, exist_ok=True)
    # Force the example app to use this isolated bus dir by patching
    # the lifespan kwargs through an env-var the example reads.  Since
    # the example doesn't honour an env-var, we monkey-patch the helper
    # to inject base_dir into bus_kwargs.
    yield root
    shutil.rmtree(root, ignore_errors=True)


def test_chat_websocket_broadcast(short_bus_dir, monkeypatch):
    """Two clients connected to the chat app see each other's messages.

    Drives the actual `examples/fastapi_chat/app.py` ASGI app — same
    lifespan, same endpoints, same WS path.
    """
    # Make the example importable without installing it as a package.
    example_root = Path(__file__).resolve().parent.parent / "examples" / "fastapi_chat"
    monkeypatch.syspath_prepend(str(example_root))

    # Patch broadcast_lifespan so the example bus lands in our short dir.
    # The example calls `broadcast_lifespan("mmcast-chat-demo", ...,
    # wal_enabled=False)`; we need base_dir=short_bus_dir injected.
    import mmbus_cast.fastapi as fa
    real_lifespan = fa.broadcast_lifespan

    def patched_lifespan(name, **kw):
        kw.setdefault("base_dir", short_bus_dir)
        # Give it a unique name per test run to avoid stale bus state.
        return real_lifespan(f"{name}-{uuid.uuid4().hex[:6]}", **kw)

    monkeypatch.setattr(fa, "broadcast_lifespan", patched_lifespan)

    # Reload the example so it picks up the patched helper.
    if "app" in sys.modules:
        del sys.modules["app"]
    import app as chat_app
    from starlette.testclient import TestClient

    with TestClient(chat_app.app) as client:
        with client.websocket_connect("/ws") as alice:
            with client.websocket_connect("/ws") as bob:
                alice.send_text("hello from alice")
                # Both clients (including the sender) see the message —
                # standard broadcast semantic.
                msg_bob = bob.receive_text()
                msg_alice = alice.receive_text()
                assert msg_bob == "hello from alice"
                assert msg_alice == "hello from alice"

                bob.send_text("hi alice!")
                msg_alice_2 = alice.receive_text()
                msg_bob_2 = bob.receive_text()
                assert msg_alice_2 == "hi alice!"
                assert msg_bob_2 == "hi alice!"
