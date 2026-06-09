"""FastAPI / Starlette integration helpers.

Optional surface — only imported when the user does
``from mmbus_cast.fastapi import broadcast_lifespan``.  Keeps the core
package free of a hard FastAPI dependency.

The helpers cover the two things every multi-worker FastAPI WS
broadcast app needs:

1. ``broadcast_lifespan(name, ...)`` — a lifespan context manager that
   opens/closes a :class:`~mmbus_cast.Broadcast` alongside the ASGI
   app.  Returns the Broadcast instance for the app to stash in
   ``app.state``.
2. ``worker_shard_from_env(workers=N)`` — pick this process's
   ``worker_id`` + ``peers`` from the environment.  Default: uses
   ``MMCAST_WORKER_ID`` if set, otherwise derives a stable id from
   ``os.getpid()``; ``peers`` are listed via ``MMCAST_PEERS`` (comma-
   separated) or the supplied ``workers`` count expanded to
   ``["w0", "w1", ..., f"w{N-1}"]``.

Tests live alongside the rest of the suite as ``test_fastapi_helpers.py``;
end-to-end uvicorn-driven coverage is exercised by the chat example
(``examples/fastapi_chat/``).
"""
from __future__ import annotations

import contextlib
import logging
import os
from typing import AsyncIterator, List, Optional, Tuple

from . import Broadcast

logger = logging.getLogger("mmbus_cast")


def worker_shard_from_env(
    workers: Optional[int] = None,
    *,
    env_worker_id: str = "MMCAST_WORKER_ID",
    env_peers: str = "MMCAST_PEERS",
) -> Tuple[str, List[str]]:
    """Resolve this process's ``(worker_id, peers)`` for sharded mode.

    Resolution order for ``worker_id``:
      1. ``$MMCAST_WORKER_ID`` (or ``env_worker_id``) if set.
      2. ``"w{os.getpid()}"`` — stable for the lifetime of the process.

    Resolution order for ``peers``:
      1. ``$MMCAST_PEERS`` (or ``env_peers``), comma-separated.
      2. ``workers`` arg expanded to ``["w0", ..., f"w{workers-1}"]``.
      3. If neither: a single-element list with our own ``worker_id``
         (works for single-process deployments).

    The caller passes these into ``Broadcast(worker_id=..., peers=...)``.
    See spec § "Multi-worker constraint".
    """
    worker_id = os.environ.get(env_worker_id)
    if not worker_id:
        worker_id = f"w{os.getpid()}"

    peers_env = os.environ.get(env_peers)
    if peers_env:
        peers = [p.strip() for p in peers_env.split(",") if p.strip()]
    elif workers is not None and workers > 0:
        peers = [f"w{i}" for i in range(workers)]
    else:
        # No peer list and no worker count → single-element list (this
        # process only).  In a genuinely multi-worker deployment that's a
        # silently-broken config (each worker only sees its own messages),
        # so warn loudly — the caller almost certainly meant to set
        # MMCAST_PEERS or pass `workers=N`.
        peers = [worker_id]
        web_concurrency = os.environ.get("WEB_CONCURRENCY")
        if web_concurrency and web_concurrency.strip() not in ("", "1"):
            logger.warning(
                "mmcast: WEB_CONCURRENCY=%s suggests a multi-worker "
                "deployment, but neither %s nor a workers count is set — "
                "fan-in will be broken (each worker sees only its own "
                "messages). Set %s to the full worker-id list.",
                web_concurrency,
                env_peers,
                env_peers,
            )

    return worker_id, peers


@contextlib.asynccontextmanager
async def broadcast_lifespan(
    name: str,
    *,
    worker_id: Optional[str] = None,
    peers: Optional[List[str]] = None,
    prepare: Optional[List[str]] = None,
    **bus_kwargs,
) -> AsyncIterator[Broadcast]:
    """ASGI lifespan helper that opens a :class:`Broadcast` for the app.

    Usage with FastAPI::

        from contextlib import asynccontextmanager
        from fastapi import FastAPI
        from mmbus_cast.fastapi import broadcast_lifespan

        @asynccontextmanager
        async def lifespan(app):
            async with broadcast_lifespan("my-app", prepare=["chat"]) as bc:
                app.state.broadcast = bc
                yield

        app = FastAPI(lifespan=lifespan)

    Pass ``worker_id`` + ``peers`` for multi-worker fan-in, or omit them
    for single-publisher mode.  ``prepare`` is an optional list of
    channels to claim publisher slots for at startup so the first WS
    connection doesn't pay the ``connect_timeout_secs`` wait.

    Remaining kwargs forward to :class:`mmbus.Bus`.
    """
    bc = Broadcast(
        name, worker_id=worker_id, peers=peers, **bus_kwargs
    )
    async with bc:
        if prepare:
            await bc.prepare(*prepare)
        yield bc
