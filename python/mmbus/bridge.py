"""Python helper for ``mmbus-bridge``.

This module is a thin wrapper around the standalone ``mmbus-bridge``
binary built from the ``bridge/`` crate.  It does not bundle the
binary into the mmbus wheel — install it separately via::

    cargo install --path bridge

or, once the binary ships as a wheel-bundled script::

    pip install mmbus-bridge

Usage from Python::

    from mmbus import bridge

    # Foreground (blocks until the process exits / receives SIGTERM):
    bridge.run("/etc/mmbus-bridge.toml")

    # Background:
    proc = bridge.spawn("/etc/mmbus-bridge.toml")
    # ...do other work...
    proc.terminate(); proc.wait(timeout=5)

The bridge binary itself accepts one positional argument: the config
file path.  See ``bridge/sample-config.toml`` for the schema.
"""
from __future__ import annotations

import os
import shutil
import subprocess
from typing import IO, Optional, Union


class BridgeNotFoundError(RuntimeError):
    """The ``mmbus-bridge`` binary was not found on PATH (or at the
    explicit path passed to ``run()``/``spawn()``)."""


def _resolve_binary(binary: Optional[str]) -> str:
    if binary is not None:
        if not os.path.isfile(binary):
            raise BridgeNotFoundError(
                f"mmbus-bridge binary not found at {binary!r}"
            )
        return binary
    found = shutil.which("mmbus-bridge")
    if found is None:
        raise BridgeNotFoundError(
            "mmbus-bridge binary not on PATH.  Install via "
            "`cargo install --path bridge` from the mmbus repo, then "
            "ensure ~/.cargo/bin is on your $PATH."
        )
    return found


def run(
    config_path: Union[str, os.PathLike],
    *,
    binary: Optional[str] = None,
    stdout: Optional[Union[int, IO]] = None,
    stderr: Optional[Union[int, IO]] = None,
    check: bool = True,
) -> int:
    """Run the bridge in the foreground.  Blocks until the binary exits.

    Returns the binary's exit code.  When ``check=True`` (the default)
    a non-zero exit raises :class:`subprocess.CalledProcessError`.

    Pass file objects (or ``subprocess.DEVNULL`` / ``subprocess.PIPE``)
    to ``stdout`` / ``stderr`` to redirect the bridge's logging — by
    default it inherits the calling process's stdio.
    """
    bin_path = _resolve_binary(binary)
    completed = subprocess.run(
        [bin_path, str(config_path)],
        stdout=stdout,
        stderr=stderr,
        check=check,
    )
    return completed.returncode


def spawn(
    config_path: Union[str, os.PathLike],
    *,
    binary: Optional[str] = None,
    stdout: Optional[Union[int, IO]] = None,
    stderr: Optional[Union[int, IO]] = None,
) -> subprocess.Popen:
    """Launch the bridge as a background process; return the
    ``Popen`` handle so the caller can ``terminate()`` / ``wait()`` /
    ``poll()`` it.

    This is the right entry point for Python services that want to
    embed the bridge alongside their main event loop (e.g. a FastAPI
    app spawning the bridge under a lifespan).
    """
    bin_path = _resolve_binary(binary)
    return subprocess.Popen(
        [bin_path, str(config_path)],
        stdout=stdout,
        stderr=stderr,
    )


__all__ = ["BridgeNotFoundError", "run", "spawn"]
