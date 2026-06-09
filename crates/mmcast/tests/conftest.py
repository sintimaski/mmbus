"""Shared pytest fixtures for the mmcast suite."""
from __future__ import annotations

import shutil
import tempfile

import pytest


@pytest.fixture
def short_bus_dir():
    """An isolated, short-path bus directory for one test.

    Why ``/tmp`` rather than pytest's ``tmp_path``: the bus opens Unix
    domain sockets whose path must fit macOS's ``SUN_LEN`` (~104 chars),
    and ``tmp_path`` (under ``/private/var/folders/...``) blows that
    budget once a topic-specific filename is appended.

    Why ``mkdtemp`` rather than a ``uuid``-named ``makedirs``: mkdtemp
    creates the directory atomically with 0700 permissions and an
    unpredictable name, so a co-tenant on a shared host can't pre-create
    or read it.
    """
    root = tempfile.mkdtemp(prefix="mmcast-test-", dir="/tmp")
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)
