# RFC — Bridge Python SDK (v0.3.0)

**Status:** Draft → implementation
**Owner:** mmbus core
**Target release:** v0.3.0
**Replaces / extends:** existing `mmbus.bridge.run` / `mmbus.bridge.spawn`
subprocess shim (kept; still useful for systemd-style supervision)

---

## 1. Problem

`mmbus-bridge` ships today as a standalone Rust binary; the Python side
is `mmbus.bridge.{run, spawn}`, both of which `subprocess.run` /
`subprocess.Popen` the binary.  That works for operators but is
clunky for embedded use — every Python service that wants
cross-machine pub/sub has to:

1. Install a second native artefact (`cargo install --path bridge`).
2. Manage a child process lifecycle (signals, exit codes, log piping).
3. Lose programmatic access to bridge-internal state — the bound
   `listen_addr` (when `listen = "0.0.0.0:0"`), the resolved
   `origin_id`, the QUIC cert fingerprint, future stats counters.
4. Pay PEX-style packaging overhead in containers (now there's an
   extra binary to copy in).

mmbus already ships a PyO3 extension wheel for the `Bus` / `Subscription`
API.  Adding the bridge to the same wheel collapses both pain
points: one `pip install`, one process, one lifecycle.

## 2. Goals

- **One `pip install mmbus` ships a usable cross-machine bridge** for
  the TCP path (the default everyone already uses).
- **Programmatic lifecycle:** `mmbus.Bridge(config).start()` returns
  immediately with a handle; `.shutdown()` joins all threads cleanly.
- **Idiomatic Python:** context-manager support, typed errors,
  property-style access to runtime state.
- **No regression for existing subprocess users** — keep
  `mmbus.bridge.run` and `mmbus.bridge.spawn` working byte-identically.
- **Zero new dependencies for non-bridge users** of the mmbus crate.

## 3. Non-goals (deferred to later releases)

- **QUIC support in the Python wheel.**  Embedding `tokio + quinn +
  rustls + rcgen + ring` blows wheel size from ~300 KB to multi-MB
  and adds significant compile time for everyone.  v0.3.0 ships
  TCP-only; QUIC users keep the binary path.  We can lift this in
  v0.4.0 if there's demand.
- **Per-peer / per-topic statistics.**  The Rust `Bridge` struct
  doesn't expose drop counters or per-peer throughput today.  When
  we add them (planned for v0.3.1), the Python wrapper grows a
  `.stats()` method.  Out of scope for v0.3.0.
- **Async / `await` support.**  The Rust bridge is thread-based;
  the Python wrapper exposes blocking calls that release the GIL.
  Wrapping it under `asyncio` is a future `python/mmbus/_aio.py`
  exercise.

## 4. Public Python API

### 4.1 `mmbus.Bridge` — in-process bridge

```python
from mmbus import Bridge

# Dict config (preferred; mirrors the TOML schema 1:1).
cfg = {
    "bus": "my-app",
    "base_dir": "/var/lib/mmbus",
    "listen": "0.0.0.0:4443",
    "topics": [
        {"name": "events", "forward": True, "receive": True},
        {"name": "alerts", "forward": False, "receive": True},
    ],
    "peers": [
        {
            "name": "machine-b",
            "endpoint": "machine-b.internal:4443",
            "preshared_key": "hunter2",
        },
    ],
}

bridge = Bridge(cfg)
bridge.start()

print(f"bridge {bridge.origin_id} listening on {bridge.listen_addr}")

# Block forever (this is what `mmbus.bridge.run` does today,
# but in-process, no subprocess required).
try:
    bridge.wait()
except KeyboardInterrupt:
    bridge.shutdown()

# Or as a context manager:
with Bridge(cfg) as bridge:
    do_app_work()
# threads joined on exit; __exit__ swallows nothing
```

### 4.2 Construction forms

| Form                          | What it does                                  |
|-------------------------------|-----------------------------------------------|
| `Bridge(config: dict)`        | Validate dict → `BridgeConfig` → `start()`-ready |
| `Bridge.from_toml(text: str)` | Parse TOML string                              |
| `Bridge.from_path(path)`      | Read file from disk, parse                     |

All three construction forms run the full `BridgeConfig::validate()`
pass synchronously and raise `mmbus.ConfigError` (a subclass of
`ValueError`) before `start()` is reached.  This matches the
"fail-fast at config load" contract the standalone binary already
honours.

### 4.3 Instance methods

| Method            | Behaviour                                                |
|-------------------|----------------------------------------------------------|
| `.start()`        | Spawn subscriber/forwarder/listener threads. Idempotent: second `start()` is a no-op on an already-started bridge. |
| `.shutdown(timeout=None)` | Signal all threads, join them. Optional float seconds; on timeout, raises `TimeoutError` and leaves the bridge in "draining" state. Idempotent. |
| `.wait()`         | Block forever (or until `shutdown()` is called from another thread). Released by `KeyboardInterrupt` so `try/except` works in scripts. |
| `.is_running()`   | `True` between `start()` and `shutdown()`.              |
| `.__enter__/__exit__` | Context manager: `__enter__` calls `start()`, `__exit__` calls `shutdown()`. |

### 4.4 Properties

| Property                | Type / contents                                     |
|-------------------------|-----------------------------------------------------|
| `.origin_id`            | `int` — 64-bit bridge identity (random unless `origin_id` in config) |
| `.listen_addr`          | `(str, int) | None` — `(host, port)` tuple when `listen` is configured, else `None`. Resolves `0.0.0.0:0` to the bound ephemeral port. |
| `.config`               | The parsed-and-validated config as a frozen `dict` (defensive copy) |

### 4.5 Exceptions

| Python class              | Raised when                              | Maps from Rust         |
|---------------------------|------------------------------------------|------------------------|
| `mmbus.ConfigError`       | TOML / dict validation fails             | `ConfigError::*`       |
| `mmbus.BridgeListenError` | `listen` address bind fails              | `BridgeError::Listen`  |
| `mmbus.BridgeQuicError`   | QUIC config seen by TCP-only wheel       | `BridgeError::QuicNotCompiled` / `QuicSetup` |
| existing mmbus exceptions | mmbus core errors (e.g. `BusFullError`)  | `BridgeError::Mmbus(_)` (passes through) |

`mmbus.ConfigError` subclasses `ValueError` (good Python citizenship —
existing `except ValueError:` blocks catch it).  The bridge errors
subclass `RuntimeError`.

### 4.6 Coexistence with the existing subprocess shim

The current `python/mmbus/bridge.py` module stays as-is.  Names:

- `mmbus.Bridge`           — new in-process class (this RFC)
- `mmbus.bridge.run`       — existing subprocess foreground runner
- `mmbus.bridge.spawn`     — existing subprocess background spawner
- `mmbus.bridge.BridgeNotFoundError` — existing

No collision; users pick the model that fits.  We update the module
docstring to point at the new class as the preferred path for
embedded use.

## 5. Implementation plan

### 5.1 Cargo wiring — companion wheel, not a bundled feature

> **Implementation note (decided during B2):** bundling the bridge
> into the core mmbus wheel via a `bridge` Cargo feature is
> **impossible** — `mmbus-bridge` already depends on `mmbus` (it
> republishes onto a local `Bus`), so adding `mmbus → mmbus-bridge`
> creates a Cargo dependency cycle (`error: cyclic package
> dependency`).  The path-dep across the `[workspace]` boundary
> itself works (verified by scratch build), but the cycle is fatal
> regardless of workspace layout.
>
> Resolution: the PyO3 bindings live **inside the bridge crate**,
> built as a **separate companion wheel** `mmbus-bridge` exposing
> the `mmbus_bridge._mmbus_bridge` extension.  Users get it via
> `pip install mmbus[bridge]` (an extra that pulls the companion
> wheel) or `pip install mmbus mmbus-bridge`.  The import surface is
> `from mmbus_bridge import Bridge`.

In `bridge/Cargo.toml`:

```toml
[lib]
name = "mmbus_bridge"
crate-type = ["cdylib", "rlib"]   # cdylib for the wheel, rlib for the binary

[features]
python = ["dep:pyo3", "dep:serde_json"]
# `extension-module` is split OUT of `python` on purpose: it tells the
# linker NOT to provide libpython (correct for a wheel, fatal for the
# binary + `cargo test` harness, which need libpython linked).  Only
# the wheel build enables it (via pyproject below).  This keeps
# `cargo build/test --features python` working on every platform,
# Linux included.
extension-module = ["python", "pyo3/extension-module"]

[dependencies]
pyo3 = { version = "0.22", optional = true }   # NO extension-module here
serde_json = { version = "1", optional = true }
```

In `bridge/pyproject.toml` (new):

```toml
[project]
name = "mmbus-bridge"
dependencies = ["mmbus==0.3.0"]

[tool.maturin]
features = ["extension-module"]    # pulls `python` transitively; TCP-only (no `quic`)
module-name = "mmbus_bridge._mmbus_bridge"
python-source = "python"
```

In the core `pyproject.toml`:

```toml
[project.optional-dependencies]
bridge = ["mmbus-bridge"]
```

### 5.2 Rust binding: `bridge/src/python.rs`

New module gated by the bridge crate's `python` feature, registering
the `_mmbus_bridge` PyO3 module:

```rust
#[pyclass(name = "_RustBridge", module = "mmbus_bridge._mmbus_bridge")]
struct PyBridge {
    state: Mutex<State>,
}

enum State { Configured(BridgeConfig), Running(Bridge), Shutdown, Transitioning }

#[pymethods]
impl PyBridge {
    #[new] fn new(config: &Bound<PyDict>) -> PyResult<Self> { /* dict → json → BridgeConfig */ }
    #[classmethod] fn from_toml(_cls: &Bound<PyType>, text: &str) -> PyResult<Self> { ... }
    #[classmethod] fn from_path(_cls: &Bound<PyType>, path: PathBuf) -> PyResult<Self> { ... }
    fn start(&self, py: Python) -> PyResult<()> { /* GIL released */ }
    fn shutdown(&self, py: Python, timeout: Option<f64>) -> PyResult<()> { /* GIL released */ }
    fn wait(&self, py: Python) -> PyResult<()> { /* GIL released; signal-safe */ }
    fn is_running(&self) -> bool { ... }
    #[getter] fn origin_id(&self) -> Option<u64> { ... }
    #[getter] fn listen_addr(&self) -> Option<(String, u16)> { ... }
}
```

**Locking:** `Mutex<State>` synchronises the
`Configured → Running → Shutdown` state machine (with a transient
`Transitioning` variant held only while a method swaps the enum).
All public methods take a short-lived lock; the actual blocking work
(`shutdown.join`, `wait` loop) releases the GIL.

**Error mapping:** the core mmbus exception types live in the
separate `_mmbus` extension, unreachable from `_mmbus_bridge`.  The
rare `BridgeError::Mmbus` from `start()` is surfaced under a
bridge-local `BridgeError` base class with the message preserved;
`Listen` / `QuicNotCompiled` / `QuicSetup` map to
`BridgeListenError` / `BridgeQuicError`.

**Dict → BridgeConfig path:** the simplest implementation is
`pyo3 dict → json::Value` (via stdlib `json.dumps` + `serde_json`) →
`serde_json::from_value::<BridgeConfig>` → `BridgeConfig::validate()`.
That keeps a single canonical
validation path (no parallel "dict validator" to drift).  Cost:
one extra `serde_json` dep at wheel-build time (~50 KB).
Alternative: hand-roll dict → BridgeConfig in Rust.  Picking the
former — the indirection is invisible to users and saves us from
duplicating the validation tree.

**Wait loop:** `wait()` is a `while bridge.is_running():
thread::park_timeout(100ms)` loop with `py.check_signals()?` each
tick — keeps Ctrl-C responsive in REPL scripts.

### 5.3 Python wrapper

`bridge/python/mmbus_bridge/__init__.py`:

```python
from ._mmbus_bridge import (
    _RustBridge, BridgeError, BridgeConfigError,
    BridgeListenError, BridgeQuicError,
)

class Bridge(_RustBridge):
    """In-process mmbus bridge.  See RFC bridge-python-sdk."""
    __slots__ = ()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.shutdown()
        return False  # never suppress exceptions from the with-body

__all__ = ["Bridge", "BridgeError", "BridgeConfigError",
           "BridgeListenError", "BridgeQuicError"]
```

Reason for the Python-side subclass rather than implementing
`__enter__/__exit__` in Rust: PyO3 0.22's `#[pyclass]` doesn't
support `__exit__` returning a typed `bool` cleanly without
boilerplate.  The Python subclass costs ~5 lines and is the
idiomatic place for protocol methods anyway.

### 5.4 Example

New file `bridge/examples/bridge_python.py`:

```python
"""Embedded bridge — runs a forwarder + listener inside your Python service."""
from mmbus_bridge import Bridge

cfg = {
    "bus": "demo",
    "listen": "127.0.0.1:0",
    "topics": [{"name": "events"}],
    "peers": [],          # forward-only; no peers configured = receive-only
}

with Bridge(cfg) as bridge:
    print(f"bridge bound on {bridge.listen_addr}, origin_id={bridge.origin_id}")
    print("Ctrl-C to exit")
    bridge.wait()
```

### 5.5 Tests

`bridge/python/smoke_test.py` — a sequential `__main__` script (no
pytest dependency), matching the core crate's `python/smoke_test.py`
convention so it runs in any CI with the two wheels installed:

| Check                           | Asserts                                       |
|---------------------------------|-----------------------------------------------|
| bad config dict                 | raises `BridgeConfigError`                     |
| empty bus name                  | raises `BridgeConfigError`                     |
| QUIC peer at `start()`          | raises `BridgeQuicError` (wheel is TCP-only)   |
| start / stop                    | `is_running()` flips; `listen_addr` resolves port; no hang |
| double start + double shutdown  | both idempotent; `origin_id` stable            |
| context manager                 | `with Bridge(cfg):` joins on exit              |
| explicit `origin_id`            | preserved through start                        |
| loopback round-trip             | Bridge A → Bridge B over `127.0.0.1`, payload arrives on B's bus |

The loopback round-trip is the integration heavyweight — it spins
up two `Bridge` instances with non-overlapping `base_dir`s and
asserts a message published on A's bus arrives on B's bus, proving
the wiring works end-to-end without the standalone binary.  The
receive-side subscribe is deterministic (see eager-producer note
below); only the publish loop retries, to cover the one inherently
async step — the A→B TCP connect + PeerHello auth — against a 25 s
safety deadline.

### 5.6 Eager producer creation (reliability)

The bridge's receive-side publisher used to create each topic's
producer + ring **lazily**, on the first republished message.  That
left a window where a local subscriber connecting right after bridge
startup either blocked or attached mid-creation and missed the first
forwarded message — the root cause of a long-standing macOS-flaky
Rust test (`psk_auth_smoke::good_psk_authenticates_and_republishes`,
previously `#[ignore]`d).

Fix: `publisher_main` now pre-creates the producer for every
`receive = true` topic at startup via
`bus.wait_for_subscribers(topic, 0, Duration::ZERO)` (which runs
`ensure_publisher` then returns immediately).  Subscribers attach to
a stable ring before any traffic flows.  This made the flaky test
deterministic (un-ignored; 20/20 under stress) and let the Python
loopback smoke check drop its subscribe-retry loop.

## 6. Risks & mitigations

| Risk                                              | Mitigation                              |
|---------------------------------------------------|-----------------------------------------|
| Wheel size grows (tokio etc. via bridge transitive deps) | Bridge with `default-features = false` skips QUIC; remaining deps (serde, toml, thiserror) are < 200 KB compiled |
| GIL deadlock if a callback re-enters Python       | No callbacks today — bridge fires no Python code. Future-proof by always releasing the GIL on long ops |
| `bridge.wait()` un-interruptible on Windows       | `py.check_signals()` every 100 ms in the wait loop; honoured on all platforms PyO3 supports |
| Cyclic Cargo dep (`mmbus` ↔ `mmbus-bridge`) blocks bundling | Resolved: ship a **separate** `mmbus-bridge` wheel; bindings live in the bridge crate. `pip install mmbus[bridge]` keeps it to one user-facing install command |
| Two wheels must stay version-locked | `bridge/pyproject.toml` pins `dependencies = ["mmbus==0.3.0"]`; CI builds + publishes both from the same tagged commit |
| Subprocess shim users confused by two APIs         | Module docstring + README "When to use which" section pointing in-process callers at `mmbus_bridge.Bridge` |

## 7. Acceptance criteria

- [ ] `pip install mmbus[bridge]` (or local `maturin develop` in
      `bridge/`) yields an importable `mmbus_bridge.Bridge` without
      `cargo install`-ing the standalone binary.
- [ ] `Bridge(cfg).start(); .shutdown()` works for a TCP-only config.
- [ ] Loopback roundtrip smoke check passes locally.
- [ ] `mmbus.bridge.run` / `mmbus.bridge.spawn` still work
      (no regression for subprocess users).
- [ ] QUIC config raises a typed error explaining the wheel is TCP-only.
- [ ] README has a "Cross-machine pub/sub" section pointing at the
      new class as the recommended embedded path.
- [ ] CHANGELOG `[0.3.0]` entry covers the new surface + breaking
      changes (none expected).

## 8. Out-of-scope follow-ups for v0.3.1+

- Stats on `PyBridge` (`bridge.stats() → {peers: [...], drops: N}`),
  needs the Rust `Bridge` struct to grow stats counters first.
- QUIC support in the wheel (gated by demand; pulls in
  tokio + quinn + rustls + ring + rcgen).
- `asyncio` wrapper (`from mmbus.aio import Bridge`) — a thin
  thread-pool-executor shim around the sync API.
- Hot config reload (today: shutdown + reconstruct).
