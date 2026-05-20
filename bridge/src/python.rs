//! PyO3 bindings — built as the `mmbus_bridge._mmbus_bridge` Python
//! extension via maturin when the `python` feature is on.
//!
//! The thin Python wrapper at `python/mmbus_bridge/__init__.py`
//! re-exports `Bridge` (a subclass that adds context-manager support).
//! Design rationale lives in `docs/rfc-bridge-python-sdk.md`.

// pyo3 0.22's create_exception! macro emits `#[cfg(feature = "gil-refs")]`
// gates that aren't ours — silence the resulting unexpected-cfg warnings.
#![allow(unexpected_cfgs)]
// pyo3 0.22's #[pymethods] macro generates `.into()` calls on already-PyErr
// values; not our code to fix.
#![allow(clippy::useless_conversion)]

use crate::{Bridge, BridgeConfig, BridgeError, ConfigError};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyType};
use std::path::PathBuf;
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

// ── Public Python exceptions ──────────────────────────────────────────────────

pyo3::create_exception!(mmbus_bridge, PyBridgeError, pyo3::exceptions::PyRuntimeError);
pyo3::create_exception!(mmbus_bridge, BridgeConfigError, pyo3::exceptions::PyValueError);
pyo3::create_exception!(mmbus_bridge, BridgeListenError, pyo3::exceptions::PyRuntimeError);
pyo3::create_exception!(mmbus_bridge, BridgeQuicError, pyo3::exceptions::PyRuntimeError);

#[pymodule]
fn _mmbus_bridge(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyBridge>()?;
    m.add("BridgeError", py.get_type_bound::<PyBridgeError>())?;
    m.add("BridgeConfigError", py.get_type_bound::<BridgeConfigError>())?;
    m.add("BridgeListenError", py.get_type_bound::<BridgeListenError>())?;
    m.add("BridgeQuicError", py.get_type_bound::<BridgeQuicError>())?;
    Ok(())
}

// ── PyBridge state machine ────────────────────────────────────────────────────

enum State {
    Configured(BridgeConfig),
    Running(Bridge),
    Shutdown,
    /// Transient state held only while a method swaps the variant
    /// (e.g. Configured → Running during `start()`).  Every method
    /// restores the lock to one of the three real states before
    /// returning to Python.
    Transitioning,
}

/// Low-level Rust-backed bridge.  Prefer the `mmbus_bridge.Bridge`
/// Python wrapper, which adds context-manager support.  `subclass` so
/// the Python wrapper can inherit from it.
#[pyclass(name = "_RustBridge", module = "mmbus_bridge._mmbus_bridge", subclass)]
struct PyBridge {
    state: Mutex<State>,
}

#[pymethods]
impl PyBridge {
    /// Construct from a Python dict mirroring the bridge TOML schema.
    ///
    /// The dict is JSON-stringified (stdlib `json`), parsed into a
    /// `serde_json::Value`, deserialised straight into `BridgeConfig`,
    /// then put through the same `validate()` the standalone binary's
    /// `from_str` runs — so validation rules stay in one place.
    #[new]
    fn new(config: &Bound<'_, PyDict>) -> PyResult<Self> {
        let value = dict_to_json_value(config)?;
        let cfg: BridgeConfig = serde_json::from_value(value).map_err(|e| {
            BridgeConfigError::new_err(format!("config dict not parseable as BridgeConfig: {e}"))
        })?;
        cfg.validate().map_err(config_err)?;
        Ok(Self {
            state: Mutex::new(State::Configured(cfg)),
        })
    }

    /// Parse from a TOML string.
    #[classmethod]
    fn from_toml(_cls: &Bound<'_, PyType>, text: &str) -> PyResult<Self> {
        let cfg = BridgeConfig::from_str(text).map_err(config_err)?;
        Ok(Self {
            state: Mutex::new(State::Configured(cfg)),
        })
    }

    /// Read + parse a TOML config file from disk.
    #[classmethod]
    fn from_path(_cls: &Bound<'_, PyType>, path: PathBuf) -> PyResult<Self> {
        let cfg = BridgeConfig::from_path(&path).map_err(config_err)?;
        Ok(Self {
            state: Mutex::new(State::Configured(cfg)),
        })
    }

    /// Spawn the bridge threads.  Idempotent — `start()` on a running
    /// bridge is a no-op.  Calling after `shutdown()` raises
    /// `RuntimeError`.
    fn start(&self, py: Python<'_>) -> PyResult<()> {
        let mut guard = self.state.lock().expect("PyBridge mutex poisoned");
        match std::mem::replace(&mut *guard, State::Transitioning) {
            State::Configured(cfg) => {
                // Bridge::start spawns threads + may block briefly on
                // QUIC runtime setup.  Release the GIL so any Python
                // thread waiting to publish to the local Bus isn't
                // starved during startup.
                let started = py.allow_threads(|| Bridge::start(&cfg));
                match started {
                    Ok(bridge) => {
                        *guard = State::Running(bridge);
                        Ok(())
                    }
                    Err(e) => {
                        *guard = State::Configured(cfg);
                        Err(bridge_err(e))
                    }
                }
            }
            other @ State::Running(_) => {
                *guard = other;
                Ok(())
            }
            State::Shutdown => {
                *guard = State::Shutdown;
                Err(PyRuntimeError::new_err(
                    "Bridge has already been shut down; construct a new one to restart",
                ))
            }
            State::Transitioning => unreachable!("Transitioning leaked across a method boundary"),
        }
    }

    /// Signal threads to stop and join them.  Idempotent.  Pass
    /// ``timeout`` (seconds) to bound the join wait; on timeout raises
    /// ``TimeoutError`` and detaches the join thread (the bridge keeps
    /// draining in the background).
    #[pyo3(signature = (timeout=None))]
    fn shutdown(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<()> {
        // Take the running bridge out of the shared state and mark it
        // Shutdown *before* releasing the lock — and do the GIL-releasing
        // join below with NO lock held.  Holding `guard` across
        // `py.allow_threads` would deadlock: a concurrent `is_running()` /
        // `wait()` poll (e.g. an asyncio `wait_async` loop) blocks on this
        // mutex while holding the GIL, and this thread needs the GIL back
        // after the join.  Setting Shutdown first also makes that poll
        // observe "stopped" immediately, so it stops polling.
        let bridge = {
            let mut guard = self.state.lock().expect("PyBridge mutex poisoned");
            match std::mem::replace(&mut *guard, State::Shutdown) {
                State::Running(bridge) => Some(bridge),
                // Configured-but-never-started, or already shut down: nothing
                // to join — the state is now Shutdown either way (idempotent).
                State::Configured(_) | State::Shutdown => None,
                State::Transitioning => {
                    unreachable!("Transitioning leaked across a method boundary")
                }
            }
        }; // guard dropped here — the join runs lock-free

        let bridge = match bridge {
            Some(b) => b,
            None => return Ok(()),
        };

        // mmbus_bridge::Bridge::shutdown consumes self and joins
        // unconditionally.  To time-bound the wait we move the bridge into a
        // worker thread and time-bound the parent's join() instead.
        let deadline = timeout.map(|secs| Instant::now() + Duration::from_secs_f64(secs));
        let handle = thread::spawn(move || {
            bridge.shutdown();
        });
        match py.allow_threads(|| join_with_deadline(handle, deadline)) {
            JoinOutcome::Done => Ok(()),
            JoinOutcome::TimedOut(handle) => {
                // Detach; the worker finishes joining the bridge threads in
                // the background.  State is already Shutdown.
                drop(handle);
                Err(PyErr::new::<pyo3::exceptions::PyTimeoutError, _>(
                    "Bridge.shutdown() timed out; threads detached",
                ))
            }
        }
    }

    /// Block the current Python thread until the bridge is shut down.
    /// Wakes every 100 ms to honour ``KeyboardInterrupt`` so REPL /
    /// script usage stays responsive.  Returns immediately if the
    /// bridge is not currently running.
    fn wait(&self, py: Python<'_>) -> PyResult<()> {
        loop {
            let still_running = {
                let guard = self.state.lock().expect("PyBridge mutex poisoned");
                matches!(*guard, State::Running(_))
            };
            if !still_running {
                return Ok(());
            }
            py.allow_threads(|| thread::sleep(Duration::from_millis(100)));
            py.check_signals()?;
        }
    }

    /// Whether the bridge threads are currently running.
    fn is_running(&self) -> bool {
        let guard = self.state.lock().expect("PyBridge mutex poisoned");
        matches!(*guard, State::Running(_))
    }

    /// 64-bit origin ID stamped into outbound frames.  Available after
    /// ``start()``; ``None`` before the first start or after shutdown.
    #[getter]
    fn origin_id(&self) -> Option<u64> {
        let guard = self.state.lock().expect("PyBridge mutex poisoned");
        match &*guard {
            State::Running(b) => Some(b.origin_id),
            _ => None,
        }
    }

    /// ``(host, port)`` the listener bound to, or ``None`` when the
    /// bridge has no ``listen`` configured / hasn't started yet.
    /// Resolves ``0.0.0.0:0`` to the actual ephemeral port.
    #[getter]
    fn listen_addr(&self) -> Option<(String, u16)> {
        let guard = self.state.lock().expect("PyBridge mutex poisoned");
        match &*guard {
            State::Running(b) => b.listen_addr.map(|a| (a.ip().to_string(), a.port())),
            _ => None,
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Convert a Python dict to a `serde_json::Value` via `json.dumps`.
/// Keeps the adapter trivially small: a schema change only touches the
/// Rust `BridgeConfig` struct, never this glue.
fn dict_to_json_value(dict: &Bound<'_, PyDict>) -> PyResult<serde_json::Value> {
    let py = dict.py();
    let json_module = py.import_bound("json")?;
    let json_text: String = json_module.call_method1("dumps", (dict,))?.extract()?;
    serde_json::from_str(&json_text)
        .map_err(|e| BridgeConfigError::new_err(format!("config dict not JSON-serialisable: {e}")))
}

fn config_err(e: ConfigError) -> PyErr {
    match e {
        ConfigError::Io(io) => PyErr::new::<pyo3::exceptions::PyOSError, _>(io.to_string()),
        other => BridgeConfigError::new_err(other.to_string()),
    }
}

fn bridge_err(e: BridgeError) -> PyErr {
    match e {
        // The core mmbus exception types live in the separate `_mmbus`
        // extension, which we can't reference from here.  Surface the
        // message under our own BridgeError base — mmbus errors out of
        // `Bridge::start` are rare (the listen-bind + QUIC paths are the
        // common failures).
        BridgeError::Mmbus(m) => PyBridgeError::new_err(format!("mmbus error: {m}")),
        BridgeError::Listen(io) => BridgeListenError::new_err(io.to_string()),
        BridgeError::QuicNotCompiled { reason } => BridgeQuicError::new_err(format!(
            "QUIC config rejected by TCP-only wheel ({reason}); use the standalone mmbus-bridge binary for QUIC"
        )),
        BridgeError::QuicSetup(msg) => BridgeQuicError::new_err(msg),
    }
}

enum JoinOutcome {
    Done,
    TimedOut(thread::JoinHandle<()>),
}

fn join_with_deadline(handle: thread::JoinHandle<()>, deadline: Option<Instant>) -> JoinOutcome {
    let Some(deadline) = deadline else {
        let _ = handle.join();
        return JoinOutcome::Done;
    };
    loop {
        if handle.is_finished() {
            let _ = handle.join();
            return JoinOutcome::Done;
        }
        if Instant::now() >= deadline {
            return JoinOutcome::TimedOut(handle);
        }
        thread::sleep(Duration::from_millis(20));
    }
}
