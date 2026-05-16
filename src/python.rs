/// PyO3 bindings — exposes Bus, Subscription, and TopicStats to Python.
///
/// The extension module is compiled as `mmbus._mmbus`; the thin Python wrapper
/// at `python/mmbus/__init__.py` re-exports the public names so callers just do:
///
///   from mmbus import Bus
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::bus::{Bus, BusConfig, Error, Subscription};

// ── Bus ───────────────────────────────────────────────────────────────────────

/// Named pub-sub namespace. One instance can both publish and subscribe.
///
/// Parameters
/// ----------
/// name : str
///     Logical bus name — used as a sub-directory under `base_dir`.
/// base_dir : str, optional
///     Root directory for ring-buffer files (default: ``/tmp/mmbus``).
/// capacity : int, optional
///     Ring-buffer slot count per topic (default: 256).
/// slot_size : int, optional
///     Max bytes per message (default: 65536).
/// max_subscribers : int, optional
///     Maximum simultaneous subscribers per topic (default: 16).
///
/// Examples
/// --------
/// >>> bus = Bus("my-app")
/// >>> bus.publish("events", b"hello")
/// >>> sub = bus.subscribe("events")
/// >>> for msg in sub:
/// ...     print(msg)
#[pyclass(name = "Bus")]
pub struct PyBus {
    inner: Bus,
}

#[pymethods]
impl PyBus {
    #[new]
    #[pyo3(signature = (name, *, base_dir=None, capacity=None, slot_size=None, max_subscribers=None))]
    pub fn new(
        name: &str,
        base_dir: Option<String>,
        capacity: Option<u32>,
        slot_size: Option<u32>,
        max_subscribers: Option<u32>,
    ) -> Self {
        let defaults = BusConfig::default();
        let config = BusConfig {
            base_dir: base_dir.map(Into::into).unwrap_or(defaults.base_dir),
            capacity: capacity.unwrap_or(defaults.capacity),
            slot_size: slot_size.unwrap_or(defaults.slot_size),
            max_subscribers: max_subscribers.unwrap_or(defaults.max_subscribers),
            ..defaults
        };
        PyBus { inner: Bus::with_config(name, config) }
    }

    /// Publish ``data`` to ``topic``.  Raises ``RuntimeError`` when the ring
    /// is full (backpressure policy is ``Error``, the default).
    fn publish(&mut self, topic: &str, data: &[u8]) -> PyResult<()> {
        self.inner
            .publish(topic, data)
            .map_err(runtime_err)
    }

    /// Subscribe to ``topic``, blocking until the publisher is ready.
    /// Releases the GIL while waiting so other Python threads can progress.
    fn subscribe(&self, py: Python<'_>, topic: &str) -> PyResult<PySubscription> {
        py.allow_threads(|| self.inner.subscribe(topic))
            .map(|s| PySubscription { inner: s })
            .map_err(runtime_err)
    }

    /// Subscribe with a custom timeout in seconds.
    #[pyo3(signature = (topic, timeout_secs=30.0))]
    fn subscribe_timeout(
        &self,
        py: Python<'_>,
        topic: &str,
        timeout_secs: f64,
    ) -> PyResult<PySubscription> {
        let timeout = Duration::from_secs_f64(timeout_secs);
        py.allow_threads(|| self.inner.subscribe_timeout(topic, timeout))
            .map(|s| PySubscription { inner: s })
            .map_err(runtime_err)
    }

    /// Block until ``n`` subscribers are connected to ``topic``.
    /// Releases the GIL while polling.
    #[pyo3(signature = (topic, n=1, timeout_secs=30.0))]
    fn wait_for_subscribers(
        &mut self,
        py: Python<'_>,
        topic: &str,
        n: usize,
        timeout_secs: f64,
    ) -> PyResult<()> {
        let timeout = Duration::from_secs_f64(timeout_secs);
        py.allow_threads(|| self.inner.wait_for_subscribers(topic, n, timeout))
            .map_err(runtime_err)
    }

    /// Return a :class:`TopicStats` snapshot for ``topic``, or ``None`` if no
    /// publisher has been created for this topic in this Bus instance.
    fn stats(&self, topic: &str) -> Option<PyTopicStats> {
        self.inner.stats(topic).map(|s| PyTopicStats {
            tail: s.ring.tail,
            active_subscribers: s.ring.active_subscribers,
            lags: s.ring.lags.clone(),
            connected_sockets: s.connected_sockets,
        })
    }
}

// ── Subscription ──────────────────────────────────────────────────────────────

/// Active subscription to a topic.  Iterable — each iteration blocks for the
/// next message and releases the GIL so other Python threads can run.
///
/// Methods
/// -------
/// recv() -> bytes
///     Block until the next message arrives.
/// recv_timeout(timeout_secs) -> bytes | None
///     Block up to ``timeout_secs``; return ``None`` on timeout.
/// try_recv() -> bytes | None
///     Non-blocking poll; return ``None`` immediately if no message is ready.
#[pyclass(name = "Subscription")]
pub struct PySubscription {
    inner: Subscription,
}

#[pymethods]
impl PySubscription {
    /// Block until the next message arrives.  Releases the GIL.
    fn recv<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let data = py
            .allow_threads(|| self.inner.recv())
            .map_err(runtime_err)?;
        Ok(PyBytes::new_bound(py, &data))
    }

    /// Block up to ``timeout_secs``.  Returns ``None`` on timeout.
    #[pyo3(signature = (timeout_secs=1.0))]
    fn recv_timeout<'py>(
        &mut self,
        py: Python<'py>,
        timeout_secs: f64,
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let timeout = Duration::from_secs_f64(timeout_secs);
        let result = py
            .allow_threads(|| self.inner.recv_timeout(timeout))
            .map_err(runtime_err)?;
        Ok(result.map(|data| PyBytes::new_bound(py, &data)))
    }

    /// Non-blocking poll.  Returns ``None`` immediately if no message is ready.
    fn try_recv<'py>(&mut self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        self.inner.try_recv().map(|data| PyBytes::new_bound(py, &data))
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Iterator protocol — blocks for the next message, releases GIL.
    /// Raises ``StopIteration`` when the publisher disconnects.
    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        match py.allow_threads(|| self.inner.recv()) {
            Ok(data) => Ok(Some(PyBytes::new_bound(py, &data))),
            Err(Error::Io(e))
                if e.kind() == std::io::ErrorKind::UnexpectedEof
                    || e.kind() == std::io::ErrorKind::ConnectionReset =>
            {
                Ok(None) // triggers StopIteration
            }
            Err(e) => Err(runtime_err(e)),
        }
    }
}

// ── TopicStats ────────────────────────────────────────────────────────────────

/// Snapshot of a topic's ring-buffer and socket state.
///
/// Attributes
/// ----------
/// tail : int
///     Next slot position the producer will write.
/// active_subscribers : int
///     Number of claimed subscriber cursor slots.
/// lags : list[int]
///     Per-subscriber lag in messages (tail − cursor).
/// connected_sockets : int
///     Number of subscriber sockets accepted by this publisher instance.
#[pyclass(name = "TopicStats")]
#[derive(Clone)]
pub struct PyTopicStats {
    #[pyo3(get)]
    pub tail: u64,
    #[pyo3(get)]
    pub active_subscribers: usize,
    #[pyo3(get)]
    pub lags: Vec<u64>,
    #[pyo3(get)]
    pub connected_sockets: usize,
}

#[pymethods]
impl PyTopicStats {
    fn __repr__(&self) -> String {
        format!(
            "TopicStats(tail={}, active_subscribers={}, lags={:?}, connected_sockets={})",
            self.tail, self.active_subscribers, self.lags, self.connected_sockets
        )
    }
}

// ── Module entry point ────────────────────────────────────────────────────────

#[pymodule]
pub fn _mmbus(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyBus>()?;
    m.add_class::<PySubscription>()?;
    m.add_class::<PyTopicStats>()?;
    Ok(())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn runtime_err(e: Error) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(e.to_string())
}

