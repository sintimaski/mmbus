/// PyO3 bindings — exposes Bus, Subscription, and TopicStats to Python.
///
/// The extension module is compiled as `mmbus._mmbus`; the thin Python wrapper
/// at `python/mmbus/__init__.py` re-exports the public API and adds
/// `AsyncSubscription` for use with asyncio.
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::bus::{Bus, BusConfig, Error, Subscription};

// ── Custom Python exception classes ──────────────────────────────────────────

pyo3::create_exception!(mmbus, BusFullError, pyo3::exceptions::PyException);
pyo3::create_exception!(mmbus, MessageTooLargeError, pyo3::exceptions::PyException);
pyo3::create_exception!(mmbus, ConnectTimeoutError, pyo3::exceptions::PyException);
pyo3::create_exception!(mmbus, TooManySubscribersError, pyo3::exceptions::PyException);
pyo3::create_exception!(mmbus, AlreadyPublishingError, pyo3::exceptions::PyException);

fn mmbus_err(e: Error) -> PyErr {
    match e {
        Error::Full => BusFullError::new_err("ring buffer full"),
        Error::TooLarge { size, max } => {
            MessageTooLargeError::new_err(format!(
                "message is {size} bytes; slot_size is {max} bytes"
            ))
        }
        Error::Timeout(topic) => {
            ConnectTimeoutError::new_err(format!("timed out waiting for '{topic}'"))
        }
        Error::TooManySubscribers(limit) => {
            TooManySubscribersError::new_err(format!("subscriber limit for this topic is {limit}"))
        }
        Error::AlreadyPublishing(topic) => {
            AlreadyPublishingError::new_err(format!(
                "a publisher is already active for topic '{topic}'"
            ))
        }
        Error::Io(e) => PyErr::new::<pyo3::exceptions::PyOSError, _>(e.to_string()),
    }
}

// ── Bus ───────────────────────────────────────────────────────────────────────

/// Low-level Rust-backed Bus.  Prefer the Python ``Bus`` wrapper in
/// ``mmbus/__init__.py`` which adds async support and context-manager protocol.
#[pyclass(name = "_RustBus")]
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

    /// Publish bytes to ``topic``.
    ///
    /// Raises :exc:`BusFullError` if the ring is saturated (backpressure
    /// policy ``Error``, the default).
    fn publish(&mut self, topic: &str, data: &[u8]) -> PyResult<()> {
        self.inner.publish(topic, data).map_err(mmbus_err)
    }

    /// Subscribe to ``topic`` with a custom connection timeout (seconds).
    /// Releases the GIL while waiting for the publisher.
    ///
    /// Raises :exc:`ConnectTimeoutError` if ``timeout_secs`` elapses.
    #[pyo3(signature = (topic, timeout_secs=30.0))]
    fn subscribe(
        &self,
        py: Python<'_>,
        topic: &str,
        timeout_secs: f64,
    ) -> PyResult<PySubscription> {
        let timeout = Duration::from_secs_f64(timeout_secs);
        py.allow_threads(|| self.inner.subscribe_timeout(topic, timeout))
            .map(|s| PySubscription { inner: s })
            .map_err(mmbus_err)
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
            .map_err(mmbus_err)
    }

    /// Return a :class:`TopicStats` snapshot, or ``None`` if no publisher
    /// exists for this topic in the current process.
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

/// Active subscription to a topic.
///
/// Supports both synchronous and context-manager use::
///
///     with bus.subscribe("events") as sub:
///         for msg in sub:
///             print(msg)
///
/// Use :class:`mmbus.AsyncSubscription` for asyncio.
#[pyclass(name = "Subscription")]
pub struct PySubscription {
    inner: Subscription,
}

#[pymethods]
impl PySubscription {
    /// Block until the next message.  Releases the GIL.
    fn recv<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let data = py.allow_threads(|| self.inner.recv()).map_err(mmbus_err)?;
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
        let result =
            py.allow_threads(|| self.inner.recv_timeout(timeout)).map_err(mmbus_err)?;
        Ok(result.map(|data| PyBytes::new_bound(py, &data)))
    }

    /// Non-blocking poll.  Returns ``None`` immediately if no message is ready.
    fn try_recv<'py>(&mut self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        self.inner.try_recv().map(|data| PyBytes::new_bound(py, &data))
    }

    /// How many messages this subscriber is behind the producer.
    #[getter]
    fn lag(&self) -> u64 {
        self.inner.lag()
    }

    /// Current read cursor position.
    #[getter]
    fn cursor(&self) -> u64 {
        self.inner.cursor()
    }

    /// The underlying wakeup fd (eventfd on Linux, socket on macOS).
    /// Pass to ``asyncio.get_event_loop().add_reader()`` for zero-thread async.
    fn fileno(&self) -> i32 {
        self.inner.fileno()
    }

    // ── Iterator protocol ────────────────────────────────────────────────────

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        match py.allow_threads(|| self.inner.recv()) {
            Ok(data) => Ok(Some(PyBytes::new_bound(py, &data))),
            Err(Error::Io(e))
                if e.kind() == std::io::ErrorKind::UnexpectedEof
                    || e.kind() == std::io::ErrorKind::ConnectionReset =>
            {
                Ok(None) // StopIteration
            }
            Err(e) => Err(mmbus_err(e)),
        }
    }

    // ── Context-manager protocol ──────────────────────────────────────────────

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &mut self,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> bool {
        // Cursor is released when this object is GC'd (Drop for Subscriber).
        // Return false: do not suppress any exception.
        false
    }
}

// ── TopicStats ────────────────────────────────────────────────────────────────

/// Ring-buffer and socket snapshot for a topic.
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
pub fn _mmbus(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyBus>()?;
    m.add_class::<PySubscription>()?;
    m.add_class::<PyTopicStats>()?;

    // Register exception classes so callers can `except mmbus.BusFullError`.
    m.add("BusFullError", py.get_type_bound::<BusFullError>())?;
    m.add("MessageTooLargeError", py.get_type_bound::<MessageTooLargeError>())?;
    m.add("ConnectTimeoutError", py.get_type_bound::<ConnectTimeoutError>())?;
    m.add("TooManySubscribersError", py.get_type_bound::<TooManySubscribersError>())?;
    m.add("AlreadyPublishingError", py.get_type_bound::<AlreadyPublishingError>())?;

    Ok(())
}
