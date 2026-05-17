//! `Subscription` + `TopicStats` PyO3 wrappers.

use crate::error::Error;
use crate::python::exceptions::mmbus_err;
use crate::stats::TopicStats;
use crate::subscription::Subscription;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::time::Duration;

/// Active subscription to a topic.
///
/// Supports both synchronous and context-manager use::
///
///     with bus.subscribe("events") as sub:
///         for msg in sub:
///             print(msg)
///
/// Use :class:`mmbus.AsyncSubscription` for asyncio.
#[pyclass(name = "Subscription", module = "mmbus._mmbus")]
pub struct PySubscription {
    inner: Subscription,
}

impl PySubscription {
    pub(crate) fn new(inner: Subscription) -> Self {
        Self { inner }
    }
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

    /// Handshake-socket fd for disconnect detection (Linux only differs from
    /// :meth:`fileno`).  Register this alongside ``fileno`` with the event
    /// loop so publisher death is detected while idle.
    fn socket_fileno(&self) -> i32 {
        self.inner.socket_fileno()
    }

    /// Non-blocking: drain one wakeup signal and try one ring read.
    /// Returns ``None`` if no wakeup was pending or the ring was empty;
    /// raises on publisher disconnect.  Use from ``asyncio.add_reader``
    /// callbacks for true zero-thread async.
    fn poll_recv<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        match self.inner.poll_recv() {
            Ok(Some(data)) => Ok(Some(PyBytes::new_bound(py, &data))),
            Ok(None) => Ok(None),
            Err(e) => Err(mmbus_err(e)),
        }
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
#[pyclass(name = "TopicStats", module = "mmbus._mmbus")]
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

impl From<TopicStats> for PyTopicStats {
    fn from(s: TopicStats) -> Self {
        Self {
            tail: s.ring.tail,
            active_subscribers: s.ring.active_subscribers,
            lags: s.ring.lags,
            connected_sockets: s.connected_sockets,
        }
    }
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
