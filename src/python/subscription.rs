//! `Subscription` + `TopicStats` PyO3 wrappers.

use crate::error::Error;
use crate::python::exceptions::mmbus_err;
use crate::stats::TopicStats;
use crate::subscription::Subscription;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};
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
    /// Reusable scratch buffer for the recv hot path — saves one
    /// per-call `Vec` allocation vs the original API that returned
    /// a fresh `Vec<u8>` from `Subscription::recv()`.  Cleared on
    /// every recv; capacity grows to the largest payload seen.
    recv_buf: Vec<u8>,
}

impl PySubscription {
    pub(crate) fn new(inner: Subscription) -> Self {
        Self { inner, recv_buf: Vec::new() }
    }
}

/// Build a Python `bytes` object from a slice in one shot.
///
/// Uses `PyBytes::new_bound_with` so PyO3 allocates the PyBytes
/// once at the right length and we memcpy directly into its
/// internal buffer — no `Vec<u8>` intermediate, no double-copy.
/// (`PyBytes::new_bound(py, &slice)` does the same memcpy
/// internally, so this is mostly a stylistic guarantee that no
/// future refactor reintroduces a copy.)
fn payload_to_pybytes<'py>(py: Python<'py>, payload: &[u8]) -> Bound<'py, PyBytes> {
    PyBytes::new_bound_with(py, payload.len(), |buf| {
        buf.copy_from_slice(payload);
        Ok(())
    })
    .expect("PyBytes allocation cannot fail for a slice we already hold")
}

#[pymethods]
impl PySubscription {
    /// Block until the next message.  Releases the GIL.
    fn recv<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        py.allow_threads(|| self.inner.recv_into(&mut self.recv_buf))
            .map_err(mmbus_err)?;
        Ok(payload_to_pybytes(py, &self.recv_buf))
    }

    /// Block up to ``timeout_secs``.  Returns ``None`` on timeout.
    #[pyo3(signature = (timeout_secs=1.0))]
    fn recv_timeout<'py>(
        &mut self,
        py: Python<'py>,
        timeout_secs: f64,
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let timeout = Duration::from_secs_f64(timeout_secs);
        let got = py
            .allow_threads(|| self.inner.recv_timeout_into(timeout, &mut self.recv_buf))
            .map_err(mmbus_err)?;
        Ok(got.then(|| payload_to_pybytes(py, &self.recv_buf)))
    }

    /// Non-blocking poll.  Returns ``None`` immediately if no message is ready.
    fn try_recv<'py>(&mut self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        if self.inner.try_recv_into(&mut self.recv_buf) {
            Some(payload_to_pybytes(py, &self.recv_buf))
        } else {
            None
        }
    }

    /// Drain up to ``n`` messages into a list under a single GIL
    /// release.  Blocks up to ``timeout_secs`` for the FIRST message;
    /// once at least one has arrived, returns immediately with
    /// whatever is currently available (up to ``n``).  Designed for
    /// burst-heavy workloads where the per-call PyO3 + GIL overhead
    /// of single-message ``recv()`` dominates.
    ///
    /// Returns an empty list on timeout.
    #[pyo3(signature = (n, timeout_secs=1.0))]
    fn recv_batch<'py>(
        &mut self,
        py: Python<'py>,
        n: usize,
        timeout_secs: f64,
    ) -> PyResult<Bound<'py, PyList>> {
        if n == 0 {
            return Ok(PyList::empty_bound(py));
        }
        let timeout = Duration::from_secs_f64(timeout_secs);

        // Phase 1 (GIL released): block for the first message up to
        // `timeout`, then drain non-blockingly until either the ring
        // is empty or we've collected `n` payloads.  We accumulate
        // into `Vec<Vec<u8>>` so we can build the PyList in one shot
        // after re-acquiring the GIL.  Yes, this allocates per
        // message — but `recv_batch` is for amortising the PyO3
        // dispatch over many messages, not for chasing the absolute
        // minimum allocation count (that's `recv_into`'s job).
        let payloads: Vec<Vec<u8>> = py
            .allow_threads(|| -> Result<Vec<Vec<u8>>, Error> {
                let mut out = Vec::with_capacity(n);
                let mut first_buf = Vec::new();
                if !self.inner.recv_timeout_into(timeout, &mut first_buf)? {
                    return Ok(out); // timeout before any message
                }
                out.push(first_buf);
                while out.len() < n {
                    let mut buf = Vec::new();
                    if !self.inner.try_recv_into(&mut buf) {
                        break;
                    }
                    out.push(buf);
                }
                Ok(out)
            })
            .map_err(mmbus_err)?;

        // Phase 2 (GIL held): build the PyList.  `PyBytes::new_bound`
        // is the only unavoidable per-payload cost left — it allocs
        // a Python bytes object + memcpy's the payload in.
        let list = PyList::empty_bound(py);
        for p in &payloads {
            list.append(payload_to_pybytes(py, p))?;
        }
        Ok(list)
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
        // poll_recv stays on the Vec-returning API because it's
        // entered from a wakeup callback (no GIL release) — the
        // intermediate Vec is one-shot anyway.  If this ever becomes
        // a profile hit, add a `poll_recv_into` on Subscription.
        match self.inner.poll_recv() {
            Ok(Some(data)) => Ok(Some(payload_to_pybytes(py, &data))),
            Ok(None) => Ok(None),
            Err(e) => Err(mmbus_err(e)),
        }
    }

    // ── Iterator protocol ────────────────────────────────────────────────────

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        match py.allow_threads(|| self.inner.recv_into(&mut self.recv_buf)) {
            Ok(()) => Ok(Some(payload_to_pybytes(py, &self.recv_buf))),
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
