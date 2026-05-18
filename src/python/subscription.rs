//! `Subscription` + `TopicStats` PyO3 wrappers.

use crate::error::Error;
use crate::python::exceptions::mmbus_err;
use crate::stats::TopicStats;
use crate::subscription::Subscription;
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyTypeError, PyValueError};
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

    /// Drain up to ``n`` messages into a list, amortising the
    /// PyO3 + GIL overhead of single-message ``recv()``.
    ///
    /// Blocks up to ``timeout_secs`` for the FIRST message (GIL
    /// released).  Once at least one has arrived, drains the rest
    /// non-blockingly with the GIL HELD — ``try_recv`` is a
    /// memory-mapped read with no syscall, so releasing the GIL
    /// per message would just add round-trip cost.  The
    /// PySubscription's reusable ``recv_buf`` is reused for every
    /// message in the batch (one ring→buf memcpy per message; one
    /// PyBytes alloc + buf→PyBytes memcpy per message).
    ///
    /// Returns an empty list on timeout.
    #[pyo3(signature = (n, timeout_secs=1.0))]
    fn recv_batch<'py>(
        &mut self,
        py: Python<'py>,
        n: usize,
        timeout_secs: f64,
    ) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty_bound(py);
        if n == 0 {
            return Ok(list);
        }
        let timeout = Duration::from_secs_f64(timeout_secs);

        // Phase 1 (GIL released): block for the first message.
        let got_first = py
            .allow_threads(|| self.inner.recv_timeout_into(timeout, &mut self.recv_buf))
            .map_err(mmbus_err)?;
        if !got_first {
            return Ok(list); // timeout before any message
        }
        list.append(payload_to_pybytes(py, &self.recv_buf))?;

        // Phase 2 (GIL held): drain non-blockingly until ring is
        // empty or n is reached.  No allow_threads per iteration —
        // try_recv_into is a cheap mmap read and the whole point
        // of recv_batch is to NOT pay per-message GIL tax.
        while list.len() < n {
            if !self.inner.try_recv_into(&mut self.recv_buf) {
                break;
            }
            list.append(payload_to_pybytes(py, &self.recv_buf))?;
        }
        Ok(list)
    }

    /// Drain up to ``len(buf) // payload_size`` messages directly
    /// into ``buf``, returning the number of messages written.
    ///
    /// ``buf`` is any writable, C-contiguous bytes-like object —
    /// ``bytearray``, ``memoryview``, or a numpy ``ndarray`` of
    /// ``uint8`` (numpy exposes the buffer protocol automatically).
    /// Every drained payload must be EXACTLY ``payload_size`` bytes;
    /// a mismatch raises :exc:`ValueError`.  Designed for fixed-size
    /// message workloads (sensor readings, feature vectors,
    /// ML pipelines) where the caller already has a pre-allocated
    /// numpy buffer they'd otherwise memcpy ``recv()`` bytes into.
    ///
    /// Blocks up to ``timeout_secs`` for the FIRST message; drains
    /// the rest non-blockingly with the GIL held (same model as
    /// :meth:`recv_batch`).  Returns 0 on timeout.
    ///
    /// Zero allocations per message: ring-slot bytes are memcpy'd
    /// straight into ``buf``, no intermediate ``Vec`` or ``bytes``
    /// object.
    #[pyo3(signature = (buf, payload_size, timeout_secs=1.0))]
    fn recv_into_buffer(
        &mut self,
        py: Python<'_>,
        buf: &Bound<'_, PyAny>,
        payload_size: usize,
        timeout_secs: f64,
    ) -> PyResult<usize> {
        if payload_size == 0 {
            return Err(PyValueError::new_err("payload_size must be > 0"));
        }
        let pybuf = PyBuffer::<u8>::get_bound(buf)?;
        if pybuf.readonly() {
            return Err(PyTypeError::new_err(
                "recv_into_buffer requires a writable buffer (got read-only)",
            ));
        }
        if !pybuf.is_c_contiguous() {
            return Err(PyTypeError::new_err(
                "recv_into_buffer requires a C-contiguous buffer",
            ));
        }
        let buf_len = pybuf.len_bytes();
        let capacity = buf_len / payload_size;
        if capacity == 0 {
            return Ok(0);
        }

        // SAFETY: we hold the GIL via `py`; PyBuffer::get_bound
        // verified the pointer is valid for `buf_len` bytes; we do
        // not call into Python (which could trigger GC + buffer
        // invalidation) between this borrow and the end of the
        // function.
        let buf_ptr = pybuf.buf_ptr() as *mut u8;
        let buf_slice = unsafe { std::slice::from_raw_parts_mut(buf_ptr, buf_len) };

        let timeout = Duration::from_secs_f64(timeout_secs);

        // Phase 1 (GIL released): block for the first message.
        // recv_timeout_into fills self.recv_buf; we then check the
        // length contract + memcpy into row 0 of the user buffer.
        let got_first = py
            .allow_threads(|| self.inner.recv_timeout_into(timeout, &mut self.recv_buf))
            .map_err(mmbus_err)?;
        if !got_first {
            return Ok(0);
        }
        if self.recv_buf.len() != payload_size {
            return Err(PyValueError::new_err(format!(
                "payload at index 0 is {} bytes; payload_size is {}",
                self.recv_buf.len(),
                payload_size
            )));
        }
        buf_slice[..payload_size].copy_from_slice(&self.recv_buf);
        let mut count: usize = 1;

        // Phase 2 (GIL held): drain straight into the user buffer
        // via try_receive_into_slice — no Vec intermediate, no
        // PyBytes alloc.  Returns Some(usize::MAX) on oversize
        // payloads; we surface that as ValueError after advancing
        // the cursor so the subscriber doesn't loop on the bad slot.
        while count < capacity {
            let dst_offset = count * payload_size;
            let dst = &mut buf_slice[dst_offset..dst_offset + payload_size];
            match self.inner.try_recv_into_slice(dst) {
                Some(bytes_written) if bytes_written == payload_size => {
                    count += 1;
                }
                Some(bytes_written) if bytes_written == usize::MAX => {
                    return Err(PyValueError::new_err(format!(
                        "payload at index {count} exceeds payload_size ({payload_size})"
                    )));
                }
                Some(short) => {
                    return Err(PyValueError::new_err(format!(
                        "payload at index {count} is {short} bytes; payload_size is {payload_size}"
                    )));
                }
                None => break, // ring exhausted
            }
        }

        Ok(count)
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
