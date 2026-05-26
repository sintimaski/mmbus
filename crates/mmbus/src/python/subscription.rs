//! `Subscription` + `TopicStats` PyO3 wrappers.

use crate::error::Error;
use crate::python::exceptions::mmbus_err;
use crate::stats::TopicStats;
use crate::subscription::Subscription;
use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};
use std::time::{Duration, Instant};

/// Active subscription to a topic.
///
/// Supports both synchronous and context-manager use:
///
/// ```text
/// with bus.subscribe("events") as sub:
///     for msg in sub:
///         print(msg)
/// ```
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
        Self {
            inner,
            recv_buf: Vec::new(),
        }
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

/// Resolve a writable, C-contiguous bytes-like object (``bytearray``,
/// writable ``memoryview``, numpy ``uint8`` array) to a `PyBuffer<u8>`.
///
/// The returned `PyBuffer` pins the exporter's memory (the bytearray
/// can't be resized or freed while it's held), so the caller may derive
/// a `&mut [u8]` from `buf_ptr()`/`len_bytes()` and read into it.  The
/// caller MUST keep the returned `PyBuffer` alive for as long as the
/// slice is used, and must hold the GIL while writing through it.
fn writable_view(buf: &Bound<'_, PyAny>) -> PyResult<PyBuffer<u8>> {
    let pybuf = PyBuffer::<u8>::get_bound(buf)?;
    if pybuf.readonly() {
        return Err(PyTypeError::new_err(
            "recv_into requires a writable buffer (got read-only)",
        ));
    }
    if !pybuf.is_c_contiguous() {
        return Err(PyTypeError::new_err(
            "recv_into requires a C-contiguous buffer",
        ));
    }
    Ok(pybuf)
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

    /// Block for the next message and write it **directly into ``buf``**,
    /// returning the number of bytes written.  ``buf`` is any writable,
    /// C-contiguous bytes-like object — ``bytearray``, a writable
    /// ``memoryview``, or a numpy ``uint8`` array.
    ///
    /// Unlike :meth:`recv` (which allocates a fresh ``bytes`` per message),
    /// this copies the payload straight into the caller's buffer with no
    /// per-message allocation.  Reuse one buffer across the receive loop for
    /// an allocation-free, low-GC-pressure pipeline (the intended path for
    /// numpy / tensor workloads).
    ///
    /// Raises :exc:`MessageTooLargeError` if the message is larger than
    /// ``buf``.  Size ``buf`` to :attr:`max_payload_size` to guarantee any
    /// message fits.  Raises on publisher disconnect, like :meth:`recv`.
    fn recv_into(&mut self, py: Python<'_>, buf: &Bound<'_, PyAny>) -> PyResult<usize> {
        let view = writable_view(buf)?;
        // SAFETY: `view` (a held PyBuffer) pins the exporter's memory for
        // its lifetime, so the pointer stays valid across the GIL-released
        // wait below.  We only WRITE through the slice with the GIL held
        // (inside try_recv_one_into_slice); the allow_threads closure does
        // not touch it.  The buffer is verified writable + C-contiguous.
        let slice =
            unsafe { std::slice::from_raw_parts_mut(view.buf_ptr() as *mut u8, view.len_bytes()) };
        loop {
            if let Some(n) = self
                .inner
                .try_recv_one_into_slice(slice)
                .map_err(mmbus_err)?
            {
                return Ok(n);
            }
            // GIL released: wait for the next wakeup without touching `slice`.
            py.allow_threads(|| self.inner.wait_readable(-1))
                .map_err(mmbus_err)?;
        }
    }

    /// Like :meth:`recv_into` but gives up after ``timeout_secs``.  Returns
    /// the byte count on success, or ``None`` on timeout.
    #[pyo3(signature = (buf, timeout_secs=1.0))]
    fn recv_timeout_into(
        &mut self,
        py: Python<'_>,
        buf: &Bound<'_, PyAny>,
        timeout_secs: f64,
    ) -> PyResult<Option<usize>> {
        let view = writable_view(buf)?;
        // SAFETY: see recv_into — `view` pins the memory; the slice is only
        // written with the GIL held; the wait runs without touching it.
        let slice =
            unsafe { std::slice::from_raw_parts_mut(view.buf_ptr() as *mut u8, view.len_bytes()) };
        let deadline = Instant::now() + Duration::from_secs_f64(timeout_secs);
        loop {
            if let Some(n) = self
                .inner
                .try_recv_one_into_slice(slice)
                .map_err(mmbus_err)?
            {
                return Ok(Some(n));
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(None);
            }
            let ms = remaining.as_millis().min(i32::MAX as u128) as i32;
            let woke = py
                .allow_threads(|| self.inner.wait_readable(ms))
                .map_err(mmbus_err)?;
            if !woke {
                return Ok(None); // timed out in the wait
            }
        }
    }

    /// Non-blocking variant of :meth:`recv_into`.  Writes the next message
    /// into ``buf`` and returns the byte count, or ``None`` immediately if
    /// no message is ready.  GIL held throughout — a single memcpy from the
    /// ring slot straight into ``buf``, no allocation.
    fn try_recv_into(
        &mut self,
        _py: Python<'_>,
        buf: &Bound<'_, PyAny>,
    ) -> PyResult<Option<usize>> {
        let view = writable_view(buf)?;
        // SAFETY: `view` pins the memory; GIL held for the whole call so the
        // pointer cannot be invalidated; verified writable + C-contiguous.
        let slice =
            unsafe { std::slice::from_raw_parts_mut(view.buf_ptr() as *mut u8, view.len_bytes()) };
        self.inner.try_recv_one_into_slice(slice).map_err(mmbus_err)
    }

    /// The largest payload a single message can carry on this topic (the
    /// ring's fixed slot size).  Size a :meth:`recv_into` buffer to this to
    /// guarantee no message is ever rejected as too large.
    #[getter]
    fn max_payload_size(&self) -> u32 {
        self.inner.slot_size()
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
        let pybuf = writable_view(buf)?;
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

    /// The underlying wakeup primitive — eventfd on Linux, Unix
    /// socket on macOS, semaphore HANDLE on Windows.  Pass to
    /// ``asyncio.get_event_loop().add_reader()`` for zero-thread
    /// async on Unix; on Windows the HANDLE is mostly diagnostic
    /// (asyncio uses IOCP, not add_reader).
    ///
    /// Returned as i64 to fit both Unix `RawFd` (i32) and Windows
    /// HANDLE (isize, which is i64 on 64-bit targets).  Python
    /// just sees an int.
    fn fileno(&self) -> i64 {
        self.inner.fileno() as i64
    }

    /// Handshake-socket fd/HANDLE for disconnect detection.  On
    /// Linux this differs from :meth:`fileno` (eventfd vs socket);
    /// on macOS it equals :meth:`fileno`; on Windows this is the
    /// named-pipe HANDLE value.  Returned as i64 — see
    /// :meth:`fileno` for the rationale.
    fn socket_fileno(&self) -> i64 {
        self.inner.socket_fileno() as i64
    }

    /// Arm the wakeup flag before an ``add_reader`` wait (asyncio path).
    /// Call after :meth:`poll_recv` returns ``None``: returns ``True`` if a
    /// message became available while arming (read again immediately, don't
    /// await), ``False`` if the flag is armed and the caller should await fd
    /// readability — the publisher's next publish fires the wakeup.
    ///
    /// Required for the coalescing handshake: the publisher only wakes
    /// subscribers whose flag is set, so an event-loop waiter must arm
    /// before sleeping or it would never be woken.
    fn arm_wakeup(&mut self) -> bool {
        self.inner.arm_wakeup()
    }

    /// Drain one pending wakeup unit so the (level-triggered) wakeup fd
    /// stops signalling; read the ring separately via :meth:`try_recv`.
    /// Returns ``True`` if a unit was drained.  Raises ``OSError`` on
    /// publisher disconnect.  Used by the asyncio ``add_reader`` callback.
    fn drain_wakeup(&mut self) -> PyResult<bool> {
        self.inner.drain_wakeup().map_err(mmbus_err)
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

/// Ring-buffer, socket, and counters snapshot for a topic.
///
/// `*_total` fields are monotonic counters since `Publisher::create`.
/// Use them as Prometheus-style rate sources (e.g. compute
/// `delta(published_total[1m])/60` for publishes/sec).
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
    #[pyo3(get)]
    pub published_total: u64,
    #[pyo3(get)]
    pub full_rejected_total: u64,
    #[pyo3(get)]
    pub subscribers_dropped_total: u64,
    #[pyo3(get)]
    pub wakeups_sent_total: u64,
    /// WAL snapshot when the publisher's WAL is enabled; `None`
    /// otherwise.
    #[pyo3(get)]
    pub wal: Option<PyWalStats>,
}

impl From<TopicStats> for PyTopicStats {
    fn from(s: TopicStats) -> Self {
        Self {
            tail: s.ring.tail,
            active_subscribers: s.ring.active_subscribers,
            lags: s.ring.lags,
            connected_sockets: s.connected_sockets,
            published_total: s.published_total,
            full_rejected_total: s.full_rejected_total,
            subscribers_dropped_total: s.subscribers_dropped_total,
            wakeups_sent_total: s.wakeups_sent_total,
            wal: s.wal.map(Into::into),
        }
    }
}

#[pymethods]
impl PyTopicStats {
    fn __repr__(&self) -> String {
        format!(
            "TopicStats(tail={}, active_subscribers={}, lags={:?}, \
             connected_sockets={}, published_total={}, \
             full_rejected_total={}, subscribers_dropped_total={}, \
             wakeups_sent_total={}, wal={})",
            self.tail,
            self.active_subscribers,
            self.lags,
            self.connected_sockets,
            self.published_total,
            self.full_rejected_total,
            self.subscribers_dropped_total,
            self.wakeups_sent_total,
            self.wal.as_ref().map(|_| "<WalStats>").unwrap_or("None"),
        )
    }
}

/// WAL snapshot: cursors, segment counts, on-disk byte usage,
/// and monotonic op counters.  Mirrors Rust's
/// [`mmbus::wal::WalStats`].
#[pyclass(name = "WalStats", module = "mmbus._mmbus")]
#[derive(Clone)]
pub struct PyWalStats {
    #[pyo3(get)]
    pub pending_cursor: u64,
    #[pyo3(get)]
    pub durable_cursor: u64,
    #[pyo3(get)]
    pub oldest_cursor: u64,
    #[pyo3(get)]
    pub active_segment_bytes: u64,
    #[pyo3(get)]
    pub total_wal_bytes: u64,
    #[pyo3(get)]
    pub segments: usize,
    #[pyo3(get)]
    pub appends_total: u64,
    #[pyo3(get)]
    pub append_bytes_total: u64,
    #[pyo3(get)]
    pub flushes_total: u64,
}

impl From<crate::wal::WalStats> for PyWalStats {
    fn from(s: crate::wal::WalStats) -> Self {
        Self {
            pending_cursor: s.pending_cursor,
            durable_cursor: s.durable_cursor,
            oldest_cursor: s.oldest_cursor,
            active_segment_bytes: s.active_segment_bytes,
            total_wal_bytes: s.total_wal_bytes,
            segments: s.segments,
            appends_total: s.appends_total,
            append_bytes_total: s.append_bytes_total,
            flushes_total: s.flushes_total,
        }
    }
}

#[pymethods]
impl PyWalStats {
    fn __repr__(&self) -> String {
        format!(
            "WalStats(pending={}, durable={}, oldest={}, \
             active_bytes={}, total_bytes={}, segments={}, \
             appends_total={}, append_bytes_total={}, flushes_total={})",
            self.pending_cursor,
            self.durable_cursor,
            self.oldest_cursor,
            self.active_segment_bytes,
            self.total_wal_bytes,
            self.segments,
            self.appends_total,
            self.append_bytes_total,
            self.flushes_total,
        )
    }
}
