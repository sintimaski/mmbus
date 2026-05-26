//! `_RustBus` — the PyO3 wrapper around [`crate::Bus`].

use crate::bus::Bus;
use crate::config::{BackpressurePolicy, BusConfig};
use crate::publisher::Publisher;
use crate::python::exceptions::mmbus_err;
use crate::python::subscription::{PySubscription, PyTopicStats};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyBytesMethods};
use std::time::Duration;

/// Low-level Rust-backed Bus.  Prefer the Python ``Bus`` wrapper in
/// ``mmbus/__init__.py`` which adds async support and context-manager protocol.
#[pyclass(name = "_RustBus", module = "mmbus._mmbus")]
pub struct PyBus {
    inner: Bus,
}

#[pymethods]
impl PyBus {
    /// ``backpressure`` accepts ``"error"`` (default; raises
    /// :exc:`BusFullError` when full) or ``"drop_oldest"`` (silently
    /// overwrites the oldest unread slot; subscribers detect the skip
    /// via the per-slot seqlock).  Any other value raises ``ValueError``.
    #[new]
    #[pyo3(signature = (
        name,
        *,
        base_dir=None,
        capacity=None,
        slot_size=None,
        max_subscribers=None,
        backpressure=None,
        wal_enabled=None,
    ))]
    pub fn new(
        name: &str,
        base_dir: Option<String>,
        capacity: Option<u32>,
        slot_size: Option<u32>,
        max_subscribers: Option<u32>,
        backpressure: Option<&str>,
        wal_enabled: Option<bool>,
    ) -> PyResult<Self> {
        let defaults = BusConfig::default();
        let policy = match backpressure {
            None => defaults.backpressure.clone(),
            Some("error") => BackpressurePolicy::Error,
            Some("drop_oldest") => BackpressurePolicy::DropOldest,
            Some(other) => {
                return Err(PyValueError::new_err(format!(
                    "backpressure must be \"error\" or \"drop_oldest\", got {other:?}"
                )));
            }
        };
        // wal_enabled overrides the Rust default's `enabled` field.
        // The remaining WalConfig knobs (fsync_policy, etc.) stay at
        // their Rust defaults.  Future expansion: a full `wal=` dict /
        // dataclass kwarg surface.
        let wal = match wal_enabled {
            None => defaults.wal,
            Some(true) => crate::wal::WalConfig {
                enabled: true,
                ..defaults.wal
            },
            Some(false) => crate::wal::WalConfig::disabled(),
        };
        let config = BusConfig {
            base_dir: base_dir.map(Into::into).unwrap_or(defaults.base_dir),
            capacity: capacity.unwrap_or(defaults.capacity),
            slot_size: slot_size.unwrap_or(defaults.slot_size),
            max_subscribers: max_subscribers.unwrap_or(defaults.max_subscribers),
            backpressure: policy,
            wal,
        };
        Ok(PyBus {
            inner: Bus::with_config(name, config),
        })
    }

    /// Publish bytes to ``topic``.
    ///
    /// Raises :exc:`BusFullError` if the ring is saturated (backpressure
    /// policy ``Error``, the default).
    ///
    /// Releases the GIL: the wakeup-broadcast step can block briefly if a
    /// subscriber's wakeup-channel buffer is full (macOS socket SO_SNDBUF
    /// or Linux eventfd at U64::MAX).  Holding the GIL across that
    /// would deadlock against a Python subscriber thread trying to drain.
    fn publish(&mut self, py: Python<'_>, topic: &str, data: &[u8]) -> PyResult<()> {
        py.allow_threads(|| self.inner.publish(topic, data))
            .map_err(mmbus_err)
    }

    /// Publish a list of ``bytes`` to ``topic`` in a single call.
    /// Fires ONE wakeup syscall per connected subscriber regardless
    /// of batch size — pairs with :meth:`Subscription.recv_batch` for
    /// end-to-end burst throughput.
    ///
    /// Returns the number of records actually written.  Under the
    /// default ``Error`` backpressure policy this is less than
    /// ``len(payloads)`` if the ring filled mid-batch (caller can
    /// retry the tail).  Under ``drop_oldest`` it always equals
    /// ``len(payloads)``.
    ///
    /// Raises :exc:`MessageTooLargeError` if any single payload
    /// exceeds ``slot_size``; raises :exc:`WalError` on a WAL
    /// failure mid-batch.  Both surface AFTER the wakeup is fired
    /// for whatever was already written, so committed records stay
    /// observable.
    /// Publish a list of ``bytes`` to ``topic`` in a single call.
    ///
    /// Zero-copy: borrows each payload directly from the Python ``bytes``
    /// objects while holding the GIL — no per-payload allocation.  Ring
    /// writes are fast mmap stores + one wakeup syscall, so the GIL hold
    /// is short and bounded by batch size.
    fn publish_many(
        &mut self,
        _py: Python<'_>,
        topic: &str,
        payloads: Vec<Bound<'_, PyBytes>>,
    ) -> PyResult<usize> {
        let slices: Vec<&[u8]> = payloads.iter().map(|b| b.as_bytes()).collect();
        self.inner.publish_many(topic, slices).map_err(mmbus_err)
    }

    /// Return a :class:`TopicPublisher` bound to ``topic`` — a prepared
    /// handle for hot publish loops.  Each ``handle.publish(data)`` skips
    /// the per-call topic hash lookup, the ``str`` → UTF-8 conversion, and
    /// the Python wrapper frame that ``Bus.publish`` pays.
    ///
    /// The handle takes *exclusive* ownership of publishing to ``topic``:
    /// it adopts an already-cached publisher if one exists, otherwise
    /// creates it.  After this call, ``publish``/``publish_many`` on this
    /// ``_RustBus`` for the same topic would open a second publisher and
    /// raise :exc:`AlreadyPublishingError` — pick one API per topic.
    fn topic(&mut self, name: &str) -> PyResult<PyTopicPublisher> {
        self.inner
            .take_publisher(name)
            .map(PyTopicPublisher::new)
            .map_err(mmbus_err)
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
            .map(PySubscription::new)
            .map_err(mmbus_err)
    }

    /// Subscribe starting ``n_messages_back`` behind the current tail.
    /// Replays recent in-ring history (best effort, capped at ring capacity).
    /// Releases the GIL while waiting for the publisher.
    #[pyo3(signature = (topic, n_messages_back, timeout_secs=30.0))]
    fn subscribe_with_history(
        &self,
        py: Python<'_>,
        topic: &str,
        n_messages_back: u64,
        timeout_secs: f64,
    ) -> PyResult<PySubscription> {
        let timeout = Duration::from_secs_f64(timeout_secs);
        py.allow_threads(|| {
            self.inner
                .subscribe_with_history_timeout(topic, n_messages_back, timeout)
        })
        .map(PySubscription::new)
        .map_err(mmbus_err)
    }

    /// Subscribe starting at an explicit ``cursor`` value.  Raises
    /// :exc:`CursorTooOldError` if the cursor is older than the oldest
    /// in-ring slot at connect time.  Releases the GIL.
    #[pyo3(signature = (topic, cursor, timeout_secs=30.0))]
    fn subscribe_from(
        &self,
        py: Python<'_>,
        topic: &str,
        cursor: u64,
        timeout_secs: f64,
    ) -> PyResult<PySubscription> {
        let timeout = Duration::from_secs_f64(timeout_secs);
        py.allow_threads(|| self.inner.subscribe_from_timeout(topic, cursor, timeout))
            .map(PySubscription::new)
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
        self.inner.stats(topic).map(PyTopicStats::from)
    }

    /// Return ``(cursor_idx, lag)`` tuples for subscribers on ``topic``
    /// whose lag is ``>= threshold`` messages.  Empty list if no
    /// subscriber is behind or no publisher exists for this topic.
    fn slow_subscribers(&self, topic: &str, threshold: u64) -> Vec<(usize, u64)> {
        self.inner.slow_subscribers(topic, threshold)
    }

    /// Remove all on-disk state for ``topic``.  Raises
    /// :exc:`AlreadyPublishingError` if a publisher is active anywhere
    /// (including this process).  For test setup / dev tooling only.
    fn clean_topic(&mut self, topic: &str) -> PyResult<()> {
        self.inner.clean_topic(topic).map_err(mmbus_err)
    }
}

/// Prepared publish handle for a single topic — see :meth:`_RustBus.topic`.
///
/// Owns a resolved :class:`~mmbus.Bus`-internal publisher, so the publish
/// hot path is a direct call with no topic lookup and no string marshaling.
/// Holds the per-topic producer lock for its lifetime; the lock releases
/// when the handle is garbage-collected (or its ``with`` block exits).
#[pyclass(name = "TopicPublisher", module = "mmbus._mmbus")]
pub struct PyTopicPublisher {
    inner: Publisher,
}

impl PyTopicPublisher {
    pub(crate) fn new(inner: Publisher) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyTopicPublisher {
    /// Publish bytes to this handle's topic.  Raises :exc:`BusFullError`
    /// under the ``Error`` backpressure policy when the ring is saturated.
    /// Releases the GIL — see :meth:`_RustBus.publish` for why.
    fn publish(&mut self, py: Python<'_>, data: &[u8]) -> PyResult<()> {
        py.allow_threads(|| self.inner.publish(data))
            .map_err(mmbus_err)
    }

    /// Publish a list of ``bytes`` in one call, firing a single wakeup per
    /// subscriber.  Returns the number of records written.  Mirrors
    /// :meth:`_RustBus.publish_many` minus the topic argument.
    ///
    /// Zero-copy: borrows each payload directly from the Python ``bytes``
    /// objects — no per-payload allocation.
    fn publish_many(
        &mut self,
        _py: Python<'_>,
        payloads: Vec<Bound<'_, PyBytes>>,
    ) -> PyResult<usize> {
        let slices: Vec<&[u8]> = payloads.iter().map(|b| b.as_bytes()).collect();
        self.inner.publish_many(slices).map_err(mmbus_err)
    }

    /// Block until ``n`` subscribers are connected.  Releases the GIL.
    #[pyo3(signature = (n=1, timeout_secs=30.0))]
    fn wait_for_subscribers(
        &mut self,
        py: Python<'_>,
        n: usize,
        timeout_secs: f64,
    ) -> PyResult<()> {
        let timeout = Duration::from_secs_f64(timeout_secs);
        py.allow_threads(|| self.inner.wait_for_subscribers(n, timeout))
            .map_err(mmbus_err)
    }

    /// Return a :class:`TopicStats` snapshot for this handle's topic.
    fn stats(&self) -> PyTopicStats {
        PyTopicStats::from(self.inner.stats())
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
        // Producer lock releases when the handle is GC'd (Drop for Publisher).
        false
    }
}
