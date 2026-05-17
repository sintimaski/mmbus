//! `_RustBus` — the PyO3 wrapper around [`crate::Bus`].

use crate::bus::Bus;
use crate::config::BusConfig;
use crate::python::exceptions::mmbus_err;
use crate::python::subscription::{PySubscription, PyTopicStats};
use pyo3::prelude::*;
use std::time::Duration;

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
    ///
    /// Releases the GIL: the wakeup-broadcast step can block briefly if a
    /// subscriber's wakeup-channel buffer is full (macOS socket SO_SNDBUF
    /// or Linux eventfd at U64::MAX).  Holding the GIL across that
    /// would deadlock against a Python subscriber thread trying to drain.
    fn publish(&mut self, py: Python<'_>, topic: &str, data: &[u8]) -> PyResult<()> {
        py.allow_threads(|| self.inner.publish(topic, data))
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
            self.inner.subscribe_with_history_timeout(topic, n_messages_back, timeout)
        })
        .map(PySubscription::new)
        .map_err(mmbus_err)
    }

    /// Subscribe starting at an explicit ``cursor`` value.  Raises
    /// :exc:`OSError` (Error::CursorTooOld) if the cursor is older than
    /// the oldest in-ring slot at connect time.  Releases the GIL.
    #[pyo3(signature = (topic, cursor, timeout_secs=30.0))]
    fn subscribe_from(
        &self,
        py: Python<'_>,
        topic: &str,
        cursor: u64,
        timeout_secs: f64,
    ) -> PyResult<PySubscription> {
        let timeout = Duration::from_secs_f64(timeout_secs);
        py.allow_threads(|| {
            self.inner.subscribe_from_timeout(topic, cursor, timeout)
        })
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
