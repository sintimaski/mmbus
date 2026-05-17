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

    /// Remove all on-disk state for ``topic``.  Raises
    /// :exc:`AlreadyPublishingError` if a publisher is active anywhere
    /// (including this process).  For test setup / dev tooling only.
    fn clean_topic(&mut self, topic: &str) -> PyResult<()> {
        self.inner.clean_topic(topic).map_err(mmbus_err)
    }
}
