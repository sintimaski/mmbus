//! PyO3 bindings.
//!
//! The extension module is compiled as `mmbus._mmbus`; the thin Python wrapper
//! at `python/mmbus/__init__.py` re-exports the public API and adds
//! `AsyncSubscription` for use with asyncio.

// pyo3 0.22's `#[pymethods]` macro generates `.into()` calls on PyResult
// errors that are already PyErr.  We can't fix the generated code — silence
// the warning for the whole bindings layer.
#![allow(clippy::useless_conversion)]

mod bus;
mod exceptions;
mod subscription;

use pyo3::prelude::*;

/// Install a global stderr subscriber for mmbus's Rust-side `tracing`
/// events (publisher/subscriber lifecycle, WAL rotation/retention,
/// publisher restart).  See [`crate::init_logging`].
///
/// Filtering: `RUST_LOG` if set (e.g. `RUST_LOG=mmbus=debug`), else the
/// `level` argument, else `"info"`.  Idempotent — returns `True` only on
/// the call that installs the subscriber.
#[pyfunction]
#[pyo3(signature = (level=None))]
fn init_logging(level: Option<&str>) -> bool {
    crate::init_logging(level)
}

#[pymodule]
pub fn _mmbus(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<bus::PyBus>()?;
    m.add_class::<bus::PyTopicPublisher>()?;
    m.add_class::<subscription::PySubscription>()?;
    m.add_class::<subscription::PyTopicStats>()?;
    m.add_class::<subscription::PyWalStats>()?;
    m.add_function(wrap_pyfunction!(init_logging, m)?)?;
    exceptions::register(py, m)?;
    Ok(())
}
