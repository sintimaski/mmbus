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

#[pymodule]
pub fn _mmbus(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<bus::PyBus>()?;
    m.add_class::<subscription::PySubscription>()?;
    m.add_class::<subscription::PyTopicStats>()?;
    m.add_class::<subscription::PyWalStats>()?;
    exceptions::register(py, m)?;
    Ok(())
}
