//! PyO3 bindings.
//!
//! The extension module is compiled as `mmbus._mmbus`; the thin Python wrapper
//! at `python/mmbus/__init__.py` re-exports the public API and adds
//! `AsyncSubscription` for use with asyncio.

mod bus;
mod exceptions;
mod subscription;

use pyo3::prelude::*;

#[pymodule]
pub fn _mmbus(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<bus::PyBus>()?;
    m.add_class::<subscription::PySubscription>()?;
    m.add_class::<subscription::PyTopicStats>()?;
    exceptions::register(py, m)?;
    Ok(())
}
