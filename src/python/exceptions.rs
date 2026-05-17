//! Typed Python exception classes + Rust `Error` → `PyErr` mapping.

use crate::error::Error;
use pyo3::prelude::*;

pyo3::create_exception!(mmbus, BusFullError, pyo3::exceptions::PyException);
pyo3::create_exception!(mmbus, MessageTooLargeError, pyo3::exceptions::PyException);
pyo3::create_exception!(mmbus, ConnectTimeoutError, pyo3::exceptions::PyException);
pyo3::create_exception!(mmbus, TooManySubscribersError, pyo3::exceptions::PyException);
pyo3::create_exception!(mmbus, AlreadyPublishingError, pyo3::exceptions::PyException);

/// Convert a Rust [`Error`] into the corresponding Python exception.
pub(crate) fn mmbus_err(e: Error) -> PyErr {
    match e {
        Error::Full => BusFullError::new_err("ring buffer full"),
        Error::TooLarge { size, max } => MessageTooLargeError::new_err(format!(
            "message is {size} bytes; slot_size is {max} bytes"
        )),
        Error::Timeout(topic) => {
            ConnectTimeoutError::new_err(format!("timed out waiting for '{topic}'"))
        }
        Error::TooManySubscribers(limit) => {
            TooManySubscribersError::new_err(format!("subscriber limit for this topic is {limit}"))
        }
        Error::AlreadyPublishing(topic) => AlreadyPublishingError::new_err(format!(
            "a publisher is already active for topic '{topic}'"
        )),
        Error::Io(e) => PyErr::new::<pyo3::exceptions::PyOSError, _>(e.to_string()),
    }
}

/// Register the exception classes on the module so callers can
/// `except mmbus.BusFullError:` etc.
pub(crate) fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("BusFullError", py.get_type_bound::<BusFullError>())?;
    m.add("MessageTooLargeError", py.get_type_bound::<MessageTooLargeError>())?;
    m.add("ConnectTimeoutError", py.get_type_bound::<ConnectTimeoutError>())?;
    m.add("TooManySubscribersError", py.get_type_bound::<TooManySubscribersError>())?;
    m.add("AlreadyPublishingError", py.get_type_bound::<AlreadyPublishingError>())?;
    Ok(())
}
