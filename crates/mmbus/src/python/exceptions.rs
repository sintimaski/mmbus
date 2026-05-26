//! Typed Python exception classes + Rust `Error` → `PyErr` mapping.

// pyo3 0.22's create_exception! macro emits `#[cfg(feature = "gil-refs")]`
// gates; that feature isn't ours and never will be — silence the warnings.
#![allow(unexpected_cfgs)]

use crate::error::Error;
use pyo3::prelude::*;

pyo3::create_exception!(mmbus, BusFullError, pyo3::exceptions::PyException);
pyo3::create_exception!(mmbus, MessageTooLargeError, pyo3::exceptions::PyException);
pyo3::create_exception!(mmbus, ConnectTimeoutError, pyo3::exceptions::PyException);
pyo3::create_exception!(
    mmbus,
    TooManySubscribersError,
    pyo3::exceptions::PyException
);
pyo3::create_exception!(mmbus, AlreadyPublishingError, pyo3::exceptions::PyException);
pyo3::create_exception!(mmbus, CursorTooOldError, pyo3::exceptions::PyException);
pyo3::create_exception!(mmbus, WalError, pyo3::exceptions::PyOSError);

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
        Error::CursorTooOld { requested, oldest } => CursorTooOldError::new_err(format!(
            "cursor {requested} is older than the oldest in-ring slot ({oldest})"
        )),
        Error::Io(e) => PyErr::new::<pyo3::exceptions::PyOSError, _>(e.to_string()),
        Error::Wal(w) => match w {
            crate::wal::WalError::CursorTooOld { requested, oldest } => CursorTooOldError::new_err(
                format!("cursor {requested} is older than the oldest WAL slot ({oldest})"),
            ),
            other => WalError::new_err(other.to_string()),
        },
    }
}

/// Register the exception classes on the module so callers can
/// `except mmbus.BusFullError:` etc.
pub(crate) fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("BusFullError", py.get_type_bound::<BusFullError>())?;
    m.add(
        "MessageTooLargeError",
        py.get_type_bound::<MessageTooLargeError>(),
    )?;
    m.add(
        "ConnectTimeoutError",
        py.get_type_bound::<ConnectTimeoutError>(),
    )?;
    m.add(
        "TooManySubscribersError",
        py.get_type_bound::<TooManySubscribersError>(),
    )?;
    m.add(
        "AlreadyPublishingError",
        py.get_type_bound::<AlreadyPublishingError>(),
    )?;
    m.add(
        "CursorTooOldError",
        py.get_type_bound::<CursorTooOldError>(),
    )?;
    m.add("WalError", py.get_type_bound::<WalError>())?;
    Ok(())
}
