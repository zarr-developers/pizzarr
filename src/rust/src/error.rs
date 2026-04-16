//! Error types for the pizzarr-zarrs bridge.

use thiserror::Error;

/// Errors that can occur in the pizzarr Rust bridge.
///
/// Variant names follow the operation that failed, matching the
/// Python-style error naming convention used in pizzarr's R code.
///
/// Some variants are defined ahead of use (Phases 2–4) and are not
/// yet constructed.
#[derive(Debug, Error)]
#[allow(dead_code)]
pub(crate) enum PizzarrError {
    /// Failed to open or create a store.
    #[error("StoreOpen({url}): {reason}")]
    StoreOpen {
        /// Store URL or path that was requested.
        url: String,
        /// Description of what went wrong.
        reason: String,
    },

    /// Failed to open an array at the given path.
    #[error("ArrayOpen({path} in {url}): {reason}")]
    ArrayOpen {
        /// Store URL or path.
        url: String,
        /// Array path within the store.
        path: String,
        /// Description of what went wrong.
        reason: String,
    },

    /// Failed to retrieve data from an array.
    #[error("Retrieve({path} in {url}): {reason}")]
    Retrieve {
        /// Store URL or path.
        url: String,
        /// Array path within the store.
        path: String,
        /// Description of what went wrong.
        reason: String,
    },

    /// Failed to store data into an array.
    #[error("Store({path} in {url}): {reason}")]
    Store {
        /// Store URL or path.
        url: String,
        /// Array path within the store.
        path: String,
        /// Description of what went wrong.
        reason: String,
    },

    /// Failed to create an array.
    #[error("ArrayCreate({path} in {url}): {reason}")]
    ArrayCreate {
        /// Store URL or path.
        url: String,
        /// Array path within the store.
        path: String,
        /// Description of what went wrong.
        reason: String,
    },

    /// Data type not supported by the zarrs bridge.
    #[error("DTypeUnsupported({dtype}): use R-native path")]
    DTypeUnsupported {
        /// The unsupported data type string.
        dtype: String,
    },

    /// Attempted to write to a read-only store (e.g., HTTP).
    #[error("StoreReadOnly({url}): cannot write to read-only store")]
    StoreReadOnly {
        /// Store URL or path.
        url: String,
    },

    /// A required compiled feature is not available.
    #[error("FeatureNotCompiled({feature}): install from r-universe")]
    FeatureNotCompiled {
        /// The missing feature name.
        feature: String,
    },

    /// An I/O error from the underlying system.
    #[error("Io: {0}")]
    Io(#[from] std::io::Error),
}

impl From<PizzarrError> for extendr_api::Error {
    fn from(err: PizzarrError) -> Self {
        extendr_api::Error::Other(err.to_string())
    }
}
