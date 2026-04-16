//! Internal helpers for opening zarrs arrays.
//!
//! Provides [`open_array_at_path`] (read-only) and
//! [`open_array_at_path_rw`] (read-write), which resolve the store
//! from the cache and open a zarrs [`Array`] at the given path.

use zarrs::array::Array;
use zarrs_storage::{ReadableStorageTraits, ReadableWritableListableStorageTraits};

use crate::error::PizzarrError;
use crate::store_open;

/// Normalize the array path for zarrs (must start with "/").
fn normalize_path(array_path: &str) -> String {
    if array_path.is_empty() || array_path == "/" {
        "/".to_string()
    } else if array_path.starts_with('/') {
        array_path.to_string()
    } else {
        format!("/{array_path}")
    }
}

/// Open a zarrs array for reading at `array_path` within `store_url`.
///
/// The store is retrieved from the process-global cache (or opened and
/// cached on first use). Works with both filesystem and HTTP stores.
///
/// # Errors
///
/// Returns [`PizzarrError::StoreOpen`] if the store cannot be opened,
/// [`PizzarrError::ArrayOpen`] if the path is invalid or no array
/// metadata is found.
pub(crate) fn open_array_at_path(
    store_url: &str,
    array_path: &str,
) -> Result<Array<dyn ReadableStorageTraits>, PizzarrError> {
    let entry = store_open::open_store(store_url)?;
    let store = entry.as_readable();
    let path_str = normalize_path(array_path);

    Array::open(store, &path_str).map_err(|e| PizzarrError::ArrayOpen {
        url: store_url.to_string(),
        path: array_path.to_string(),
        reason: e.to_string(),
    })
}

/// Open a zarrs array for writing at `array_path` within `store_url`.
///
/// Requires a read-write-listable store (e.g., filesystem). Returns
/// [`PizzarrError::StoreReadOnly`] for read-only stores (e.g., HTTP).
///
/// # Errors
///
/// Returns [`PizzarrError::StoreReadOnly`] if the store is read-only,
/// [`PizzarrError::StoreOpen`] if the store cannot be opened,
/// [`PizzarrError::ArrayOpen`] if the path is invalid or no array
/// metadata is found.
pub(crate) fn open_array_at_path_rw(
    store_url: &str,
    array_path: &str,
) -> Result<Array<dyn ReadableWritableListableStorageTraits>, PizzarrError> {
    let entry = store_open::open_store(store_url)?;
    let store = entry.as_readable_writable_listable(store_url)?;
    let path_str = normalize_path(array_path);

    Array::open(store, &path_str).map_err(|e| PizzarrError::ArrayOpen {
        url: store_url.to_string(),
        path: array_path.to_string(),
        reason: e.to_string(),
    })
}
