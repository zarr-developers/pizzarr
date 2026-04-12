//! Internal helper for opening zarrs arrays.
//!
//! Provides [`open_array_at_path`], which resolves the store from the
//! cache and opens a zarrs [`Array`] at the given path. Used by
//! `zarrs_open_array_metadata` now and `zarrs_retrieve_subset` later.

use zarrs::array::Array;
use zarrs_storage::ReadableWritableListableStorageTraits;

use crate::error::PizzarrError;
use crate::store_open;

/// Open a zarrs array at `array_path` within the store at `store_url`.
///
/// The store is retrieved from the process-global cache (or opened and
/// cached on first use). The returned `Array` borrows from the cached
/// `Arc`, so the store stays alive as long as the array does.
///
/// # Errors
///
/// Returns [`PizzarrError::StoreOpen`] if the store cannot be opened,
/// [`PizzarrError::ArrayOpen`] if the path is invalid or no array
/// metadata is found.
pub(crate) fn open_array_at_path(
    store_url: &str,
    array_path: &str,
) -> Result<Array<dyn ReadableWritableListableStorageTraits>, PizzarrError> {
    let store = store_open::open_store(store_url)?;

    // zarrs requires paths to start with "/".
    let path_str = if array_path.is_empty() || array_path == "/" {
        "/".to_string()
    } else if array_path.starts_with('/') {
        array_path.to_string()
    } else {
        format!("/{array_path}")
    };

    Array::open(store, &path_str).map_err(|e| PizzarrError::ArrayOpen {
        url: store_url.to_string(),
        path: array_path.to_string(),
        reason: e.to_string(),
    })
}
