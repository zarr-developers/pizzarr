//! Process-global store handle cache.
//!
//! Stores are expensive to open. This module provides a singleton
//! `HashMap` that maps normalized store URLs to opened zarrs stores.
//! Stores persist until explicitly removed via [`remove`].
//!
//! On native targets the cache lives in a `LazyLock<RwLock<...>>`.
//! On Wasm (single-threaded) we use a `thread_local! { RefCell<...> }`
//! to avoid `Send + Sync` bounds that the Wasm target cannot satisfy.

use std::collections::HashMap;
use std::sync::Arc;

use zarrs_storage::{ReadableStorage, ReadableWritableListableStorage};

use crate::error::PizzarrError;

/// A cached store entry.
///
/// Filesystem stores implement all three trait families (Readable,
/// Writable, Listable) and get stored as `ReadWriteList`. HTTP stores
/// are read-only and get stored as `ReadOnly`.
#[derive(Clone)]
pub(crate) enum StorageEntry {
    /// Full read-write-list store (e.g., `FilesystemStore`).
    ReadWriteList(ReadableWritableListableStorage),
    /// Read-only store (e.g., `HTTPStore`).
    ReadOnly(ReadableStorage),
}

impl StorageEntry {
    /// Get a readable storage handle suitable for `Array::open` and
    /// `retrieve_array_subset`.
    ///
    /// Both variants satisfy `ReadableStorageTraits`.
    pub(crate) fn as_readable(&self) -> ReadableStorage {
        match self {
            Self::ReadWriteList(s) => Arc::clone(s) as ReadableStorage,
            Self::ReadOnly(s) => Arc::clone(s),
        }
    }

    /// Get the full read-write-list storage handle, or error if this
    /// is a read-only store.
    ///
    /// Used by the write path, which requires `WritableStorageTraits`.
    pub(crate) fn as_readable_writable_listable(
        &self,
        url: &str,
    ) -> std::result::Result<ReadableWritableListableStorage, PizzarrError> {
        match self {
            Self::ReadWriteList(s) => Ok(Arc::clone(s)),
            Self::ReadOnly(_) => Err(PizzarrError::StoreReadOnly {
                url: url.to_string(),
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// Native: LazyLock + RwLock (thread-safe)
// ---------------------------------------------------------------------------
#[cfg(not(target_family = "wasm"))]
mod inner {
    use super::*;
    use std::sync::LazyLock;
    use parking_lot::RwLock;

    static STORE_CACHE: LazyLock<RwLock<HashMap<String, StorageEntry>>> =
        LazyLock::new(|| RwLock::new(HashMap::new()));

    pub(crate) fn get_or_insert<F>(
        key: &str,
        normalized: &str,
        factory: F,
    ) -> Result<StorageEntry, PizzarrError>
    where
        F: FnOnce(&str) -> Result<StorageEntry, PizzarrError>,
    {
        // Fast path: read lock.
        {
            let cache = STORE_CACHE.read();
            if let Some(entry) = cache.get(normalized) {
                return Ok(entry.clone());
            }
        }

        // Slow path: write lock + double-check.
        let mut cache = STORE_CACHE.write();
        if let Some(entry) = cache.get(normalized) {
            return Ok(entry.clone());
        }

        let store = factory(key)?;
        cache.insert(normalized.to_string(), store.clone());
        Ok(store)
    }

    pub(crate) fn remove(normalized: &str) -> bool {
        let mut cache = STORE_CACHE.write();
        cache.remove(normalized).is_some()
    }

    pub(crate) fn entry_count() -> usize {
        let cache = STORE_CACHE.read();
        cache.len()
    }
}

// ---------------------------------------------------------------------------
// Wasm: thread_local + RefCell (single-threaded, no Send/Sync needed)
// ---------------------------------------------------------------------------
#[cfg(target_family = "wasm")]
mod inner {
    use super::*;
    use std::cell::RefCell;

    thread_local! {
        static STORE_CACHE: RefCell<HashMap<String, StorageEntry>> =
            RefCell::new(HashMap::new());
    }

    pub(crate) fn get_or_insert<F>(
        key: &str,
        normalized: &str,
        factory: F,
    ) -> Result<StorageEntry, PizzarrError>
    where
        F: FnOnce(&str) -> Result<StorageEntry, PizzarrError>,
    {
        STORE_CACHE.with(|cache| {
            let map = cache.borrow();
            if let Some(entry) = map.get(normalized) {
                return Ok(entry.clone());
            }
            drop(map);

            let store = factory(key)?;
            cache.borrow_mut().insert(normalized.to_string(), store.clone());
            Ok(store)
        })
    }

    pub(crate) fn remove(normalized: &str) -> bool {
        STORE_CACHE.with(|cache| cache.borrow_mut().remove(normalized).is_some())
    }

    pub(crate) fn entry_count() -> usize {
        STORE_CACHE.with(|cache| cache.borrow().len())
    }
}

/// Normalize a store URL for use as a cache key.
///
/// Strips trailing slashes. For URLs with a scheme (`://`), lowercases
/// the scheme portion. For bare filesystem paths, attempts to
/// canonicalize to an absolute path (falls back to the trimmed string
/// on error).
pub(crate) fn normalize_store_url(url: &str) -> String {
    let trimmed = url.trim_end_matches('/');

    if let Some(colon_pos) = trimmed.find("://") {
        let (scheme, rest) = trimmed.split_at(colon_pos);
        format!("{}{}", scheme.to_ascii_lowercase(), rest)
    } else {
        // Bare filesystem path — canonicalize.
        match std::path::Path::new(trimmed).canonicalize() {
            Ok(abs) => abs.to_string_lossy().to_string(),
            Err(_) => trimmed.to_string(),
        }
    }
}

/// Get or insert a store into the cache.
///
/// If the normalized URL already has a cached entry, returns a clone
/// of the entry. Otherwise calls `factory` to create a new store and
/// inserts it.
///
/// # Errors
///
/// Returns the factory error if store creation fails.
pub(crate) fn get_or_insert<F>(url: &str, factory: F) -> Result<StorageEntry, PizzarrError>
where
    F: FnOnce(&str) -> Result<StorageEntry, PizzarrError>,
{
    let key = normalize_store_url(url);
    inner::get_or_insert(url, &key, factory)
}

/// Remove a store from the cache.
///
/// Returns `true` if the entry existed and was removed.
pub(crate) fn remove(url: &str) -> bool {
    let key = normalize_store_url(url);
    inner::remove(&key)
}

/// Return the number of entries in the store cache.
///
/// Used by `zarrs_runtime_info`.
#[must_use]
#[allow(dead_code)]
pub(crate) fn entry_count() -> usize {
    inner::entry_count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_url_strips_trailing_slash() {
        assert_eq!(normalize_store_url("/tmp/data/"), "/tmp/data");
        assert_eq!(normalize_store_url("/tmp/data///"), "/tmp/data");
    }

    #[test]
    fn normalize_url_lowercases_scheme() {
        assert_eq!(
            normalize_store_url("HTTP://example.com/data/"),
            "http://example.com/data"
        );
        assert_eq!(
            normalize_store_url("S3://bucket/prefix"),
            "s3://bucket/prefix"
        );
    }
}
