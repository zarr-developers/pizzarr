//! Process-global store handle cache.
//!
//! Stores are expensive to open. This module provides a singleton
//! `HashMap` that maps normalized store URLs to opened zarrs stores.
//! Stores persist until explicitly removed via [`remove`].

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use parking_lot::RwLock;
use zarrs_storage::ReadableWritableListableStorage;

use crate::error::PizzarrError;

/// A cached store entry.
///
/// `FilesystemStore` implements all three trait families
/// (`Readable`, `Writable`, `Listable`), so the widest trait object
/// works for Phase 1. When read-only backends arrive (Phase 4),
/// this will become a tagged enum.
pub(crate) type StorageEntry = ReadableWritableListableStorage;

/// Process-global store cache.
static STORE_CACHE: LazyLock<RwLock<HashMap<String, StorageEntry>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

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
/// of the `Arc`. Otherwise calls `factory` to create a new store and
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

    // Fast path: read lock.
    {
        let cache = STORE_CACHE.read();
        if let Some(entry) = cache.get(&key) {
            return Ok(Arc::clone(entry));
        }
    }

    // Slow path: write lock + double-check.
    let mut cache = STORE_CACHE.write();
    if let Some(entry) = cache.get(&key) {
        return Ok(Arc::clone(entry));
    }

    let store = factory(&key)?;
    cache.insert(key, Arc::clone(&store));
    Ok(store)
}

/// Remove a store from the cache.
///
/// Returns `true` if the entry existed and was removed.
pub(crate) fn remove(url: &str) -> bool {
    let key = normalize_store_url(url);
    let mut cache = STORE_CACHE.write();
    cache.remove(&key).is_some()
}

/// Return the number of entries in the store cache.
///
/// Used by `zarrs_runtime_info` (Phase 2).
#[must_use]
#[allow(dead_code)]
pub(crate) fn entry_count() -> usize {
    let cache = STORE_CACHE.read();
    cache.len()
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
