//! pizzarr: extendr bridge to the zarrs Rust crate.
//!
//! Exposes zarrs chunk I/O, codec pipeline, and store abstraction
//! to R via a small set of `#[extendr]`-annotated functions.

// The extendr proc macros generate internal functions that trigger
// missing_docs warnings. Allow at module level since we cannot
// annotate generated code.
#![allow(missing_docs)]

use extendr_api::prelude::*;

mod error;
mod store_cache;
mod store_open;

/// Return compiled zarrs feature flags as a character vector.
///
/// Called once at `.onLoad` to populate `.pizzarr_env$zarrs_available`.
/// The feature list is determined at compile time via `cfg!` checks.
/// @export
#[extendr]
fn zarrs_compiled_features() -> Vec<String> {
    let mut features = Vec::new();

    // The zarrs crate is always present when this code compiles.
    features.push("zarrs".into());

    // Store backends
    #[cfg(feature = "filesystem")]
    features.push("filesystem".into());

    // Codec features are set via zarrs Cargo features.
    #[cfg(feature = "gzip")]
    features.push("gzip".into());

    #[cfg(feature = "blosc")]
    features.push("blosc".into());

    #[cfg(feature = "zstd")]
    features.push("zstd".into());

    #[cfg(feature = "sharding")]
    features.push("sharding".into());

    features
}

/// Check whether a node exists at the given path in a zarrs store.
///
/// Open (or reuse) the store at `store_url`, then probe for V3 and V2
/// metadata keys at `path`.
///
/// # Arguments
///
/// * `store_url` - Filesystem path or URL to the store root.
/// * `path` - Path within the store (e.g. `"group1/array1"` or `""` for root).
///
/// # Errors
///
/// Returns an R error if the store cannot be opened or a storage I/O
/// error occurs while probing keys.
/// @export
#[extendr]
fn zarrs_node_exists(store_url: &str, path: &str) -> extendr_api::Result<List> {
    let store = store_open::open_store(store_url)?;

    // Build NodePath. zarrs requires paths to start with "/".
    let node_path = if path.is_empty() || path == "/" {
        zarrs::node::NodePath::root()
    } else {
        let prefixed = if path.starts_with('/') {
            path.to_string()
        } else {
            format!("/{path}")
        };
        zarrs::node::NodePath::new(&prefixed).map_err(|e| error::PizzarrError::ArrayOpen {
            url: store_url.to_string(),
            path: path.to_string(),
            reason: e.to_string(),
        })?
    };

    // Probe V3 metadata (zarr.json).
    let key_v3 = zarrs::node::meta_key_v3(&node_path);
    if let Some(bytes) = store_get(&store, &key_v3, store_url, path)? {
        if let Ok(meta) = serde_json::from_slice::<serde_json::Value>(&bytes) {
            let node_type = meta
                .get("node_type")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown");
            return Ok(list!(
                exists = true,
                node_type = node_type,
                zarr_format = 3_i32
            ));
        }
    }

    // Probe V2 array (.zarray).
    let key_v2_arr = zarrs::node::meta_key_v2_array(&node_path);
    if store_get(&store, &key_v2_arr, store_url, path)?.is_some() {
        return Ok(list!(
            exists = true,
            node_type = "array",
            zarr_format = 2_i32
        ));
    }

    // Probe V2 group (.zgroup).
    let key_v2_grp = zarrs::node::meta_key_v2_group(&node_path);
    if store_get(&store, &key_v2_grp, store_url, path)?.is_some() {
        return Ok(list!(
            exists = true,
            node_type = "group",
            zarr_format = 2_i32
        ));
    }

    // Nothing found.
    Ok(list!(exists = false, node_type = "none", zarr_format = ()))
}

/// Close (remove) a cached store handle.
///
/// Returns `TRUE` if the store was in the cache and was removed,
/// `FALSE` if it was not cached.
/// @export
#[extendr]
fn zarrs_close_store(store_url: &str) -> bool {
    store_cache::remove(store_url)
}

/// Convenience wrapper around `ReadableStorageTraits::get`.
///
/// Maps the zarrs `StorageError` into a [`error::PizzarrError::ArrayOpen`]
/// with the given URL and path context.
fn store_get(
    store: &store_cache::StorageEntry,
    key: &zarrs_storage::StoreKey,
    store_url: &str,
    path: &str,
) -> std::result::Result<zarrs_storage::MaybeBytes, error::PizzarrError> {
    use zarrs_storage::ReadableStorageTraits;
    store.get(key).map_err(|e| error::PizzarrError::ArrayOpen {
        url: store_url.to_string(),
        path: path.to_string(),
        reason: e.to_string(),
    })
}

// Macro to generate exports.
// This ensures exported functions are registered with R.
// See corresponding C code in `entrypoint.c`.
extendr_module! {
    mod pizzarr;
    fn zarrs_compiled_features;
    fn zarrs_node_exists;
    fn zarrs_close_store;
}
