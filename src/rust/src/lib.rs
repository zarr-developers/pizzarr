//! pizzarr: extendr bridge to the zarrs Rust crate.
//!
//! Exposes zarrs chunk I/O, codec pipeline, and store abstraction
//! to R via a small set of `#[extendr]`-annotated functions.

// The extendr proc macros generate internal functions that trigger
// missing_docs warnings. Allow at module level since we cannot
// annotate generated code.
#![allow(missing_docs)]

use extendr_api::prelude::*;

mod array_open;
mod create;
mod dtype_dispatch;
mod error;
mod http_config;
mod info;
mod metadata;
mod retrieve;
#[cfg(feature = "full")]
mod runtime;
mod store;
mod store_cache;
mod store_open;
mod transpose;

/// Return compiled zarrs feature flags as a character vector.
///
/// Called once at `.onLoad` to populate `.pizzarr_env$zarrs_available`.
/// The feature list is determined at compile time via `cfg!` checks.
/// @export
#[extendr]
fn zarrs_compiled_features() -> Vec<String> {
    compiled_features()
}

/// Shared implementation for [`zarrs_compiled_features`].
///
/// Also called by [`info::runtime_info`].
pub(crate) fn compiled_features() -> Vec<String> {
    let mut features = Vec::new();

    // The zarrs crate is always present when this code compiles.
    features.push("zarrs".into());

    // Store backends
    #[cfg(feature = "filesystem")]
    features.push("filesystem".into());

    #[cfg(feature = "http_sync")]
    features.push("http_sync".into());

    // Codec features are set via zarrs Cargo features.
    #[cfg(feature = "gzip")]
    features.push("gzip".into());

    #[cfg(feature = "blosc")]
    features.push("blosc".into());

    #[cfg(feature = "zstd")]
    features.push("zstd".into());

    #[cfg(feature = "sharding")]
    features.push("sharding".into());

    #[cfg(feature = "full")]
    features.push("object_store".into());

    #[cfg(feature = "s3")]
    features.push("s3".into());

    #[cfg(feature = "gcs")]
    features.push("gcs".into());

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

/// Open a zarrs array and return its metadata as an R list.
///
/// Returns a named list with `shape`, `chunks`, `dtype`, `r_type`,
/// `fill_value_json`, `zarr_format`, and `order`.
///
/// @param store_url Filesystem path or URL to the store root.
/// @param array_path Path to the array within the store.
/// @export
#[extendr]
fn zarrs_open_array_metadata(store_url: &str, array_path: &str) -> extendr_api::Result<List> {
    metadata::open_array_metadata(store_url, array_path)
}

/// Return runtime information about the zarrs backend.
///
/// Returns a named list with `codec_concurrent_target`,
/// `store_cache_entries`, and `compiled_features`.
/// @export
#[extendr]
fn zarrs_runtime_info() -> List {
    info::runtime_info()
}

/// Set the zarrs codec concurrent target.
///
/// Controls the number of concurrent codec operations zarrs uses
/// within a single array operation.
///
/// @param n Positive integer.
/// @export
#[extendr]
fn zarrs_set_codec_concurrent_target(n: i32) -> extendr_api::Result<()> {
    info::set_codec_concurrent_target(n)
}

/// Set the rayon thread pool size.
///
/// Initialises the rayon global thread pool with `n` threads. The pool
/// can only be initialised once per process; returns TRUE on success,
/// FALSE if the pool was already initialised.
///
/// @param n Positive integer.
/// @export
#[extendr]
fn zarrs_set_nthreads(n: i32) -> extendr_api::Result<bool> {
    info::set_nthreads(n)
}

/// Set whether new HTTP stores use batched range requests.
///
/// Controls multipart range request behaviour for HTTP stores created
/// after this call. Existing cached stores are not affected.
///
/// @param enable Logical scalar.
/// @export
#[extendr]
fn zarrs_set_http_batch_range_requests(enable: bool) {
    http_config::set_http_batch_range_requests(enable);
}

/// Get a contiguous subset of an array.
///
/// Returns a named list with `data` (numeric, integer, or logical vector)
/// and `shape` (integer vector). Ranges are 0-based, exclusive stop.
///
/// @param store_url Filesystem path or URL to the store root.
/// @param array_path Path to the array within the store.
/// @param ranges R list of length-2 integer vectors `c(start, stop)`.
/// @param concurrent_target Optional codec concurrency override.
/// @export
#[extendr]
fn zarrs_get_subset(
    store_url: &str,
    array_path: &str,
    ranges: List,
    concurrent_target: Nullable<i32>,
) -> extendr_api::Result<List> {
    retrieve::retrieve_subset(store_url, array_path, ranges, concurrent_target)
}

/// Set a contiguous subset of an array from R data.
///
/// Returns `true` on success. Ranges are 0-based, exclusive stop.
/// Data must be a flat vector in C-order (row-major).
///
/// @param store_url Filesystem path or URL to the store root.
/// @param array_path Path to the array within the store.
/// @param ranges R list of length-2 integer vectors `c(start, stop)`.
/// @param data R vector (numeric, integer, or logical).
/// @param concurrent_target Optional codec concurrency override.
/// @export
#[extendr]
fn zarrs_set_subset(
    store_url: &str,
    array_path: &str,
    ranges: List,
    data: Robj,
    concurrent_target: Nullable<i32>,
) -> extendr_api::Result<bool> {
    store::store_subset(store_url, array_path, ranges, data, concurrent_target)
}

/// Create a new zarr array and write its metadata to the store.
///
/// Returns the same metadata list as `zarrs_open_array_metadata`.
///
/// @param store_url Filesystem path to the store root.
/// @param array_path Path to the array within the store.
/// @param shape Integer vector of array dimensions.
/// @param chunks Integer vector of chunk dimensions.
/// @param dtype V3-style data type name (e.g., "float64", "int32").
/// @param codec_preset Compression preset: "none", "gzip", "blosc", or "zstd".
/// @param fill_value Scalar fill value (numeric, integer, logical, or NA).
/// @param attributes_json JSON string of array attributes.
/// @param zarr_format Integer: 2 for V2, 3 for V3.
/// @export
#[extendr]
fn zarrs_create_array(
    store_url: &str,
    array_path: &str,
    shape: &[i32],
    chunks: &[i32],
    dtype: &str,
    codec_preset: &str,
    fill_value: Robj,
    attributes_json: &str,
    zarr_format: i32,
) -> extendr_api::Result<List> {
    create::create_array(
        store_url,
        array_path,
        shape,
        chunks,
        dtype,
        codec_preset,
        &fill_value,
        attributes_json,
        zarr_format,
    )
    .map_err(extendr_api::Error::from)
}

/// Convenience wrapper around `ReadableStorageTraits::get`.
///
/// Extracts the readable storage from the cache entry and reads the key.
/// Maps the zarrs `StorageError` into a [`error::PizzarrError::ArrayOpen`]
/// with the given URL and path context.
fn store_get(
    store: &store_cache::StorageEntry,
    key: &zarrs_storage::StoreKey,
    store_url: &str,
    path: &str,
) -> std::result::Result<zarrs_storage::MaybeBytes, error::PizzarrError> {
    use zarrs_storage::ReadableStorageTraits;
    let readable = store.as_readable();
    readable.get(key).map_err(|e| error::PizzarrError::ArrayOpen {
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
    fn zarrs_open_array_metadata;
    fn zarrs_runtime_info;
    fn zarrs_set_codec_concurrent_target;
    fn zarrs_set_nthreads;
    fn zarrs_set_http_batch_range_requests;
    fn zarrs_get_subset;
    fn zarrs_set_subset;
    fn zarrs_create_array;
}
