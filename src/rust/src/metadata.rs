//! Retrieve array metadata via zarrs.
//!
//! Opens an array at the given store path and returns a named R list
//! with shape, chunk shape, data type, fill value, and zarr format.

use extendr_api::prelude::*;

use zarrs::array::ArrayMetadata;

use crate::array_open;
use crate::dtype_dispatch;
use crate::error::PizzarrError;

/// Open a zarrs array and return its metadata as an R list.
///
/// # Returns
///
/// A named list with:
/// - `shape`: integer vector
/// - `chunks`: integer vector (regular chunk grid only)
/// - `dtype`: character scalar (zarrs data type name, e.g. `"float64"`)
/// - `r_type`: character scalar (`"double"`, `"integer"`, `"logical"`,
///   or `"unsupported"`)
/// - `fill_value_json`: character scalar (JSON representation)
/// - `zarr_format`: integer scalar (2 or 3)
/// - `order`: character scalar (`"C"` or `"F"`)
///
/// # Errors
///
/// Returns an R error if the store cannot be opened, the array does
/// not exist, or the chunk grid is not regular.
pub(crate) fn open_array_metadata(
    store_url: &str,
    array_path: &str,
) -> extendr_api::Result<List> {
    let array = array_open::open_array_at_path(store_url, array_path)?;

    // Shape: &[u64] → Vec<i32>
    let shape: Vec<i32> = array.shape().iter().map(|&s| s as i32).collect();
    let ndim = shape.len();

    // Chunk shape via chunk_grid_shape or query at origin.
    let chunks: Vec<i32> = extract_chunk_shape(&array, ndim, store_url, array_path)?;

    // Data type
    let dt = array.data_type();
    let dtype_str = dt.to_string();
    let r_type = dtype_dispatch::dtype_family(dt)
        .map_or("unsupported", |f| f.r_type_name());

    // Fill value as a hex string of the native-endian bytes.
    // FillValue doesn't impl Serialize; use as_ne_bytes() instead.
    // R already has fill value parsing; typed extraction deferred
    // to zarrs_get_subset.
    let fv_bytes = array.fill_value().as_ne_bytes();
    let fill_value_json = format!("{fv_bytes:?}");

    // Zarr format from metadata variant.
    let zarr_format: i32 = match array.metadata() {
        ArrayMetadata::V2(_) => 2,
        ArrayMetadata::V3(_) => 3,
    };

    // Order: V2 stores "C" or "F"; V3 is always C-order.
    let order = match array.metadata() {
        ArrayMetadata::V2(v2) => {
            // ArrayMetadataV2 has an `order` field.
            // Use serde to extract it since the field may not be public.
            extract_v2_order(v2)
        }
        ArrayMetadata::V3(_) => "C".to_string(),
    };

    Ok(list!(
        shape = shape,
        chunks = chunks,
        dtype = dtype_str,
        r_type = r_type,
        fill_value_json = fill_value_json,
        zarr_format = zarr_format,
        order = order
    ))
}

/// Extract chunk shape from an array.
///
/// Queries the chunk grid at the origin index. Only regular
/// (fixed-size) chunk grids are supported.
fn extract_chunk_shape(
    array: &zarrs::array::Array<dyn zarrs_storage::ReadableWritableListableStorageTraits>,
    ndim: usize,
    store_url: &str,
    array_path: &str,
) -> std::result::Result<Vec<i32>, PizzarrError> {
    let origin: Vec<u64> = vec![0; ndim];
    let chunk_shape = array
        .chunk_grid()
        .chunk_shape(&origin)
        .map_err(|e| PizzarrError::ArrayOpen {
            url: store_url.to_string(),
            path: array_path.to_string(),
            reason: format!("chunk grid error: {e}"),
        })?
        .ok_or_else(|| PizzarrError::ArrayOpen {
            url: store_url.to_string(),
            path: array_path.to_string(),
            reason: "only regular chunk grids are supported".to_string(),
        })?;

    Ok(chunk_shape.iter().map(|&c| c.get() as i32).collect())
}

/// Extract the memory order from V2 metadata.
///
/// Falls back to `"C"` if the order field cannot be determined.
fn extract_v2_order(v2: &zarrs::array::ArrayMetadataV2) -> String {
    // Serialize to JSON and extract the "order" field.
    // This avoids depending on the struct field being public.
    if let Ok(json) = serde_json::to_value(v2) {
        if let Some(order) = json.get("order").and_then(serde_json::Value::as_str) {
            return order.to_string();
        }
    }
    "C".to_string()
}
