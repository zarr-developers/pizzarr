//! Create zarr arrays via zarrs.
//!
//! Implements `zarrs_create_array`: builds array metadata from R
//! parameters, writes it to the store via zarrs, and returns the
//! metadata as an R list (same shape as `zarrs_open_array_metadata`).

use extendr_api::prelude::*;
use zarrs::array::{Array, ArrayMetadata, ArrayMetadataV2, ArrayMetadataV3};
use zarrs_storage::ReadableWritableListableStorageTraits;

use crate::dtype_dispatch;
use crate::error::PizzarrError;
use crate::store_open;

// ---------------------------------------------------------------------------
// Dtype mapping
// ---------------------------------------------------------------------------

/// Map a V3-style dtype name to the V2 numpy-style dtype string.
///
/// Returns the little-endian form for multi-byte types and the
/// `|`-prefixed form for single-byte types, matching zarr-python
/// conventions.
fn dtype_to_v2_string(dtype: &str) -> std::result::Result<&'static str, PizzarrError> {
    match dtype {
        "float64" => Ok("<f8"),
        "float32" => Ok("<f4"),
        "int32" => Ok("<i4"),
        "int16" => Ok("<i2"),
        "int8" => Ok("|i1"),
        "uint8" => Ok("|u1"),
        "uint16" => Ok("<u2"),
        "uint32" => Ok("<u4"),
        "int64" => Ok("<i8"),
        "uint64" => Ok("<u8"),
        "bool" => Ok("|b1"),
        _ => Err(PizzarrError::DTypeUnsupported {
            dtype: dtype.to_string(),
        }),
    }
}

/// Validate that a dtype string is supported by the zarrs bridge.
fn validate_dtype(dtype: &str) -> std::result::Result<(), PizzarrError> {
    // If we can map to V2, it's a supported type.
    dtype_to_v2_string(dtype).map(|_| ())
}

// ---------------------------------------------------------------------------
// Fill value helpers
// ---------------------------------------------------------------------------

/// Convert an R fill value to a `serde_json::Value` for V2 metadata.
///
/// V2 `.zarray` fill values are JSON scalars: `0`, `0.0`, `null`,
/// `"NaN"`, `"Infinity"`, `"-Infinity"`.
fn fill_value_to_json(robj: &Robj, dtype: &str) -> serde_json::Value {
    // NA → type-appropriate default
    if robj.is_na() {
        return match dtype {
            "float64" | "float32" => serde_json::json!("NaN"),
            "bool" => serde_json::json!(false),
            _ => serde_json::json!(0),
        };
    }

    // Bool dtype: zarrs rejects integer fill values (0/1) for bool.
    // Must produce JSON true/false.
    if dtype == "bool" {
        if let Some(slice) = robj.as_logical_slice() {
            if let Some(v) = slice.first() {
                return serde_json::json!(v.is_true());
            }
        }
        if let Some(slice) = robj.as_real_slice() {
            if let Some(&v) = slice.first() {
                return serde_json::json!(v != 0.0);
            }
        }
        if let Some(slice) = robj.as_integer_slice() {
            if let Some(&v) = slice.first() {
                return serde_json::json!(v != 0);
            }
        }
        return serde_json::json!(false);
    }

    // Try double first (R stores most numerics as double)
    if let Some(slice) = robj.as_real_slice() {
        if let Some(&v) = slice.first() {
            if v.is_nan() {
                return serde_json::json!("NaN");
            }
            if v.is_infinite() {
                return if v > 0.0 {
                    serde_json::json!("Infinity")
                } else {
                    serde_json::json!("-Infinity")
                };
            }
            // For integer-stored types, emit an integer JSON value
            if is_integer_dtype(dtype) && v.fract() == 0.0 {
                return serde_json::json!(v as i64);
            }
            return serde_json::json!(v);
        }
    }

    // Integer
    if let Some(slice) = robj.as_integer_slice() {
        if let Some(&v) = slice.first() {
            return serde_json::json!(v);
        }
    }

    // Logical
    if let Some(slice) = robj.as_logical_slice() {
        if let Some(v) = slice.first() {
            return serde_json::json!(v.is_true());
        }
    }

    // Fallback
    serde_json::json!(0)
}

/// Check if a dtype string is an integer type.
fn is_integer_dtype(dtype: &str) -> bool {
    matches!(
        dtype,
        "int8" | "int16" | "int32" | "int64" | "uint8" | "uint16" | "uint32" | "uint64" | "bool"
    )
}

// ---------------------------------------------------------------------------
// Codec preset mapping
// ---------------------------------------------------------------------------

/// Build V2 compressor JSON from a preset name.
///
/// Returns `Ok(None)` for "none" (no compression).
fn v2_compressor_json(
    preset: &str,
    store_url: &str,
    array_path: &str,
) -> std::result::Result<Option<serde_json::Value>, PizzarrError> {
    // zarrs treats "zlib" and "gzip" as separate codecs. zarr-python V2
    // uses "zlib" as the compressor id, but zarrs rejects "zlib". Use
    // "gzip" here; zarrs reads both on input.
    match preset {
        "none" => Ok(None),
        "gzip" => Ok(Some(serde_json::json!({
            "id": "gzip",
            "level": 1
        }))),
        "blosc" => Ok(Some(serde_json::json!({
            "id": "blosc",
            "cname": "lz4",
            "clevel": 5,
            "shuffle": 1,
            "blocksize": 0
        }))),
        "zstd" => Err(PizzarrError::ArrayCreate {
            url: store_url.to_string(),
            path: array_path.to_string(),
            reason: "zstd codec preset requires zarr_format = 3".to_string(),
        }),
        _ => Err(PizzarrError::ArrayCreate {
            url: store_url.to_string(),
            path: array_path.to_string(),
            reason: format!(
                "unknown codec_preset '{preset}'; supported: none, gzip, blosc, zstd"
            ),
        }),
    }
}

/// Build V3 codec pipeline JSON from a preset name.
///
/// Returns a JSON array of codec entries. The `bytes` (endian) codec
/// is always included as the array-to-bytes codec.
fn v3_codecs_json(
    preset: &str,
    dtype: &str,
    store_url: &str,
    array_path: &str,
) -> std::result::Result<serde_json::Value, PizzarrError> {
    // Endian configuration for the bytes codec.
    // Single-byte types and bool don't need endian specification.
    let bytes_codec = if matches!(dtype, "int8" | "uint8" | "bool") {
        serde_json::json!({"name": "bytes", "configuration": {"endian": "little"}})
    } else {
        serde_json::json!({"name": "bytes", "configuration": {"endian": "little"}})
    };

    let compression_codec = match preset {
        "none" => None,
        "gzip" => {
            check_feature("gzip", store_url, array_path)?;
            Some(serde_json::json!({"name": "gzip", "configuration": {"level": 1}}))
        }
        "blosc" => {
            check_feature("blosc", store_url, array_path)?;
            Some(serde_json::json!({
                "name": "blosc",
                "configuration": {
                    "cname": "lz4",
                    "clevel": 5,
                    "shuffle": "shuffle",
                    "typesize": dtype_byte_size(dtype),
                    "blocksize": 0
                }
            }))
        }
        "zstd" => {
            check_feature("zstd", store_url, array_path)?;
            Some(serde_json::json!({
                "name": "zstd",
                "configuration": {"level": 3, "checksum": false}
            }))
        }
        _ => {
            return Err(PizzarrError::ArrayCreate {
                url: store_url.to_string(),
                path: array_path.to_string(),
                reason: format!(
                    "unknown codec_preset '{preset}'; supported: none, gzip, blosc, zstd"
                ),
            })
        }
    };

    let mut codecs = vec![bytes_codec];
    if let Some(cc) = compression_codec {
        codecs.push(cc);
    }
    Ok(serde_json::Value::Array(codecs))
}

/// Check if a codec feature is compiled in.
fn check_feature(
    feature: &str,
    store_url: &str,
    array_path: &str,
) -> std::result::Result<(), PizzarrError> {
    let available = match feature {
        "gzip" => cfg!(feature = "gzip"),
        "blosc" => cfg!(feature = "blosc"),
        "zstd" => cfg!(feature = "zstd"),
        _ => false,
    };
    if available {
        Ok(())
    } else {
        Err(PizzarrError::ArrayCreate {
            url: store_url.to_string(),
            path: array_path.to_string(),
            reason: format!("codec '{feature}' not compiled; install from r-universe"),
        })
    }
}

/// Return the byte size for a dtype string.
fn dtype_byte_size(dtype: &str) -> usize {
    match dtype {
        "bool" | "int8" | "uint8" => 1,
        "int16" | "uint16" => 2,
        "int32" | "uint32" | "float32" => 4,
        "int64" | "uint64" | "float64" => 8,
        _ => 1,
    }
}

// ---------------------------------------------------------------------------
// V2 array creation
// ---------------------------------------------------------------------------

/// Create a V2 array by building `.zarray` JSON metadata.
///
/// Metadata is constructed as `serde_json::Value` and deserialized into
/// `ArrayMetadataV2` rather than using `ArrayBuilder`. This decouples
/// from the builder's exact API shape (which varies across zarrs
/// versions) and works identically for V2 and V3.
fn create_array_v2(
    store: zarrs_storage::ReadableWritableListableStorage,
    store_url: &str,
    array_path: &str,
    shape: &[i32],
    chunks: &[i32],
    dtype: &str,
    codec_preset: &str,
    fill_value: &Robj,
) -> std::result::Result<Array<dyn ReadableWritableListableStorageTraits>, PizzarrError> {
    let dtype_v2 = dtype_to_v2_string(dtype)?;
    let compressor = v2_compressor_json(codec_preset, store_url, array_path)?;
    let fv_json = fill_value_to_json(fill_value, dtype);

    let shape_u64: Vec<u64> = shape.iter().map(|&s| s as u64).collect();
    let chunks_u64: Vec<u64> = chunks.iter().map(|&c| c as u64).collect();

    let meta_json = serde_json::json!({
        "zarr_format": 2,
        "shape": shape_u64,
        "chunks": chunks_u64,
        "dtype": dtype_v2,
        "compressor": compressor,
        "fill_value": fv_json,
        "order": "C",
        "filters": null,
        "dimension_separator": "/"
    });

    let v2_meta: ArrayMetadataV2 =
        serde_json::from_value(meta_json).map_err(|e| PizzarrError::ArrayCreate {
            url: store_url.to_string(),
            path: array_path.to_string(),
            reason: format!("V2 metadata construction failed: {e}"),
        })?;

    let node_path = normalize_node_path(array_path);
    let array =
        Array::new_with_metadata(store, &node_path, ArrayMetadata::V2(v2_meta)).map_err(|e| {
            PizzarrError::ArrayCreate {
                url: store_url.to_string(),
                path: array_path.to_string(),
                reason: format!("Array::new_with_metadata failed: {e}"),
            }
        })?;

    array
        .store_metadata()
        .map_err(|e| PizzarrError::ArrayCreate {
            url: store_url.to_string(),
            path: array_path.to_string(),
            reason: format!("store_metadata failed: {e}"),
        })?;

    Ok(array)
}

// ---------------------------------------------------------------------------
// V3 array creation
// ---------------------------------------------------------------------------

/// Create a V3 array by building `zarr.json` metadata.
fn create_array_v3(
    store: zarrs_storage::ReadableWritableListableStorage,
    store_url: &str,
    array_path: &str,
    shape: &[i32],
    chunks: &[i32],
    dtype: &str,
    codec_preset: &str,
    fill_value: &Robj,
    attributes_json: &str,
) -> std::result::Result<Array<dyn ReadableWritableListableStorageTraits>, PizzarrError> {
    let shape_u64: Vec<u64> = shape.iter().map(|&s| s as u64).collect();
    let chunks_u64: Vec<u64> = chunks.iter().map(|&c| c as u64).collect();
    let fv_json = fill_value_to_json(fill_value, dtype);
    let codecs = v3_codecs_json(codec_preset, dtype, store_url, array_path)?;

    let attributes: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(attributes_json).unwrap_or_default();

    let meta_json = serde_json::json!({
        "zarr_format": 3,
        "node_type": "array",
        "shape": shape_u64,
        "data_type": dtype,
        "chunk_grid": {
            "name": "regular",
            "configuration": {"chunk_shape": chunks_u64}
        },
        "chunk_key_encoding": {
            "name": "default",
            "configuration": {"separator": "/"}
        },
        "codecs": codecs,
        "fill_value": fv_json,
        "attributes": attributes
    });

    let v3_meta: ArrayMetadataV3 =
        serde_json::from_value(meta_json).map_err(|e| PizzarrError::ArrayCreate {
            url: store_url.to_string(),
            path: array_path.to_string(),
            reason: format!("V3 metadata construction failed: {e}"),
        })?;

    let node_path = normalize_node_path(array_path);
    let array =
        Array::new_with_metadata(store, &node_path, ArrayMetadata::V3(v3_meta)).map_err(|e| {
            PizzarrError::ArrayCreate {
                url: store_url.to_string(),
                path: array_path.to_string(),
                reason: format!("Array::new_with_metadata failed: {e}"),
            }
        })?;

    array
        .store_metadata()
        .map_err(|e| PizzarrError::ArrayCreate {
            url: store_url.to_string(),
            path: array_path.to_string(),
            reason: format!("store_metadata failed: {e}"),
        })?;

    Ok(array)
}

// ---------------------------------------------------------------------------
// Path normalization
// ---------------------------------------------------------------------------

/// Normalize array path for zarrs (must start with "/").
fn normalize_node_path(array_path: &str) -> String {
    if array_path.is_empty() || array_path == "/" {
        "/".to_string()
    } else if array_path.starts_with('/') {
        array_path.to_string()
    } else {
        format!("/{array_path}")
    }
}

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------

/// Create a zarr array and write its metadata to the store.
///
/// Returns the same metadata list as `zarrs_open_array_metadata`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn create_array(
    store_url: &str,
    array_path: &str,
    shape: &[i32],
    chunks: &[i32],
    dtype: &str,
    codec_preset: &str,
    fill_value: &Robj,
    attributes_json: &str,
    zarr_format: i32,
) -> std::result::Result<List, PizzarrError> {
    // Validate inputs.
    validate_dtype(dtype)?;

    if shape.is_empty() {
        return Err(PizzarrError::ArrayCreate {
            url: store_url.to_string(),
            path: array_path.to_string(),
            reason: "shape must have at least one dimension".to_string(),
        });
    }
    if shape.len() != chunks.len() {
        return Err(PizzarrError::ArrayCreate {
            url: store_url.to_string(),
            path: array_path.to_string(),
            reason: format!(
                "shape length ({}) must equal chunks length ({})",
                shape.len(),
                chunks.len()
            ),
        });
    }

    // Open the store (must be read-write).
    let entry = store_open::open_store(store_url)?;
    let store = entry.as_readable_writable_listable(store_url)?;

    // Dispatch on zarr format.
    let array = if zarr_format == 3 {
        create_array_v3(
            store,
            store_url,
            array_path,
            shape,
            chunks,
            dtype,
            codec_preset,
            fill_value,
            attributes_json,
        )?
    } else {
        create_array_v2(
            store,
            store_url,
            array_path,
            shape,
            chunks,
            dtype,
            codec_preset,
            fill_value,
        )?
    };

    // Build the return list matching zarrs_open_array_metadata format.
    let shape_out: Vec<i32> = array.shape().iter().map(|&s| s as i32).collect();
    let ndim = shape_out.len();

    let chunks_out: Vec<i32> = {
        let origin: Vec<u64> = vec![0; ndim];
        array
            .chunk_grid()
            .chunk_shape(&origin)
            .map_err(|e| PizzarrError::ArrayCreate {
                url: store_url.to_string(),
                path: array_path.to_string(),
                reason: format!("chunk grid error: {e}"),
            })?
            .ok_or_else(|| PizzarrError::ArrayCreate {
                url: store_url.to_string(),
                path: array_path.to_string(),
                reason: "only regular chunk grids are supported".to_string(),
            })?
            .iter()
            .map(|&c| c.get() as i32)
            .collect()
    };

    let dt = array.data_type();
    // DataType::to_string() returns "float64 / <f8" etc. Extract just
    // the V3 name (before " / ") for consistency with dtype_dispatch.
    let dtype_full = dt.to_string();
    let dtype_str = dtype_full
        .split(" / ")
        .next()
        .unwrap_or(&dtype_full)
        .to_string();
    let r_type = dtype_dispatch::dtype_family(dt).map_or("unsupported", |f| f.r_type_name());

    let fv_bytes = array.fill_value().as_ne_bytes();
    let fill_value_json = format!("{fv_bytes:?}");

    let zarr_format_out: i32 = match array.metadata() {
        ArrayMetadata::V2(_) => 2,
        ArrayMetadata::V3(_) => 3,
    };

    let order = match array.metadata() {
        ArrayMetadata::V2(v2) => {
            if let Ok(json) = serde_json::to_value(v2) {
                json.get("order")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("C")
                    .to_string()
            } else {
                "C".to_string()
            }
        }
        ArrayMetadata::V3(_) => "C".to_string(),
    };

    Ok(list!(
        shape = shape_out,
        chunks = chunks_out,
        dtype = dtype_str,
        r_type = r_type,
        fill_value_json = fill_value_json,
        zarr_format = zarr_format_out,
        order = order
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dtype_to_v2_string() {
        assert_eq!(dtype_to_v2_string("float64").unwrap(), "<f8");
        assert_eq!(dtype_to_v2_string("int32").unwrap(), "<i4");
        assert_eq!(dtype_to_v2_string("bool").unwrap(), "|b1");
        assert_eq!(dtype_to_v2_string("uint8").unwrap(), "|u1");
        assert!(dtype_to_v2_string("string").is_err());
    }

    #[test]
    fn test_is_integer_dtype() {
        assert!(is_integer_dtype("int32"));
        assert!(is_integer_dtype("uint8"));
        assert!(is_integer_dtype("bool"));
        assert!(!is_integer_dtype("float64"));
        assert!(!is_integer_dtype("float32"));
    }

    #[test]
    fn test_dtype_byte_size() {
        assert_eq!(dtype_byte_size("float64"), 8);
        assert_eq!(dtype_byte_size("float32"), 4);
        assert_eq!(dtype_byte_size("int32"), 4);
        assert_eq!(dtype_byte_size("bool"), 1);
        assert_eq!(dtype_byte_size("uint8"), 1);
    }

    #[test]
    fn test_v2_compressor_none() {
        let result = v2_compressor_json("none", "", "").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_v2_compressor_gzip() {
        let result = v2_compressor_json("gzip", "", "").unwrap().unwrap();
        assert_eq!(result["id"], "gzip");
        assert_eq!(result["level"], 1);
    }

    #[test]
    fn test_v2_compressor_zstd_rejected() {
        let result = v2_compressor_json("zstd", "store", "arr");
        assert!(result.is_err());
    }

    #[test]
    fn test_normalize_node_path() {
        assert_eq!(normalize_node_path(""), "/");
        assert_eq!(normalize_node_path("/"), "/");
        assert_eq!(normalize_node_path("foo"), "/foo");
        assert_eq!(normalize_node_path("/foo"), "/foo");
        assert_eq!(normalize_node_path("a/b/c"), "/a/b/c");
    }
}
