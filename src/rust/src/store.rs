//! Store array subsets via zarrs.
//!
//! Implements the write path: open an array, convert R-side ranges to
//! an `ArraySubset`, dispatch on the stored data type, narrow from R
//! types to stored types, and write the data.

use std::ops::Range;

use extendr_api::prelude::*;
use zarrs::array::ArraySubset;
use zarrs_codec::CodecOptions;

use crate::array_open;
use crate::dtype_dispatch::{self, RTypeFamily};
use crate::error::PizzarrError;

/// Store a contiguous subset of an array from R data.
///
/// # Arguments
///
/// * `store_url` - Filesystem path or URL to the store root.
/// * `array_path` - Path to the array within the store.
/// * `ranges` - R list of length-2 integer vectors `c(start, stop)`,
///   0-based, exclusive stop.
/// * `data` - R vector (numeric, integer, or logical) in C-order.
/// * `concurrent_target` - Optional codec concurrency override.
///
/// # Returns
///
/// `true` on success.
///
/// # Errors
///
/// Returns an R error if the store/array cannot be opened, the dtype
/// is unsupported, data cannot be narrowed to the stored type, or the
/// store operation fails.
pub(crate) fn store_subset(
    store_url: &str,
    array_path: &str,
    ranges: List,
    data: Robj,
    concurrent_target: Nullable<i32>,
) -> extendr_api::Result<bool> {
    let array = array_open::open_array_at_path(store_url, array_path)?;

    // Convert R list of c(start, stop) to Vec<Range<u64>>.
    let ranges_vec = parse_ranges(&ranges, store_url, array_path)?;

    let subset = ArraySubset::new_with_ranges(&ranges_vec);

    // Build CodecOptions if concurrent_target is provided.
    let opts = match concurrent_target {
        Nullable::NotNull(n) => CodecOptions::default().with_concurrent_target(n as usize),
        Nullable::Null => CodecOptions::default(),
    };

    // Classify dtype and dispatch storage.
    let dt = array.data_type();
    let family =
        dtype_dispatch::dtype_family(dt).ok_or_else(|| PizzarrError::DTypeUnsupported {
            dtype: dt.to_string(),
        })?;

    dispatch_store(&array, &subset, &opts, family, &data, store_url, array_path)?;

    Ok(true)
}

/// Parse R list of `c(start, stop)` into `Vec<Range<u64>>`.
fn parse_ranges(
    ranges: &List,
    store_url: &str,
    array_path: &str,
) -> std::result::Result<Vec<Range<u64>>, PizzarrError> {
    let mut out = Vec::with_capacity(ranges.len());
    for (_, robj) in ranges.iter() {
        let pair: Vec<i32> = robj
            .as_integer_slice()
            .ok_or_else(|| PizzarrError::Store {
                url: store_url.to_string(),
                path: array_path.to_string(),
                reason: "each range must be an integer vector of length 2".to_string(),
            })?
            .to_vec();
        if pair.len() != 2 {
            return Err(PizzarrError::Store {
                url: store_url.to_string(),
                path: array_path.to_string(),
                reason: format!("range has length {}, expected 2", pair.len()),
            });
        }
        out.push(pair[0] as u64..pair[1] as u64);
    }
    Ok(out)
}

/// Helper: store elements as `Vec<T>` using the current API.
///
/// `Vec<T>` implements `IntoArrayBytes` when `T: Element`, so we can
/// pass it directly to `store_array_subset_opt`.
fn store_with_opts<T: zarrs::array::ElementOwned>(
    array: &zarrs::array::Array<dyn zarrs_storage::ReadableWritableListableStorageTraits>,
    subset: &ArraySubset,
    data: Vec<T>,
    opts: &CodecOptions,
    store_url: &str,
    array_path: &str,
) -> std::result::Result<(), PizzarrError> {
    array
        .store_array_subset_opt(subset, data, opts)
        .map_err(|e| PizzarrError::Store {
            url: store_url.to_string(),
            path: array_path.to_string(),
            reason: e.to_string(),
        })
}

/// Extract R doubles as `Vec<f64>`.
fn extract_doubles(data: &Robj, store_url: &str, array_path: &str) -> std::result::Result<Vec<f64>, PizzarrError> {
    data.as_real_slice()
        .ok_or_else(|| PizzarrError::Store {
            url: store_url.to_string(),
            path: array_path.to_string(),
            reason: "expected numeric (double) vector".to_string(),
        })
        .map(|s| s.to_vec())
}

/// Extract R integers as `Vec<i32>`.
fn extract_integers(data: &Robj, store_url: &str, array_path: &str) -> std::result::Result<Vec<i32>, PizzarrError> {
    data.as_integer_slice()
        .ok_or_else(|| PizzarrError::Store {
            url: store_url.to_string(),
            path: array_path.to_string(),
            reason: "expected integer vector".to_string(),
        })
        .map(|s| s.to_vec())
}

/// Dispatch storage based on R type family, narrowing as needed.
///
/// Each arm extracts the R data, narrows to the stored type with
/// range checks where necessary, and calls `store_with_opts`.
fn dispatch_store(
    array: &zarrs::array::Array<dyn zarrs_storage::ReadableWritableListableStorageTraits>,
    subset: &ArraySubset,
    opts: &CodecOptions,
    family: RTypeFamily,
    data: &Robj,
    store_url: &str,
    array_path: &str,
) -> std::result::Result<(), PizzarrError> {
    // Macro for narrowing R doubles to a smaller numeric type.
    macro_rules! narrow_double_to {
        ($t:ty) => {{
            let raw = extract_doubles(data, store_url, array_path)?;
            let narrowed: Vec<$t> = raw
                .into_iter()
                .map(|v| {
                    let converted = v as $t;
                    // Round-trip check for lossless narrowing
                    if (converted as f64 - v).abs() > 0.5 {
                        Err(PizzarrError::Store {
                            url: store_url.to_string(),
                            path: array_path.to_string(),
                            reason: format!(
                                "value {} out of range for {}",
                                v,
                                stringify!($t)
                            ),
                        })
                    } else {
                        Ok(converted)
                    }
                })
                .collect::<std::result::Result<Vec<$t>, _>>()?;
            store_with_opts(array, subset, narrowed, opts, store_url, array_path)
        }};
    }

    // Macro for narrowing R integers to a smaller integer type.
    macro_rules! narrow_integer_to {
        ($t:ty) => {{
            let raw = extract_integers(data, store_url, array_path)?;
            let narrowed: Vec<$t> = raw
                .into_iter()
                .map(|v| {
                    <$t>::try_from(v).map_err(|_| PizzarrError::Store {
                        url: store_url.to_string(),
                        path: array_path.to_string(),
                        reason: format!(
                            "value {} out of range for {}",
                            v,
                            stringify!($t)
                        ),
                    })
                })
                .collect::<std::result::Result<Vec<$t>, _>>()?;
            store_with_opts(array, subset, narrowed, opts, store_url, array_path)
        }};
    }

    match family {
        RTypeFamily::Double => {
            let raw = extract_doubles(data, store_url, array_path)?;
            store_with_opts(array, subset, raw, opts, store_url, array_path)
        }
        RTypeFamily::Float32AsDouble => {
            let raw = extract_doubles(data, store_url, array_path)?;
            let narrowed: Vec<f32> = raw.into_iter().map(|v| v as f32).collect();
            store_with_opts(array, subset, narrowed, opts, store_url, array_path)
        }
        RTypeFamily::Integer => {
            let raw = extract_integers(data, store_url, array_path)?;
            store_with_opts(array, subset, raw, opts, store_url, array_path)
        }
        RTypeFamily::Int16AsInteger => narrow_integer_to!(i16),
        RTypeFamily::Int8AsInteger => narrow_integer_to!(i8),
        RTypeFamily::Uint8AsInteger => narrow_integer_to!(u8),
        RTypeFamily::Uint16AsInteger => narrow_integer_to!(u16),
        RTypeFamily::Uint32AsDouble => narrow_double_to!(u32),
        RTypeFamily::Int64AsDouble => narrow_double_to!(i64),
        RTypeFamily::Uint64AsDouble => narrow_double_to!(u64),
        RTypeFamily::Logical => {
            let logicals: Vec<Rbool> = data
                .as_logical_slice()
                .ok_or_else(|| PizzarrError::Store {
                    url: store_url.to_string(),
                    path: array_path.to_string(),
                    reason: "expected logical vector".to_string(),
                })?
                .to_vec();
            let bools: Vec<bool> = logicals.into_iter().map(|v| v.is_true()).collect();
            store_with_opts(array, subset, bools, opts, store_url, array_path)
        }
    }
}
