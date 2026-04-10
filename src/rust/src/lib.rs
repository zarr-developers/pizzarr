//! pizzarr: extendr bridge to the zarrs Rust crate.
//!
//! Exposes zarrs chunk I/O, codec pipeline, and store abstraction
//! to R via a small set of `#[extendr]`-annotated functions.

use extendr_api::prelude::*;

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

// Macro to generate exports.
// This ensures exported functions are registered with R.
// See corresponding C code in `entrypoint.c`.
extendr_module! {
    mod pizzarr;
    fn zarrs_compiled_features;
}
