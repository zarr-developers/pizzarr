//! Runtime diagnostics and configuration.
//!
//! Provides [`runtime_info`] for debugging and
//! [`set_codec_concurrent_target`] for tuning zarrs parallelism.

use extendr_api::prelude::*;

use crate::store_cache;

/// Return runtime information about the zarrs backend.
///
/// Returns a named list with:
/// - `codec_concurrent_target`: integer
/// - `store_cache_entries`: integer
/// - `compiled_features`: character vector
pub(crate) fn runtime_info() -> List {
    let target = zarrs::config::global_config().codec_concurrent_target() as i32;
    let entries = store_cache::entry_count() as i32;
    // Re-use the compiled features logic from lib.rs.
    let features = crate::compiled_features();
    list!(
        codec_concurrent_target = target,
        store_cache_entries = entries,
        compiled_features = features
    )
}

/// Set the zarrs codec concurrent target.
///
/// Controls the number of concurrent codec operations zarrs uses
/// within a single array operation. Defaults to the number of CPUs.
///
/// # Errors
///
/// Returns an error if `n` is not positive.
pub(crate) fn set_codec_concurrent_target(n: i32) -> extendr_api::Result<()> {
    if n < 1 {
        return Err(extendr_api::Error::Other(
            "codec_concurrent_target must be a positive integer".to_string(),
        ));
    }
    zarrs::config::global_config_mut().set_codec_concurrent_target(n as usize);
    Ok(())
}
