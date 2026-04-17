//! Runtime diagnostics and configuration.
//!
//! Provides [`runtime_info`] for debugging,
//! [`set_codec_concurrent_target`] for tuning zarrs parallelism,
//! and [`set_nthreads`] for controlling the rayon thread pool size.

use extendr_api::prelude::*;

use crate::store_cache;

/// Return runtime information about the zarrs backend.
///
/// Returns a named list with:
/// - `codec_concurrent_target`: integer
/// - `nthreads`: integer (rayon thread pool size)
/// - `store_cache_entries`: integer
/// - `compiled_features`: character vector
pub(crate) fn runtime_info() -> List {
    let target = zarrs::config::global_config().codec_concurrent_target() as i32;
    let nthreads = rayon::current_num_threads() as i32;
    let entries = store_cache::entry_count() as i32;
    // Re-use the compiled features logic from lib.rs.
    let features = crate::compiled_features();
    list!(
        codec_concurrent_target = target,
        nthreads = nthreads,
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

/// Set the rayon thread pool size.
///
/// Initialises the rayon global thread pool with `n` threads. The pool
/// can only be initialised once per process; subsequent calls return
/// `false` without error (the pool retains its original size).
///
/// # Errors
///
/// Returns an error if `n` is not positive.
pub(crate) fn set_nthreads(n: i32) -> extendr_api::Result<bool> {
    if n < 1 {
        return Err(extendr_api::Error::Other(
            "nthreads must be a positive integer".to_string(),
        ));
    }
    match rayon::ThreadPoolBuilder::new()
        .num_threads(n as usize)
        .build_global()
    {
        Ok(()) => Ok(true),
        Err(_) => Ok(false),
    }
}
