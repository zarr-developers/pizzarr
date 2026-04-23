//! HTTP store configuration.
//!
//! Process-global settings applied at HTTP store creation time.
//! The batch range requests flag controls whether `zarrs_http::HTTPStore`
//! uses multipart range requests (default: true). Some servers have
//! incomplete multipart range support; set to false for compatibility.

use std::sync::atomic::{AtomicBool, Ordering};

/// Whether HTTP stores should batch range requests (multipart).
/// Defaults to `true`.
static HTTP_BATCH_RANGE_REQUESTS: AtomicBool = AtomicBool::new(true);

/// Get the current batch range requests setting.
pub(crate) fn http_batch_range_requests() -> bool {
    HTTP_BATCH_RANGE_REQUESTS.load(Ordering::Relaxed)
}

/// Set the batch range requests flag.
pub(crate) fn set_http_batch_range_requests(enable: bool) {
    HTTP_BATCH_RANGE_REQUESTS.store(enable, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_true() {
        assert!(http_batch_range_requests());
    }
}
