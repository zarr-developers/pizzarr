//! Tokio runtime singleton for async-to-sync store adaptation.
//!
//! Lazily initialized on first use of an async backend (S3, GCS,
//! object_store HTTP). Never created on the CRAN tier (no `full`
//! feature). Worker thread count read from `PIZZARR_TOKIO_THREADS`
//! env var or default (4).

use std::sync::OnceLock;

use tokio::runtime::Runtime;
use zarrs::storage::storage_adapter::async_to_sync::AsyncToSyncBlockOn;

/// Process-global tokio runtime.
static TOKIO_RT: OnceLock<Runtime> = OnceLock::new();

/// Default worker thread count when `PIZZARR_TOKIO_THREADS` is unset.
const DEFAULT_THREADS: usize = 4;

/// Adapter implementing [`AsyncToSyncBlockOn`] via the singleton runtime.
pub(crate) struct TokioBlockOn;

impl TokioBlockOn {
    /// Get or create the singleton tokio runtime.
    fn runtime() -> &'static Runtime {
        TOKIO_RT.get_or_init(|| {
            let threads = std::env::var("PIZZARR_TOKIO_THREADS")
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .filter(|&n| n > 0)
                .unwrap_or(DEFAULT_THREADS);
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(threads)
                .enable_all()
                .build()
                .expect("pizzarr: failed to create tokio runtime")
        })
    }

    /// Whether the runtime has been initialized.
    pub(crate) fn is_active() -> bool {
        TOKIO_RT.get().is_some()
    }
}

impl AsyncToSyncBlockOn for TokioBlockOn {
    fn block_on<F: core::future::Future>(&self, future: F) -> F::Output {
        Self::runtime().block_on(future)
    }
}
