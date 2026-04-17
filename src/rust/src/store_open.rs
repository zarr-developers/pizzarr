//! Store opening: URL to zarrs store handle.
//!
//! Phase 1 supports filesystem stores (bare paths and `file://` URLs).
//! Phase 4 Iteration 1 adds synchronous HTTP via `zarrs_http::HTTPStore`.
//! Phase 4 Iteration 2 adds S3/GCS via `object_store` + async-to-sync adapter.

use std::sync::Arc;

use crate::error::PizzarrError;
use crate::store_cache::{self, StorageEntry};

/// Open (or reuse) a store for the given URL.
///
/// Supported URL schemes:
/// - Bare path: `/path/to/store` or `C:\path\to\store`
/// - `file:///path/to/store`
/// - `http://` or `https://` (requires `http_sync` feature)
///
/// # Errors
///
/// Returns [`PizzarrError::StoreOpen`] if the path/URL is invalid or
/// the store cannot be created.
/// Returns [`PizzarrError::FeatureNotCompiled`] for unsupported URL
/// schemes.
pub(crate) fn open_store(url: &str) -> Result<StorageEntry, PizzarrError> {
    store_cache::get_or_insert(url, |_normalized_url| {
        let trimmed = url.trim_end_matches('/');

        // --- HTTP/HTTPS ---
        if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
            return open_http_store(trimmed);
        }

        // --- S3 ---
        if trimmed.starts_with("s3://") {
            return open_s3_store(trimmed);
        }

        // --- GCS ---
        if trimmed.starts_with("gs://") {
            return open_gcs_store(trimmed);
        }

        // --- Azure (not yet supported) ---
        if trimmed.starts_with("az://") {
            return Err(PizzarrError::FeatureNotCompiled {
                feature: "full".to_string(),
            });
        }

        // --- Filesystem ---
        let path = url_to_path(url)?;
        open_filesystem_store(url, &path)
    })
}

/// Open a filesystem store.
fn open_filesystem_store(url: &str, path: &str) -> Result<StorageEntry, PizzarrError> {
    #[cfg(feature = "filesystem")]
    {
        let store = zarrs_filesystem::FilesystemStore::new(path).map_err(|e| {
            PizzarrError::StoreOpen {
                url: url.to_string(),
                reason: e.to_string(),
            }
        })?;
        Ok(StorageEntry::ReadWriteList(Arc::new(store)))
    }

    #[cfg(not(feature = "filesystem"))]
    {
        let _ = path;
        Err(PizzarrError::FeatureNotCompiled {
            feature: "filesystem".to_string(),
        })
    }
}

/// Open an HTTP store (read-only).
#[allow(unused_variables)]
fn open_http_store(url: &str) -> Result<StorageEntry, PizzarrError> {
    #[cfg(feature = "http_sync")]
    {
        let mut http_store =
            zarrs_http::HTTPStore::new(url).map_err(|e| PizzarrError::StoreOpen {
                url: url.to_string(),
                reason: e.to_string(),
            })?;
        // Apply global HTTP config before wrapping in Arc.
        let batch = crate::http_config::http_batch_range_requests();
        http_store.set_batch_range_requests(batch);
        Ok(StorageEntry::ReadOnly(Arc::new(http_store)))
    }

    #[cfg(not(feature = "http_sync"))]
    {
        Err(PizzarrError::FeatureNotCompiled {
            feature: "http_sync".to_string(),
        })
    }
}

/// Extract a filesystem path from a URL or bare path.
///
/// # Errors
///
/// Returns [`PizzarrError::FeatureNotCompiled`] for non-filesystem
/// URL schemes.
fn url_to_path(url: &str) -> Result<String, PizzarrError> {
    let trimmed = url.trim_end_matches('/');

    if let Some(rest) = trimmed.strip_prefix("file://") {
        // file:///path/to/store → /path/to/store
        // On Windows: file:///C:/path → strip leading / → C:/path
        #[cfg(windows)]
        {
            let path =
                if rest.len() >= 3 && rest.as_bytes()[0] == b'/' && rest.as_bytes()[2] == b':' {
                    &rest[1..]
                } else {
                    rest
                };
            Ok(path.to_string())
        }

        #[cfg(not(windows))]
        {
            Ok(rest.to_string())
        }
    } else {
        // Bare filesystem path.
        Ok(trimmed.to_string())
    }
}

/// Parse a cloud URL into bucket-only URL and prefix path.
///
/// `s3://bucket/prefix/path` → (`s3://bucket`, `prefix/path`)
/// `gs://bucket/prefix`      → (`gs://bucket`, `prefix`)
/// `s3://bucket`             → (`s3://bucket`, `""`)
#[cfg(any(feature = "s3", feature = "gcs"))]
fn split_bucket_prefix(url: &str) -> (String, String) {
    // Find the third slash (after scheme://)
    if let Some(scheme_end) = url.find("://") {
        let rest = &url[scheme_end + 3..];
        if let Some(slash_pos) = rest.find('/') {
            let bucket_url = format!("{}{}", &url[..scheme_end + 3], &rest[..slash_pos]);
            let prefix = rest[slash_pos + 1..].trim_end_matches('/').to_string();
            (bucket_url, prefix)
        } else {
            (url.to_string(), String::new())
        }
    } else {
        (url.to_string(), String::new())
    }
}

/// Wrap an object_store backend with optional prefix and async-to-sync adapter.
#[cfg(any(feature = "s3", feature = "gcs"))]
fn wrap_object_store(
    store: impl object_store::ObjectStore,
    prefix: &str,
    url: &str,
) -> Result<StorageEntry, PizzarrError> {
    use object_store::prefix::PrefixStore;
    use zarrs::storage::storage_adapter::async_to_sync::AsyncToSyncStorageAdapter;
    use zarrs_object_store::AsyncObjectStore;

    if prefix.is_empty() {
        let async_store = Arc::new(AsyncObjectStore::new(store));
        let sync_store =
            Arc::new(AsyncToSyncStorageAdapter::new(async_store, crate::runtime::TokioBlockOn));
        Ok(StorageEntry::ReadWriteList(sync_store))
    } else {
        let prefix_path = object_store::path::Path::from(prefix);
        let prefix_store = PrefixStore::new(store, prefix_path);
        let async_store = Arc::new(AsyncObjectStore::new(prefix_store));
        let sync_store =
            Arc::new(AsyncToSyncStorageAdapter::new(async_store, crate::runtime::TokioBlockOn));
        Ok(StorageEntry::ReadWriteList(sync_store))
    }
}

/// Open an S3 store via object_store + async-to-sync adapter.
#[allow(unused_variables)]
fn open_s3_store(url: &str) -> Result<StorageEntry, PizzarrError> {
    #[cfg(feature = "s3")]
    {
        use object_store::aws::AmazonS3Builder;

        let (bucket_url, prefix) = split_bucket_prefix(url);
        let store = AmazonS3Builder::from_env()
            .with_url(&bucket_url)
            .with_skip_signature(true)
            .build()
            .map_err(|e| PizzarrError::StoreOpen {
                url: url.to_string(),
                reason: e.to_string(),
            })?;
        wrap_object_store(store, &prefix, url)
    }

    #[cfg(not(feature = "s3"))]
    {
        Err(PizzarrError::FeatureNotCompiled {
            feature: "s3".to_string(),
        })
    }
}

/// Open a GCS store via object_store + async-to-sync adapter.
#[allow(unused_variables)]
fn open_gcs_store(url: &str) -> Result<StorageEntry, PizzarrError> {
    #[cfg(feature = "gcs")]
    {
        use object_store::gcp::GoogleCloudStorageBuilder;

        let (bucket_url, prefix) = split_bucket_prefix(url);
        let store = GoogleCloudStorageBuilder::from_env()
            .with_url(&bucket_url)
            .build()
            .map_err(|e| PizzarrError::StoreOpen {
                url: url.to_string(),
                reason: e.to_string(),
            })?;
        wrap_object_store(store, &prefix, url)
    }

    #[cfg(not(feature = "gcs"))]
    {
        Err(PizzarrError::FeatureNotCompiled {
            feature: "gcs".to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_to_path_bare_path() {
        assert_eq!(url_to_path("/tmp/data").unwrap(), "/tmp/data");
        assert_eq!(url_to_path("/tmp/data/").unwrap(), "/tmp/data");
    }

    #[cfg(not(windows))]
    #[test]
    fn url_to_path_file_url_unix() {
        assert_eq!(url_to_path("file:///tmp/data").unwrap(), "/tmp/data");
    }

    #[cfg(windows)]
    #[test]
    fn url_to_path_file_url_windows() {
        assert_eq!(
            url_to_path("file:///C:/Users/data").unwrap(),
            "C:/Users/data"
        );
    }
}
