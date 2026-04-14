//! Store opening: URL to zarrs store handle.
//!
//! Phase 1 supports only filesystem stores (bare paths and `file://`
//! URLs). HTTP, S3, and GCS backends will be added in Phase 4.

use std::sync::Arc;

use crate::error::PizzarrError;
use crate::store_cache::{self, StorageEntry};

/// Open (or reuse) a store for the given URL.
///
/// Currently only filesystem paths are supported:
/// - Bare path: `/path/to/store` or `C:\path\to\store`
/// - `file:///path/to/store`
///
/// # Errors
///
/// Returns [`PizzarrError::StoreOpen`] if the path is invalid or
/// the store cannot be created.
/// Returns [`PizzarrError::FeatureNotCompiled`] for unsupported URL
/// schemes (HTTP, S3, GCS, Azure).
pub(crate) fn open_store(url: &str) -> Result<StorageEntry, PizzarrError> {
    store_cache::get_or_insert(url, |_normalized_url| {
        let path = url_to_path(url)?;

        #[cfg(feature = "filesystem")]
        {
            let store = zarrs_filesystem::FilesystemStore::new(&path).map_err(|e| {
                PizzarrError::StoreOpen {
                    url: url.to_string(),
                    reason: e.to_string(),
                }
            })?;
            Ok(Arc::new(store) as StorageEntry)
        }

        #[cfg(not(feature = "filesystem"))]
        {
            let _ = path;
            Err(PizzarrError::FeatureNotCompiled {
                feature: "filesystem".to_string(),
            })
        }
    })
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
    } else if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        Err(PizzarrError::FeatureNotCompiled {
            feature: "http".to_string(),
        })
    } else if trimmed.starts_with("s3://")
        || trimmed.starts_with("gs://")
        || trimmed.starts_with("az://")
    {
        Err(PizzarrError::FeatureNotCompiled {
            feature: "full".to_string(),
        })
    } else {
        // Bare filesystem path.
        Ok(trimmed.to_string())
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

    #[test]
    fn url_to_path_unsupported_scheme_errors() {
        assert!(url_to_path("http://example.com/data").is_err());
        assert!(url_to_path("https://example.com/data").is_err());
        assert!(url_to_path("s3://bucket/prefix").is_err());
        assert!(url_to_path("gs://bucket/prefix").is_err());
        assert!(url_to_path("az://container/prefix").is_err());
    }
}
