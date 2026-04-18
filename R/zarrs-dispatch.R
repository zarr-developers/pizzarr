# zarrs dispatch helpers
#
# R-side functions for deciding whether to use the zarrs fast path
# and converting pizzarr selection objects to zarrs ranges.

#' Check whether the zarrs path can handle this selection
#'
#' Returns TRUE for BasicIndexer with contiguous SliceDimIndexer
#' (step==1) and/or IntDimIndexer on every dimension. Step>1 slices,
#' IntArrayDimIndexer (fancy indexing), and OrthogonalIndexer fall
#' through to R-native.
#'
#' @param indexer A BasicIndexer or OrthogonalIndexer object.
#' @param store A Store object.
#' @return Logical scalar.
#' @keywords internal
can_use_zarrs <- function(indexer, store) {
  if (!.pizzarr_env$zarrs_available) return(FALSE)
  store_id <- store$get_store_identifier()
  if (is.null(store_id)) return(FALSE)  # MemoryStore

  # Check cloud store features are compiled
  features <- pizzarr_compiled_features()
  if (inherits(store, "S3Store") && !("s3" %in% features)) return(FALSE)
  if (inherits(store, "GcsStore") && !("gcs" %in% features)) return(FALSE)

  # BasicIndexer with contiguous SliceDimIndexer or IntDimIndexer on every dim
  if (!inherits(indexer, "BasicIndexer")) return(FALSE)
  all(vapply(indexer$dim_indexers, function(di) {
    if (inherits(di, "SliceDimIndexer")) {
      # Reject step>1 — zarrs retrieves contiguous blocks only
      di$step == 1L
    } else {
      inherits(di, "IntDimIndexer")
    }
  }, logical(1)))
}

#' Check whether the zarrs write path can handle this selection
#'
#' Same eligibility as read. Separate function for future extensibility
#' (e.g., read-only remote stores).
#'
#' @param indexer A BasicIndexer or OrthogonalIndexer object.
#' @param store A Store object.
#' @return Logical scalar.
#' @keywords internal
can_use_zarrs_write <- function(indexer, store) {
  # Remote stores are read-only for now
  if (inherits(store, "HttpStore")) return(FALSE)
  if (inherits(store, "S3Store")) return(FALSE)
  if (inherits(store, "GcsStore")) return(FALSE)
  can_use_zarrs(indexer, store)
}

#' Check whether the zarrs create path can handle this array
#'
#' Returns TRUE when zarrs is available, the store is writable and
#' has a store identifier, and the dtype is numeric/logical (not
#' object or string).
#'
#' @param store A Store object.
#' @param dtype A Dtype object.
#' @return Logical scalar.
#' @keywords internal
can_use_zarrs_create <- function(store, dtype) {
  if (!.pizzarr_env$zarrs_available) return(FALSE)
  store_id <- store$get_store_identifier()
  if (is.null(store_id)) return(FALSE)  # MemoryStore
  if (inherits(store, "HttpStore")) return(FALSE)  # read-only
  if (inherits(store, "S3Store")) return(FALSE)  # read-only for now
  if (inherits(store, "GcsStore")) return(FALSE)  # read-only for now
  if (dtype$is_object) return(FALSE)
  if (dtype$basic_type %in% c("S", "U")) return(FALSE)
  TRUE
}

#' Map an R compressor object to a zarrs codec preset string
#'
#' Returns one of \code{"none"}, \code{"gzip"}, \code{"blosc"},
#' \code{"zstd"}, or \code{NA_character_} if the compressor is not
#' recognised (signalling fallback to R-native).
#'
#' @param compressor A codec object or NA.
#' @param compressor_config A config list or NA.
#' @return Character scalar.
#' @keywords internal
compressor_to_preset <- function(compressor, compressor_config) {
  if (is_na(compressor) || is.null(compressor)) return("none")
  if (is.list(compressor_config)) {
    id <- compressor_config$id
    if (id %in% c("zlib", "gzip")) return("gzip")
    if (identical(id, "blosc")) return("blosc")
    if (identical(id, "zstd")) return("zstd")
  }
  NA_character_
}

#' Convert pizzarr indexer to zarrs ranges
#'
#' @param indexer A BasicIndexer with SliceDimIndexer or IntDimIndexer dims.
#' @return List of integer vectors.
#' @keywords internal
selection_to_ranges <- function(indexer) {
  lapply(indexer$dim_indexers, function(di) {
    if (inherits(di, "IntDimIndexer")) {
      # Scalar selection: single-element range [i, i+1)
      as.integer(c(di$dim_sel, di$dim_sel + 1L))
    } else {
      as.integer(c(di$start, di$stop))
    }
  })
}
