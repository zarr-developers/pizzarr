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

#' Convert pizzarr indexer to zarrs ranges
#'
#' Extracts per-dimension start/stop from a BasicIndexer's
#' SliceDimIndexer and IntDimIndexer objects. Returns a list of
#' length-2 integer vectors: \code{list(c(start, stop), ...)}.
#' 0-based, exclusive stop. IntDimIndexer dims become length-1 ranges.
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
