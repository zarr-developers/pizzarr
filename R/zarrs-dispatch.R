# zarrs dispatch helpers
#
# R-side functions for deciding whether to use the zarrs fast path
# and converting pizzarr selection objects to zarrs ranges.

#' Check whether the zarrs path can handle this selection
#'
#' Returns TRUE only for BasicIndexer with all SliceDimIndexer dims
#' (contiguous slice selections). IntDimIndexer (scalar selection),
#' IntArrayDimIndexer (fancy indexing), OrthogonalIndexer, and
#' negative steps all fall through to R-native.
#'
#' @param indexer A BasicIndexer or OrthogonalIndexer object.
#' @param store A Store object.
#' @return Logical scalar.
#' @keywords internal
can_use_zarrs <- function(indexer, store) {
  if (!.pizzarr_env$zarrs_available) return(FALSE)
  store_id <- store$get_store_identifier()
  if (is.null(store_id)) return(FALSE)  # MemoryStore

  # Only BasicIndexer with SliceDimIndexer on every dimension
  if (!inherits(indexer, "BasicIndexer")) return(FALSE)
  all(vapply(indexer$dim_indexers, function(di) {
    inherits(di, "SliceDimIndexer")
  }, logical(1)))
}

#' Convert pizzarr indexer to zarrs ranges
#'
#' Extracts per-dimension start/stop from a BasicIndexer's
#' SliceDimIndexer objects. Returns a list of length-2 integer
#' vectors: \code{list(c(start, stop), ...)}. 0-based, exclusive stop.
#'
#' @param indexer A BasicIndexer with SliceDimIndexer on every dimension.
#' @return List of integer vectors.
#' @keywords internal
selection_to_ranges <- function(indexer) {
  lapply(indexer$dim_indexers, function(di) {
    as.integer(c(di$start, di$stop))
  })
}
