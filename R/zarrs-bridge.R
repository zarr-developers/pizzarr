# zarrs backend availability and upgrade helpers
#
# The zarrs Rust backend is compiled into the shared library on the
# r-universe tier. On the CRAN tier (pure R, no src/), the shared
# library does not exist and these functions report that zarrs is
# unavailable.

# Package-level environment, created at source time.
# .onLoad populates $zarrs_available.
.pizzarr_env <- new.env(parent = emptyenv())

#' Check whether the zarrs Rust backend is available
#'
#' @return Logical scalar. \code{TRUE} when the package was installed with the
#'   compiled zarrs backend (r-universe tier), \code{FALSE} on the CRAN tier
#'   (pure R).
#' @keywords internal
is_zarrs_available <- function() {
  tryCatch(
    {
      feats <- zarrs_compiled_features()
      is.character(feats) && length(feats) > 0
    },
    error = function(e) FALSE
  )
}

#' List compiled zarrs features
#'
#' Returns the feature flags compiled into the zarrs backend, or
#' \code{character(0)} with a message when the backend is absent.
#'
#' @return Character vector of feature names (e.g. \code{"filesystem"},
#'   \code{"gzip"}).
#' @export
pizzarr_compiled_features <- function() {
  if (.pizzarr_env$zarrs_available) {
    zarrs_compiled_features()
  } else {
    message(
      "zarrs backend not available (pure R install).\n",
      "See ?pizzarr_upgrade for the r-universe install."
    )
    character(0)
  }
}

#' Upgrade to the zarrs backend
#'
#' Prints the command to install pizzarr from r-universe with the
#' compiled zarrs backend, or reports that zarrs is already available.
#'
#' @export
pizzarr_upgrade <- function() {
  if (.pizzarr_env$zarrs_available) {
    message("zarrs backend is already available.")
  } else {
    message(
      "Install pizzarr with the zarrs backend from r-universe:\n\n",
      '  install.packages("pizzarr", ',
      'repos = "https://zarr-developers.r-universe.dev")\n'
    )
  }
  invisible(NULL)
}
