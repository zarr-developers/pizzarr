# User-facing configuration for pizzarr concurrency settings.
#
# Three settings control zarrs parallelism and HTTP behaviour:
#   - nthreads: rayon thread pool size (set-once per process)
#   - concurrent_target: codec concurrency level (dynamic)
#   - http_batch_range_requests: multipart range toggle (at store creation)
#
# Each is backed by an R option (pizzarr.*) and an env var (PIZZARR_*).
# The env var → option mapping is handled by init_options() in options.R.
# This file provides the user-facing pizzarr_config() getter/setter and
# the internal apply_zarrs_config() called from .onLoad.

#' Get or set pizzarr configuration
#'
#' Controls parallelism and HTTP behaviour for the zarrs backend.
#' Called with no arguments, returns the current settings as a named list.
#' Called with arguments, sets the specified options and applies them to
#' the Rust backend immediately.
#'
#' @param nthreads Integer or NULL. Number of threads for the rayon thread
#'   pool. NULL uses all CPUs (the default). The pool can only be initialised
#'   once per R session; later changes require a restart. Use the
#'   \code{PIZZARR_NTHREADS} environment variable for reliable session-level
#'   control.
#' @param concurrent_target Integer or NULL. Codec concurrency level — how
#'   many codec operations zarrs runs in parallel within a single read/write
#'   call. NULL uses the zarrs default (CPU count). Can be changed at any time.
#' @param http_batch_range_requests Logical or NULL. Whether HTTP stores use
#'   multipart range requests (default TRUE). Set to FALSE for servers with
#'   incomplete multipart range support. Takes effect on the next
#'   \code{zarr_open()} or \code{zarrs_get_subset()} call that opens a new
#'   HTTP store; existing cached stores are not affected (use
#'   \code{zarrs_close_store(url)} to force re-creation).
#' @return When called with no arguments, a named list of current settings.
#'   When called with arguments, the previous values (invisibly).
#' @export
pizzarr_config <- function(nthreads = NULL, concurrent_target = NULL,
                           http_batch_range_requests = NULL) {

  getting <- is.null(nthreads) && is.null(concurrent_target) &&
    is.null(http_batch_range_requests)

  if (getting) {
    out <- list(
      nthreads = getOption("pizzarr.nthreads"),
      concurrent_target = getOption("pizzarr.concurrent_target"),
      http_batch_range_requests = getOption("pizzarr.http_batch_range_requests",
                                            TRUE)
    )
    # When zarrs is available, report the actual Rust-side thread count.
    if (.pizzarr_env$zarrs_available) {
      info <- zarrs_runtime_info()
      out$nthreads <- info$nthreads
      out$concurrent_target <- info$codec_concurrent_target
    }
    return(out)
  }

  # --- setter path ---
  prev <- list(
    nthreads = getOption("pizzarr.nthreads"),
    concurrent_target = getOption("pizzarr.concurrent_target"),
    http_batch_range_requests = getOption("pizzarr.http_batch_range_requests",
                                          TRUE)
  )

  if (!is.null(nthreads)) {
    nthreads <- as.integer(nthreads)
    options(pizzarr.nthreads = nthreads)
    if (.pizzarr_env$zarrs_available) {
      ok <- zarrs_set_nthreads(nthreads)
      if (!ok) {
        warning(
          "rayon thread pool already initialised; nthreads change takes ",
          "effect on restart. Set PIZZARR_NTHREADS env var for reliable ",
          "session-level control.",
          call. = FALSE
        )
      }
    } else {
      message("zarrs backend not available; nthreads stored but not applied.")
    }
  }

  if (!is.null(concurrent_target)) {
    concurrent_target <- as.integer(concurrent_target)
    options(pizzarr.concurrent_target = concurrent_target)
    if (.pizzarr_env$zarrs_available) {
      zarrs_set_codec_concurrent_target(concurrent_target)
    } else {
      message(
        "zarrs backend not available; concurrent_target stored but not applied."
      )
    }
  }

  if (!is.null(http_batch_range_requests)) {
    http_batch_range_requests <- as.logical(http_batch_range_requests)
    options(pizzarr.http_batch_range_requests = http_batch_range_requests)
    if (.pizzarr_env$zarrs_available) {
      zarrs_set_http_batch_range_requests(http_batch_range_requests)
    } else {
      message(
        "zarrs backend not available; http_batch_range_requests stored ",
        "but not applied."
      )
    }
  }

  invisible(prev)
}

# Apply R options to the Rust backend.
#
# Called from .onLoad after init_options() and zarrs availability check.
# Reads the three pizzarr.* options and calls the corresponding Rust
# setters. Silently skips NULL values (= use defaults).
#
# @keywords internal
apply_zarrs_config <- function() {
  ct <- getOption("pizzarr.concurrent_target")
  if (!is.null(ct)) {
    zarrs_set_codec_concurrent_target(as.integer(ct))
  }

  nt <- getOption("pizzarr.nthreads")
  if (!is.null(nt)) {
    zarrs_set_nthreads(as.integer(nt))
    # If FALSE (pool already init), silently accept — .onLoad should not warn.
  }

  batch <- getOption("pizzarr.http_batch_range_requests")
  if (!is.null(batch)) {
    zarrs_set_http_batch_range_requests(as.logical(batch))
  }
}
