# Adapted from https://github.com/IRkernel/IRkernel/blob/master/R/options.r

#' pizzarr_option_defaults
#' @description
#' * pizzarr.http_store_cache_time_seconds how long to cache web requests
#' * pizzarr.nthreads number of threads for parallel codec operations
#'   (NULL = all CPUs). Set-once: takes effect only before the first zarrs
#'   operation. Use env var PIZZARR_NTHREADS for reliable session-level control.
#' * pizzarr.concurrent_target codec concurrency level — how many codec
#'   operations zarrs runs in parallel within a single read/write call
#'   (NULL = zarrs default, typically CPU count). Can be changed at any time.
#' * pizzarr.http_batch_range_requests whether HTTP stores use multipart
#'   range requests (TRUE by default). Set to FALSE for servers with
#'   incomplete multipart range support. Takes effect on next store open.
#' @export
pizzarr_option_defaults <- list(
    pizzarr.http_store_cache_time_seconds = 3600,
    pizzarr.nthreads = NULL,
    pizzarr.concurrent_target = NULL,
    pizzarr.http_batch_range_requests = TRUE
)

#' @keywords internal
from_env <- list(
    PIZZARR_HTTP_STORE_CACHE_TIME_SECONDS = as.integer,
    PIZZARR_NTHREADS = as.integer,
    PIZZARR_CONCURRENT_TARGET = as.integer,
    PIZZARR_HTTP_BATCH_RANGE_REQUESTS = function(x) as.logical(toupper(x))
)

# converts e.g. jupyter.log_level to JUPYTER_LOG_LEVEL
#' @keywords internal
opt_to_env <- function(nms) {
    gsub('.', '_', toupper(nms), fixed = TRUE)
}

# called in .onLoad
#' @keywords internal
init_options <- function() {
    for (opt_name in names(pizzarr_option_defaults)) {
        # skip option if it is already set, e.g. in the Rprofile
        if (is.null(getOption(opt_name))) {
            # prepare `options` call from the default
            call_arg <- pizzarr_option_defaults[opt_name]  # single [] preserve names

            # if an env var is set, get value from it.
            env_name <- opt_to_env(opt_name)
            convert <- from_env[[env_name]]
            env_val <- Sys.getenv(env_name, unset = NA)
            if (!is.null(convert) && !is.na(env_val)) {
                call_arg[[opt_name]] <- convert(env_val)
            }

            do.call(options, call_arg)
        }
    }
}
