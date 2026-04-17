library(pizzarr)

has_zarrs <- .pizzarr_env$zarrs_available

# --- pizzarr_config() getter ---

test_that("pizzarr_config() returns named list with expected keys", {
  cfg <- pizzarr_config()
  expect_true(is.list(cfg))
  expect_true("nthreads" %in% names(cfg))
  expect_true("concurrent_target" %in% names(cfg))
  expect_true("http_batch_range_requests" %in% names(cfg))
})

test_that("pizzarr_config() nthreads is positive integer when zarrs available", {
  skip_if(!has_zarrs, "zarrs backend not available")
  cfg <- pizzarr_config()
  expect_true(is.integer(cfg$nthreads))
  expect_true(cfg$nthreads > 0L)
})

# --- pizzarr_config() setter: concurrent_target ---

test_that("pizzarr_config(concurrent_target) round-trips via zarrs", {
  skip_if(!has_zarrs, "zarrs backend not available")

  # Save and restore original
  orig <- zarrs_runtime_info()$codec_concurrent_target
  on.exit({
    zarrs_set_codec_concurrent_target(orig)
    options(pizzarr.concurrent_target = NULL)
  }, add = TRUE)

  prev <- pizzarr_config(concurrent_target = 2L)
  expect_equal(zarrs_runtime_info()$codec_concurrent_target, 2L)

  # Returns previous value invisibly
  expect_true(is.list(prev))
})

# --- pizzarr_config() setter: http_batch_range_requests ---

test_that("pizzarr_config(http_batch_range_requests) sets R option", {
  saved <- options(pizzarr.http_batch_range_requests = TRUE)
  on.exit(do.call(options, saved), add = TRUE)

  pizzarr_config(http_batch_range_requests = FALSE)
  expect_false(getOption("pizzarr.http_batch_range_requests"))

  pizzarr_config(http_batch_range_requests = TRUE)
  expect_true(getOption("pizzarr.http_batch_range_requests"))
})

# --- zarrs_runtime_info includes nthreads ---

test_that("zarrs_runtime_info includes nthreads field", {
  skip_if(!has_zarrs, "zarrs backend not available")
  info <- zarrs_runtime_info()
  expect_true("nthreads" %in% names(info))
  expect_true(info$nthreads > 0L)
})

# --- zarrs_set_nthreads validates input ---

test_that("zarrs_set_nthreads rejects non-positive input", {
  skip_if(!has_zarrs, "zarrs backend not available")
  expect_error(zarrs_set_nthreads(0L), "positive")
  expect_error(zarrs_set_nthreads(-1L), "positive")
})

# --- pure-R tier ---

test_that("pizzarr_config works without zarrs backend", {
  # Temporarily pretend zarrs is unavailable
  orig <- .pizzarr_env$zarrs_available
  on.exit(.pizzarr_env$zarrs_available <- orig, add = TRUE)
  .pizzarr_env$zarrs_available <- FALSE

  saved <- options(pizzarr.concurrent_target = NULL)
  on.exit(do.call(options, saved), add = TRUE)

  expect_message(
    pizzarr_config(concurrent_target = 3L),
    "not available"
  )
  expect_equal(getOption("pizzarr.concurrent_target"), 3L)
})
