library(pizzarr)

# --- opt_to_env ---

test_that("opt_to_env converts option names to env var names", {
  expect_equal(opt_to_env("pizzarr.http_store_cache_time_seconds"),
               "PIZZARR_HTTP_STORE_CACHE_TIME_SECONDS")
})

# --- pizzarr_option_defaults ---

test_that("pizzarr_option_defaults has expected keys and values", {
  expect_true("pizzarr.http_store_cache_time_seconds" %in% names(pizzarr_option_defaults))
  expect_true("pizzarr.nthreads" %in% names(pizzarr_option_defaults))
  expect_true("pizzarr.concurrent_target" %in% names(pizzarr_option_defaults))
  expect_true("pizzarr.http_batch_range_requests" %in% names(pizzarr_option_defaults))

  expect_equal(pizzarr_option_defaults$pizzarr.http_store_cache_time_seconds, 3600)
  expect_null(pizzarr_option_defaults$pizzarr.nthreads)
  expect_null(pizzarr_option_defaults$pizzarr.concurrent_target)
  expect_true(pizzarr_option_defaults$pizzarr.http_batch_range_requests)
})

# --- init_options sets defaults ---

test_that("init_options sets default option values", {
  saved <- options(
    pizzarr.http_store_cache_time_seconds = NULL
  )
  on.exit(do.call(options, saved), add = TRUE)

  init_options()

  expect_equal(getOption("pizzarr.http_store_cache_time_seconds"), 3600)
})

test_that("init_options does not overwrite pre-existing options", {
  saved <- options(
    pizzarr.http_store_cache_time_seconds = 999
  )
  on.exit(do.call(options, saved), add = TRUE)

  init_options()

  expect_equal(getOption("pizzarr.http_store_cache_time_seconds"), 999)
})

# --- init_options reads environment variables ---

test_that("init_options picks up env var for http_store_cache_time_seconds", {
  saved <- options(pizzarr.http_store_cache_time_seconds = NULL)
  on.exit(do.call(options, saved), add = TRUE)
  withr::local_envvar(PIZZARR_HTTP_STORE_CACHE_TIME_SECONDS = "120")

  init_options()

  expect_equal(getOption("pizzarr.http_store_cache_time_seconds"), 120L)
})

test_that("init_options env var does not override pre-existing option", {
  saved <- options(pizzarr.http_store_cache_time_seconds = 999)
  on.exit(do.call(options, saved), add = TRUE)
  withr::local_envvar(PIZZARR_HTTP_STORE_CACHE_TIME_SECONDS = "120")

  init_options()

  # pre-existing option wins over env var
  expect_equal(getOption("pizzarr.http_store_cache_time_seconds"), 999)
})

# --- init_options reads env vars for new options ---

test_that("init_options picks up PIZZARR_NTHREADS env var", {
  saved <- options(pizzarr.nthreads = NULL)
  on.exit(do.call(options, saved), add = TRUE)
  withr::local_envvar(PIZZARR_NTHREADS = "4")

  init_options()

  expect_equal(getOption("pizzarr.nthreads"), 4L)
})

test_that("init_options picks up PIZZARR_CONCURRENT_TARGET env var", {
  saved <- options(pizzarr.concurrent_target = NULL)
  on.exit(do.call(options, saved), add = TRUE)
  withr::local_envvar(PIZZARR_CONCURRENT_TARGET = "2")

  init_options()

  expect_equal(getOption("pizzarr.concurrent_target"), 2L)
})

test_that("init_options picks up PIZZARR_HTTP_BATCH_RANGE_REQUESTS env var", {
  saved <- options(pizzarr.http_batch_range_requests = NULL)
  on.exit(do.call(options, saved), add = TRUE)
  withr::local_envvar(PIZZARR_HTTP_BATCH_RANGE_REQUESTS = "FALSE")

  init_options()

  expect_false(getOption("pizzarr.http_batch_range_requests"))
})

test_that("init_options does not overwrite pre-existing nthreads option", {
  saved <- options(pizzarr.nthreads = 8L)
  on.exit(do.call(options, saved), add = TRUE)
  withr::local_envvar(PIZZARR_NTHREADS = "2")

  init_options()

  expect_equal(getOption("pizzarr.nthreads"), 8L)
})

# --- from_env converters ---

test_that("from_env converters produce expected types", {
  expect_equal(from_env$PIZZARR_HTTP_STORE_CACHE_TIME_SECONDS("42"), 42L)
  expect_equal(from_env$PIZZARR_NTHREADS("8"), 8L)
  expect_equal(from_env$PIZZARR_CONCURRENT_TARGET("4"), 4L)
  expect_true(from_env$PIZZARR_HTTP_BATCH_RANGE_REQUESTS("TRUE"))
  expect_false(from_env$PIZZARR_HTTP_BATCH_RANGE_REQUESTS("false"))
})
