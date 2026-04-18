# Tests for zarrs bridge helpers (R/zarrs-bridge.R)

test_that("is_zarrs_available returns logical", {
  expect_true(is.logical(is_zarrs_available()))
})

test_that("pizzarr_compiled_features returns character when available", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  feats <- pizzarr_compiled_features()
  expect_true(is.character(feats))
  expect_true(length(feats) > 0)
})

test_that("pizzarr_compiled_features messages when unavailable", {
  orig <- .pizzarr_env$zarrs_available
  on.exit(.pizzarr_env$zarrs_available <- orig)
  .pizzarr_env$zarrs_available <- FALSE

  expect_message(feats <- pizzarr_compiled_features(), "not available")
  expect_equal(feats, character(0))
})

test_that("pizzarr_upgrade messages already available when zarrs present", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  expect_message(pizzarr_upgrade(), "already available")
})

test_that("pizzarr_upgrade shows install instructions when zarrs absent", {
  orig <- .pizzarr_env$zarrs_available
  on.exit(.pizzarr_env$zarrs_available <- orig)
  .pizzarr_env$zarrs_available <- FALSE

  expect_message(pizzarr_upgrade(), "r-universe")
})

test_that("pizzarr_upgrade returns NULL invisibly", {
  result <- pizzarr_upgrade()
  expect_null(result)
})
