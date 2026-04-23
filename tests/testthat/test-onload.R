# Tests for .onAttach startup message (R/onload.R)

test_that(".onAttach shows message when zarrs unavailable", {
  orig <- .pizzarr_env$zarrs_available
  on.exit(.pizzarr_env$zarrs_available <- orig)
  .pizzarr_env$zarrs_available <- FALSE

  saved <- options(pizzarr.suggest_runiverse = TRUE)
  on.exit(do.call(options, saved), add = TRUE)

  expect_message(.onAttach(NULL, "pizzarr"), "without zarrs backend")
})

test_that(".onAttach suppressed when suggest_runiverse is FALSE", {
  orig <- .pizzarr_env$zarrs_available
  on.exit(.pizzarr_env$zarrs_available <- orig)
  .pizzarr_env$zarrs_available <- FALSE

  saved <- options(pizzarr.suggest_runiverse = FALSE)
  on.exit(do.call(options, saved), add = TRUE)

  expect_silent(.onAttach(NULL, "pizzarr"))
})

test_that(".onAttach silent when zarrs is available", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  expect_silent(.onAttach(NULL, "pizzarr"))
})
