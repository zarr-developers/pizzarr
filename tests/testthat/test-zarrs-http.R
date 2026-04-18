# --- zarrs HTTP store tests ---
# These tests require network access and the http_sync compiled feature.
# Skipped on CRAN and when offline.

test_that("http_sync feature is compiled", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  features <- pizzarr_compiled_features()
  expect_true("http_sync" %in% features)
})

test_that("zarrs reads from HTTPS store", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  skip_if(!("http_sync" %in% pizzarr_compiled_features()),
          "http_sync feature not compiled")
  skip_on_cran()
  skip_if_offline()

  url <- "https://raw.githubusercontent.com/DOI-USGS/rnz/main/inst/extdata/bcsd.zarr"

  # Open array metadata via zarrs
  meta <- zarrs_open_array_metadata(url, "pr")
  expect_equal(meta$zarr_format, 2L)
  expect_true(length(meta$shape) > 0)

  # Read a small subset: first element along each dimension
  ndim <- length(meta$shape)
  ranges <- lapply(seq_len(ndim), function(i) c(0L, 1L))
  result <- zarrs_get_subset(url, "pr", ranges, NULL)
  expect_true(length(result$data) == 1)
  expect_equal(length(result$shape), ndim)

  zarrs_close_store(url)
})

test_that("zarrs HTTP read matches R-native read", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  skip_if(!("http_sync" %in% pizzarr_compiled_features()),
          "http_sync feature not compiled")
  skip_on_cran()
  skip_if_offline()
  skip_if_not_installed("crul")

  url <- "https://raw.githubusercontent.com/DOI-USGS/rnz/main/inst/extdata/bcsd.zarr"

  # Read via zarrs
  z_zarrs <- zarr_open(store = HttpStore$new(url))
  arr_zarrs <- z_zarrs$get_item("pr")

  # Read via R-native
  old <- .pizzarr_env$zarrs_available
  .pizzarr_env$zarrs_available <- FALSE
  z_native <- zarr_open(store = HttpStore$new(url))
  arr_native <- z_native$get_item("pr")
  .pizzarr_env$zarrs_available <- old

  expect_equal(arr_zarrs$data, arr_native$data)

  zarrs_close_store(url)
})

test_that("can_use_zarrs_write returns FALSE for HttpStore", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  skip_if_not_installed("crul")

  hs <- HttpStore$new("https://example.com/fake.zarr")
  # Create a minimal BasicIndexer-like object for the check
  mock_indexer <- list(dim_indexers = list(
    structure(list(step = 1L), class = "SliceDimIndexer")
  ))
  class(mock_indexer) <- "BasicIndexer"

  expect_false(can_use_zarrs_write(mock_indexer, hs))
})
