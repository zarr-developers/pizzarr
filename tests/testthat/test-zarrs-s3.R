# --- zarrs S3 store tests ---
# These tests require network access and the s3 compiled feature.
# Skipped on CRAN and when offline.

test_that("s3 feature is compiled", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  features <- pizzarr_compiled_features()
  skip_if(!("s3" %in% features), "s3 feature not compiled")
  expect_true("s3" %in% features)
  expect_true("object_store" %in% features)
})

test_that("zarrs reads metadata from S3 store", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  skip_if(!("s3" %in% pizzarr_compiled_features()), "s3 feature not compiled")
  skip_on_cran()
  skip_if_offline()

  # OME-Zarr SciVis bonsai V2 (zstd, uint8, public, us-east-1)
  url <- "s3://ome-zarr-scivis/v0.4/64x0/bonsai.ome.zarr"

  meta <- zarrs_open_array_metadata(url, "scale0/bonsai")
  expect_equal(meta$zarr_format, 2L)
  expect_true(length(meta$shape) > 0)

  zarrs_close_store(url)
})

test_that("zarrs reads subset from S3 store", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  skip_if(!("s3" %in% pizzarr_compiled_features()), "s3 feature not compiled")
  skip_on_cran()
  skip_if_offline()

  url <- "s3://ome-zarr-scivis/v0.4/64x0/bonsai.ome.zarr"

  meta <- zarrs_open_array_metadata(url, "scale0/bonsai")
  ndim <- length(meta$shape)

  # Read a small subset: first element along each dimension
  ranges <- lapply(seq_len(ndim), function(i) c(0L, 1L))
  result <- zarrs_get_subset(url, "scale0/bonsai", ranges, NULL)
  expect_true(length(result$data) == 1)
  expect_equal(length(result$shape), ndim)

  zarrs_close_store(url)
})

test_that("S3Store class works", {
  s3 <- S3Store$new("s3://bucket/prefix")
  expect_equal(s3$get_store_identifier(), "s3://bucket/prefix")
  expect_false(s3$is_writeable())
  expect_output(print(s3), "<S3Store> s3://bucket/prefix")
  expect_error(S3Store$new("https://example.com"), "s3://")
})

test_that("can_use_zarrs_write returns FALSE for S3Store", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")

  s3 <- S3Store$new("s3://bucket/prefix")
  mock_indexer <- list(dim_indexers = list(
    structure(list(step = 1L), class = "SliceDimIndexer")
  ))
  class(mock_indexer) <- "BasicIndexer"

  expect_false(can_use_zarrs_write(mock_indexer, s3))
})
