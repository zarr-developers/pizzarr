# --- zarrs GCS store tests ---
# Tests for GCS-hosted data and the GcsStore class.
# The gs:// protocol requires GCP credentials; public GCS data is
# tested via HTTPS (storage.googleapis.com) which exercises the
# same blosc codec path.

test_that("gcs feature is compiled", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  features <- pizzarr_compiled_features()
  skip_if(!("gcs" %in% features), "gcs feature not compiled")
  expect_true("gcs" %in% features)
  expect_true("object_store" %in% features)
})

test_that("zarrs reads blosc-compressed GCS data via HTTPS", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  skip_if(!("blosc" %in% pizzarr_compiled_features()), "blosc feature not compiled")
  skip_on_cran()
  skip_if_offline()

  # Pangeo ECCO_basins via public HTTPS endpoint (blosc/lz4 compressed)
  url <- "https://storage.googleapis.com/pangeo-data/ECCO_basins.zarr"

  meta <- zarrs_open_array_metadata(url, "basin_mask")
  expect_equal(meta$zarr_format, 2L)
  expect_true(length(meta$shape) > 0)

  # Read a small subset
  ndim <- length(meta$shape)
  ranges <- lapply(seq_len(ndim), function(i) c(0L, 1L))
  result <- zarrs_get_subset(url, "basin_mask", ranges, NULL)
  expect_true(length(result$data) == 1)
  expect_equal(length(result$shape), ndim)

  zarrs_close_store(url)
})

test_that("zarrs reads from gs:// with credentials", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  skip_if(!("gcs" %in% pizzarr_compiled_features()), "gcs feature not compiled")
  skip_on_cran()
  skip_if_offline()
  # gs:// requires GCP credentials (env vars or application default)
  skip_if(!nzchar(Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS")) &&
          !nzchar(Sys.getenv("GOOGLE_SERVICE_ACCOUNT")),
          "no GCP credentials configured")

  url <- "gs://pangeo-data/ECCO_basins.zarr"

  meta <- zarrs_open_array_metadata(url, "basin_mask")
  expect_equal(meta$zarr_format, 2L)

  ndim <- length(meta$shape)
  ranges <- lapply(seq_len(ndim), function(i) c(0L, 1L))
  result <- zarrs_get_subset(url, "basin_mask", ranges, NULL)
  expect_true(length(result$data) == 1)

  zarrs_close_store(url)
})

test_that("GcsStore class works", {
  gcs <- GcsStore$new("gs://bucket/prefix")
  expect_equal(gcs$get_store_identifier(), "gs://bucket/prefix")
  expect_false(gcs$is_writeable())
  expect_output(print(gcs), "<GcsStore> gs://bucket/prefix")
  expect_error(GcsStore$new("https://example.com"), "gs://")
})

test_that("can_use_zarrs_write returns FALSE for GcsStore", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")

  gcs <- GcsStore$new("gs://bucket/prefix")
  mock_indexer <- list(dim_indexers = list(
    structure(list(step = 1L), class = "SliceDimIndexer")
  ))
  class(mock_indexer) <- "BasicIndexer"

  expect_false(can_use_zarrs_write(mock_indexer, gcs))
})
