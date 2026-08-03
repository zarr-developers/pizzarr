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

test_that("zarrs_get_key reads consolidated metadata from an S3 store", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  skip_if(!("s3" %in% pizzarr_compiled_features()), "s3 feature not compiled")
  skip_on_cran()
  skip_if_offline()

  # USGS gridMET on an Open Storage Network pod: public, alternate endpoint,
  # and consolidated. S3Store has no key-level I/O of its own, so this is the
  # only route to `.zmetadata` for an s3:// URL.
  url <- "s3://mdmf/gdp/gridMET.zarr"

  zarrs_close_store(url)
  withr::defer(zarrs_close_store(url))

  withr::with_envvar(c(AWS_ENDPOINT = "https://usgs.osn.mghpcc.org"), {
    raw_meta <- zarrs_get_key(url, ".zmetadata")

    expect_type(raw_meta, "raw")

    meta <- try_fromJSON(rawToChar(raw_meta), simplifyVector = FALSE)
    expect_equal(meta$zarr_consolidated_format, 1)

    # the blob carries both .zarray and .zattrs for every array, which is
    # what makes group-level inquiry possible over s3://
    expect_true("lat/.zarray" %in% names(meta$metadata))
    expect_equal(meta$metadata[["lat/.zattrs"]][["_ARRAY_DIMENSIONS"]][[1]],
                 "lat")

    # absent keys are NULL rather than an error
    expect_null(zarrs_get_key(url, ".does-not-exist"))
  })
})

test_that("zarrs_get_key rejects an invalid store key", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")

  expect_error(zarrs_get_key(tempdir(), "//not//a//key//"))
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

test_that("cloud URLs resolve to cloud store classes", {
  expect_s3_class(normalize_store_arg("s3://bucket/prefix"), "S3Store")
  expect_s3_class(normalize_store_arg("gs://bucket/prefix"), "GcsStore")
  # Existing coercions are unchanged.
  expect_s3_class(normalize_store_arg("https://example.com/x.zarr"), "HttpStore")
  expect_s3_class(normalize_store_arg(tempdir()), "DirectoryStore")
})

test_that("S3Store key-level methods error with guidance", {
  s3 <- S3Store$new("s3://bucket/prefix")

  for (method in c("get_item", "contains_item")) {
    expect_error(s3[[method]]("key"), "not implemented")
    expect_error(s3[[method]]("key"), "zarrs_get_subset")
  }
  expect_error(s3$listdir(), "not implemented")
  expect_error(s3$set_item("key", raw(1)), "not implemented")
})

test_that("GcsStore key-level methods error with guidance", {
  gcs <- GcsStore$new("gs://bucket/prefix")

  expect_error(gcs$get_item("key"), "not implemented")
  expect_error(gcs$contains_item("key"), "zarrs_get_subset")
  expect_error(gcs$listdir(), "not implemented")
})

test_that("zarr_open on an s3:// URL errors without touching the filesystem", {
  wd <- withr::local_tempdir()
  withr::local_dir(wd)

  expect_error(zarr_open("s3://bucket/does-not-exist.zarr"), "not implemented")
  # The old behaviour created a DirectoryStore named after the URL.
  expect_length(list.files(wd, all.files = TRUE, no.. = TRUE), 0)
})

test_that("public S3 buckets read anonymously with no credentials set", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  skip_if(!("s3" %in% pizzarr_compiled_features()), "s3 feature not compiled")
  skip_on_cran()
  skip_if_offline()

  url <- "s3://ome-zarr-scivis/v0.4/64x0/bonsai.ome.zarr"

  # Store handles are cached by URL, so clear before and after changing the
  # environment the client reads at open time.
  zarrs_close_store(url)
  withr::defer(zarrs_close_store(url))

  withr::with_envvar(
    c(AWS_ACCESS_KEY_ID = NA, AWS_SECRET_ACCESS_KEY = NA, AWS_SESSION_TOKEN = NA,
      AWS_PROFILE = NA, AWS_SKIP_SIGNATURE = NA),
    {
      meta <- zarrs_open_array_metadata(url, "scale0/bonsai")
      expect_equal(meta$zarr_format, 2L)
    }
  )
})

test_that("credentials in the environment produce signed requests", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  skip_if(!("s3" %in% pizzarr_compiled_features()), "s3 feature not compiled")
  skip_on_cran()
  skip_if_offline()

  url <- "s3://ome-zarr-scivis/v0.4/64x0/bonsai.ome.zarr"

  zarrs_close_store(url)
  withr::defer(zarrs_close_store(url))

  # Bogus credentials must be used rather than ignored: an unsigned request
  # would succeed against this public bucket, a signed one must not.
  withr::with_envvar(
    c(AWS_ACCESS_KEY_ID = "AKIAIOSFODNN7EXAMPLE",
      AWS_SECRET_ACCESS_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
      AWS_SESSION_TOKEN = NA, AWS_PROFILE = NA, AWS_SKIP_SIGNATURE = NA),
    expect_error(zarrs_open_array_metadata(url, "scale0/bonsai"))
  )
})

test_that("AWS_SKIP_SIGNATURE forces anonymous access despite credentials", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  skip_if(!("s3" %in% pizzarr_compiled_features()), "s3 feature not compiled")
  skip_on_cran()
  skip_if_offline()

  url <- "s3://ome-zarr-scivis/v0.4/64x0/bonsai.ome.zarr"

  zarrs_close_store(url)
  withr::defer(zarrs_close_store(url))

  withr::with_envvar(
    c(AWS_ACCESS_KEY_ID = "AKIAIOSFODNN7EXAMPLE",
      AWS_SECRET_ACCESS_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
      AWS_SKIP_SIGNATURE = "true"),
    {
      meta <- zarrs_open_array_metadata(url, "scale0/bonsai")
      expect_equal(meta$zarr_format, 2L)
    }
  )
})

test_that("AWS_ENDPOINT reaches an S3-compatible store", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  skip_if(!("s3" %in% pizzarr_compiled_features()), "s3 feature not compiled")
  skip_if(!("blosc" %in% pizzarr_compiled_features()), "blosc feature not compiled")
  skip_on_cran()
  skip_if_offline()

  # USGS gridMET on an Open Storage Network pod: public, non-AWS endpoint.
  url <- "s3://mdmf/gdp/gridMET.zarr"

  zarrs_close_store(url)
  withr::defer(zarrs_close_store(url))

  withr::with_envvar(
    c(AWS_ENDPOINT = "https://usgs.osn.mghpcc.org",
      AWS_ACCESS_KEY_ID = NA, AWS_SECRET_ACCESS_KEY = NA, AWS_SKIP_SIGNATURE = NA),
    {
      result <- zarrs_get_subset(url, "lat", list(c(0L, 5L)), NULL)
      expect_length(result$data, 5)
      expect_equal(result$data[1], 49.4, tolerance = 1e-6)
    }
  )
})
