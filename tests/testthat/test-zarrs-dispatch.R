# Tests for zarrs dispatch helpers (R/zarrs-dispatch.R)

# --- can_use_zarrs_create() ---

test_that("can_use_zarrs_create returns FALSE when zarrs unavailable", {
  orig <- .pizzarr_env$zarrs_available
  on.exit(.pizzarr_env$zarrs_available <- orig)
  .pizzarr_env$zarrs_available <- FALSE

  d <- tempfile()
  dir.create(d)
  on.exit(unlink(d, recursive = TRUE), add = TRUE)
  ds <- DirectoryStore$new(d)
  dtype <- Dtype$new("<f8")
  expect_false(can_use_zarrs_create(ds, dtype))
})

test_that("can_use_zarrs_create returns FALSE for MemoryStore", {
  ms <- MemoryStore$new()
  dtype <- Dtype$new("<f8")
  expect_false(can_use_zarrs_create(ms, dtype))
})

test_that("can_use_zarrs_create returns FALSE for HttpStore", {
  hs <- HttpStore$new("http://example.com")
  dtype <- Dtype$new("<f8")
  expect_false(can_use_zarrs_create(hs, dtype))
})

test_that("can_use_zarrs_create returns FALSE for S3Store", {
  s3 <- S3Store$new("s3://bucket/prefix")
  dtype <- Dtype$new("<f8")
  expect_false(can_use_zarrs_create(s3, dtype))
})

test_that("can_use_zarrs_create returns FALSE for GcsStore", {
  gcs <- GcsStore$new("gs://bucket/prefix")
  dtype <- Dtype$new("<f8")
  expect_false(can_use_zarrs_create(gcs, dtype))
})

test_that("can_use_zarrs_create returns FALSE for object dtype", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  d <- tempfile()
  dir.create(d)
  on.exit(unlink(d, recursive = TRUE))
  ds <- DirectoryStore$new(d)
  dtype <- Dtype$new("|O")
  expect_false(can_use_zarrs_create(ds, dtype))
})

test_that("can_use_zarrs_create returns FALSE for string dtype", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  d <- tempfile()
  dir.create(d)
  on.exit(unlink(d, recursive = TRUE))
  ds <- DirectoryStore$new(d)
  dtype <- Dtype$new("<U10")
  expect_false(can_use_zarrs_create(ds, dtype))
})

test_that("can_use_zarrs_create returns TRUE for numeric DirectoryStore", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  d <- tempfile()
  dir.create(d)
  on.exit(unlink(d, recursive = TRUE))
  ds <- DirectoryStore$new(d)
  dtype <- Dtype$new("<f8")
  expect_true(can_use_zarrs_create(ds, dtype))
})

# --- compressor_to_preset() ---

test_that("compressor_to_preset returns 'none' for NA", {
  expect_equal(compressor_to_preset(NA, NA), "none")
})

test_that("compressor_to_preset returns 'none' for NULL", {
  expect_equal(compressor_to_preset(NULL, NULL), "none")
})

test_that("compressor_to_preset maps zlib to gzip", {
  expect_equal(compressor_to_preset("x", list(id = "zlib")), "gzip")
})

test_that("compressor_to_preset maps gzip to gzip", {
  expect_equal(compressor_to_preset("x", list(id = "gzip")), "gzip")
})

test_that("compressor_to_preset maps blosc to blosc", {
  expect_equal(compressor_to_preset("x", list(id = "blosc")), "blosc")
})

test_that("compressor_to_preset maps zstd to zstd", {
  expect_equal(compressor_to_preset("x", list(id = "zstd")), "zstd")
})

test_that("compressor_to_preset returns NA for unknown compressor", {
  result <- compressor_to_preset("x", list(id = "lz4"))
  expect_true(is.na(result))
})

# --- can_use_zarrs with step > 1 ---

test_that("can_use_zarrs returns FALSE for step > 1 slice", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  d <- tempfile()
  on.exit(unlink(d, recursive = TRUE))
  z <- zarr_create(store = d, shape = 10L, chunks = 5L, dtype = "<f8")
  ds <- DirectoryStore$new(d)
  indexer <- BasicIndexer$new(list(slice(1L, 10L, 2L)), z)
  expect_false(can_use_zarrs(indexer, ds))
})
