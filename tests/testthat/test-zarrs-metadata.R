test_that("zarrs_open_array_metadata reads V2 integer array", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  d <- tempfile("zarrs_meta_v2i_")
  z <- zarr_create(store = d, shape = c(10L, 5L), chunks = c(5L, 5L), dtype = "<i4")
  z$set_item("...", array(1:50, dim = c(10, 5)))

  meta <- zarrs_open_array_metadata(d, "")
  expect_equal(meta$shape, c(10L, 5L))
  expect_equal(meta$chunks, c(5L, 5L))
  expect_equal(meta$r_type, "integer")
  expect_equal(meta$zarr_format, 2L)
  expect_equal(meta$order, "F")
  expect_true(grepl("int32", meta$dtype))

  zarrs_close_store(d)
  unlink(d, recursive = TRUE)
})

test_that("zarrs_open_array_metadata reads V3 float64 array", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  d <- tempfile("zarrs_meta_v3f_")
  store <- DirectoryStore$new(d)
  g <- zarr_create_group(store = store, zarr_format = 3L)
  a <- g$create_dataset("arr", shape = 6L, dtype = "<f8", compressor = NA)
  a$set_item("...", as.double(1:6))

  meta <- zarrs_open_array_metadata(d, "arr")
  expect_equal(meta$shape, 6L)
  expect_equal(meta$r_type, "double")
  expect_equal(meta$zarr_format, 3L)
  expect_equal(meta$order, "C")
  expect_true(grepl("float64", meta$dtype))

  zarrs_close_store(d)
  unlink(d, recursive = TRUE)
})

test_that("zarrs_open_array_metadata reads 3D array", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  d <- tempfile("zarrs_meta_3d_")
  z <- zarr_create(store = d, shape = c(4L, 6L, 8L), chunks = c(2L, 3L, 4L), dtype = "<f8")

  meta <- zarrs_open_array_metadata(d, "")
  expect_equal(meta$shape, c(4L, 6L, 8L))
  expect_equal(meta$chunks, c(2L, 3L, 4L))
  expect_equal(meta$r_type, "double")
  expect_equal(meta$zarr_format, 2L)

  zarrs_close_store(d)
  unlink(d, recursive = TRUE)
})

test_that("zarrs_open_array_metadata errors on nonexistent path", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  d <- tempfile("zarrs_meta_err_")
  dir.create(d)

  expect_error(zarrs_open_array_metadata(d, "nope"), "ArrayOpen")

  zarrs_close_store(d)
  unlink(d, recursive = TRUE)
})

test_that("zarrs_runtime_info returns expected fields", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  info <- zarrs_runtime_info()
  expect_true(is.list(info))
  expect_true("codec_concurrent_target" %in% names(info))
  expect_true("nthreads" %in% names(info))
  expect_true("store_cache_entries" %in% names(info))
  expect_true("compiled_features" %in% names(info))
  expect_true(info$codec_concurrent_target > 0L)
  expect_true(info$nthreads > 0L)
})

test_that("zarrs_set_codec_concurrent_target validates input", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  expect_error(zarrs_set_codec_concurrent_target(0L), "positive")
  expect_error(zarrs_set_codec_concurrent_target(-1L), "positive")
})

test_that("zarrs_set_codec_concurrent_target round-trips", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  zarrs_set_codec_concurrent_target(2L)
  info <- zarrs_runtime_info()
  expect_equal(info$codec_concurrent_target, 2L)
})
