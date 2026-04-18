# Tests for zarrs_create_array — Rust-side array creation.
# All tests gated on zarrs availability (skip on CRAN / pure-R tier).

skip_if_no_zarrs <- function() {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
}

# -- V3 creation tests -------------------------------------------------------

test_that("zarrs_create_array V3 float64 no compression", {
  skip_if_no_zarrs()
  d <- tempfile("zarrs_create_v3_none_")
  dir.create(d)
  on.exit(unlink(d, recursive = TRUE))

  meta <- zarrs_create_array(d, "", c(10L, 20L), c(5L, 10L),
                              "float64", "none", 0.0, "{}", 3L)
  expect_equal(meta$shape, c(10L, 20L))
  expect_equal(meta$chunks, c(5L, 10L))
  expect_equal(meta$dtype, "float64")
  expect_equal(meta$r_type, "double")
  expect_equal(meta$zarr_format, 3L)

  # Metadata file should exist
  expect_true(file.exists(file.path(d, "zarr.json")))
  zarrs_close_store(d)
})

test_that("zarrs_create_array V3 float64 gzip roundtrip", {
  skip_if_no_zarrs()
  d <- tempfile("zarrs_create_v3_gzip_")
  dir.create(d)
  on.exit(unlink(d, recursive = TRUE))

  zarrs_create_array(d, "", 10L, 5L, "float64", "gzip", 0.0, "{}", 3L)

  # Write and read back
  zarrs_set_subset(d, "", list(c(0L, 10L)), as.double(1:10), NULL)
  result <- zarrs_get_subset(d, "", list(c(0L, 10L)), NULL)
  expect_equal(result$data, as.double(1:10))
  zarrs_close_store(d)
})

test_that("zarrs_create_array V3 int32 zstd roundtrip", {
  skip_if_no_zarrs()
  skip_if(!("zstd" %in% pizzarr_compiled_features()),
          "zstd not compiled")
  d <- tempfile("zarrs_create_v3_zstd_")
  dir.create(d)
  on.exit(unlink(d, recursive = TRUE))

  zarrs_create_array(d, "", 10L, 5L, "int32", "zstd", 0L, "{}", 3L)

  zarrs_set_subset(d, "", list(c(0L, 10L)), 1:10, NULL)
  result <- zarrs_get_subset(d, "", list(c(0L, 10L)), NULL)
  expect_equal(result$data, 1:10)
  zarrs_close_store(d)
})

# -- V2 creation tests -------------------------------------------------------

test_that("zarrs_create_array V2 float64 no compression", {
  skip_if_no_zarrs()
  d <- tempfile("zarrs_create_v2_none_")
  dir.create(d)
  on.exit(unlink(d, recursive = TRUE))

  meta <- zarrs_create_array(d, "", c(10L, 20L), c(5L, 10L),
                              "float64", "none", 0.0, "{}", 2L)
  expect_equal(meta$shape, c(10L, 20L))
  expect_equal(meta$chunks, c(5L, 10L))
  expect_equal(meta$dtype, "float64")
  expect_equal(meta$zarr_format, 2L)

  # V2 metadata file should exist
  expect_true(file.exists(file.path(d, ".zarray")))
  zarrs_close_store(d)
})

test_that("zarrs_create_array V2 float64 gzip roundtrip", {
  skip_if_no_zarrs()
  d <- tempfile("zarrs_create_v2_gzip_")
  dir.create(d)
  on.exit(unlink(d, recursive = TRUE))

  zarrs_create_array(d, "", 10L, 5L, "float64", "gzip", 0.0, "{}", 2L)

  zarrs_set_subset(d, "", list(c(0L, 10L)), as.double(1:10), NULL)
  result <- zarrs_get_subset(d, "", list(c(0L, 10L)), NULL)
  expect_equal(result$data, as.double(1:10))
  zarrs_close_store(d)
})

test_that("zarrs_create_array V2 zstd rejected", {
  skip_if_no_zarrs()
  d <- tempfile("zarrs_create_v2_zstd_")
  dir.create(d)
  on.exit(unlink(d, recursive = TRUE))

  expect_error(
    zarrs_create_array(d, "", 10L, 5L, "float64", "zstd", 0.0, "{}", 2L),
    "zstd.*zarr_format"
  )
  zarrs_close_store(d)
})

# -- All dtypes ---------------------------------------------------------------

test_that("zarrs_create_array V3 supports all numeric dtypes", {
  skip_if_no_zarrs()

  dtypes <- c("float64", "float32", "int32", "int16", "int8",
              "uint8", "uint16", "uint32", "int64", "uint64", "bool")

  for (dt in dtypes) {
    d <- tempfile(paste0("zarrs_create_dtype_", dt, "_"))
    dir.create(d)

    meta <- zarrs_create_array(d, "", 10L, 5L, dt, "none", 0L, "{}", 3L)
    expect_equal(meta$dtype, dt, info = paste("dtype:", dt))
    expect_true(meta$r_type %in% c("double", "integer", "logical"),
                info = paste("dtype:", dt))

    zarrs_close_store(d)
    unlink(d, recursive = TRUE)
  }
})

# -- End-to-end roundtrip -----------------------------------------------------

test_that("zarrs create + write + read full roundtrip", {
  skip_if_no_zarrs()
  d <- tempfile("zarrs_roundtrip_")
  dir.create(d)
  on.exit(unlink(d, recursive = TRUE))

  # Create 2D V3 array with gzip
  zarrs_create_array(d, "", c(4L, 6L), c(2L, 3L),
                      "float64", "gzip", 0.0, "{}", 3L)

  # Write a subset
  zarrs_set_subset(d, "", list(c(0L, 4L), c(0L, 6L)),
                   as.double(1:24), NULL)

  # Read back
  result <- zarrs_get_subset(d, "", list(c(0L, 4L), c(0L, 6L)), NULL)
  expect_equal(result$data, as.double(1:24))
  expect_equal(result$shape, c(4L, 6L))

  zarrs_close_store(d)
})

# -- Transparent dispatch via zarr_create() -----------------------------------

test_that("zarr_create transparently uses zarrs backend for DirectoryStore", {
  skip_if_no_zarrs()
  d <- tempfile("zarrs_dispatch_")
  on.exit(unlink(d, recursive = TRUE))

  z <- zarr_create(shape = c(10L, 20L), chunks = c(5L, 10L),
                   dtype = "<f8", store = d, zarr_format = 3L)

  expect_s3_class(z, "ZarrArray")
  # V3 metadata should exist
  expect_true(file.exists(file.path(d, "zarr.json")))
})

test_that("zarr_create V2 works via zarrs dispatch", {
  skip_if_no_zarrs()
  d <- tempfile("zarrs_dispatch_v2_")
  on.exit(unlink(d, recursive = TRUE))

  z <- zarr_create(shape = 10L, chunks = 5L, dtype = "<f8",
                   store = d, zarr_format = 2L)

  expect_s3_class(z, "ZarrArray")
  expect_true(file.exists(file.path(d, ".zarray")))
})

# -- R-native fallback --------------------------------------------------------

test_that("zarr_create falls back for MemoryStore", {
  z <- zarr_create(shape = 10L, chunks = 5L, dtype = "<f8")
  expect_s3_class(z, "ZarrArray")
})

# -- Fill value handling ------------------------------------------------------

test_that("zarrs_create_array fill value 0 roundtrip", {
  skip_if_no_zarrs()
  d <- tempfile("zarrs_fv0_")
  dir.create(d)
  on.exit(unlink(d, recursive = TRUE))

  zarrs_create_array(d, "", 5L, 5L, "float64", "none", 0.0, "{}", 3L)
  # Read uninitialized data — should be fill value
  result <- zarrs_get_subset(d, "", list(c(0L, 5L)), NULL)
  expect_equal(result$data, rep(0.0, 5))
  zarrs_close_store(d)
})

test_that("zarrs_create_array fill value 42 roundtrip", {
  skip_if_no_zarrs()
  d <- tempfile("zarrs_fv42_")
  dir.create(d)
  on.exit(unlink(d, recursive = TRUE))

  zarrs_create_array(d, "", 5L, 5L, "float64", "none", 42.0, "{}", 3L)
  result <- zarrs_get_subset(d, "", list(c(0L, 5L)), NULL)
  expect_equal(result$data, rep(42.0, 5))
  zarrs_close_store(d)
})

test_that("zarrs_create_array fill value NA maps to NaN for floats", {
  skip_if_no_zarrs()
  d <- tempfile("zarrs_fvna_")
  dir.create(d)
  on.exit(unlink(d, recursive = TRUE))

  zarrs_create_array(d, "", 5L, 5L, "float64", "none", NA, "{}", 3L)
  result <- zarrs_get_subset(d, "", list(c(0L, 5L)), NULL)
  expect_true(all(is.nan(result$data)))
  zarrs_close_store(d)
})

# -- Cross-read: zarrs-created, R-native read ---------------------------------

test_that("zarrs-created V3 array readable by R-native path", {
  skip_if_no_zarrs()
  d <- tempfile("zarrs_crossread_")
  dir.create(d)
  on.exit(unlink(d, recursive = TRUE))

  # Create and populate via zarrs
  zarrs_create_array(d, "", 10L, 5L, "float64", "gzip", 0.0, "{}", 3L)
  zarrs_set_subset(d, "", list(c(0L, 10L)), as.double(1:10), NULL)
  zarrs_close_store(d)

  # Read via R-native path (open DirectoryStore, read normally)
  store <- DirectoryStore$new(d)
  z <- ZarrArray$new(store)
  data <- z$get_item("...")$data
  expect_equal(as.vector(data), as.double(1:10))
})

# -- Nested path with parent groups -------------------------------------------

test_that("zarrs_create_array with nested path via zarr_create", {
  skip_if_no_zarrs()
  d <- tempfile("zarrs_nested_")
  on.exit(unlink(d, recursive = TRUE))

  z <- zarr_create(shape = 10L, chunks = 5L, dtype = "<f8",
                   store = d, path = "group1/array1",
                   zarr_format = 3L)
  expect_s3_class(z, "ZarrArray")

  # Parent group metadata should exist
  expect_true(file.exists(file.path(d, "group1", "zarr.json")))
  # Array metadata should exist
  expect_true(file.exists(file.path(d, "group1", "array1", "zarr.json")))
})

# -- Error cases --------------------------------------------------------------

test_that("zarrs_create_array rejects unsupported dtype", {
  skip_if_no_zarrs()
  d <- tempfile("zarrs_bad_dtype_")
  dir.create(d)
  on.exit(unlink(d, recursive = TRUE))

  expect_error(
    zarrs_create_array(d, "", 10L, 5L, "string", "none", "", "{}", 3L),
    "DTypeUnsupported"
  )
  zarrs_close_store(d)
})

test_that("zarrs_create_array rejects shape/chunks length mismatch", {
  skip_if_no_zarrs()
  d <- tempfile("zarrs_mismatch_")
  dir.create(d)
  on.exit(unlink(d, recursive = TRUE))

  expect_error(
    zarrs_create_array(d, "", c(10L, 20L), 5L, "float64", "none", 0.0, "{}", 3L),
    "shape length.*chunks length"
  )
  zarrs_close_store(d)
})
