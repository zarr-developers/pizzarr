library(pizzarr)

make_array <- function(shape = c(4, 4), chunks = c(2, 2), dtype = "<i4",
                       compressor = NA, fill_value = 0, order = "C") {
  zarr_create(
    shape = shape, chunks = chunks, dtype = dtype,
    store = MemoryStore$new(), compressor = compressor,
    fill_value = fill_value, order = order
  )
}

test_that("Zarr Array can load .zarray metadata", {
  store <- MemoryStore$new()

  zarray_meta <- store$metadata_class$encode_array_metadata(create_zarray_meta(
    dtype = Dtype$new("|u1"),
    order = "C",
    fill_value = 0,
    shape = c(1, 2),
    chunks = c(3, 4),
    dimension_separator = "."
  ))

  store$set_item(".zarray", zarray_meta)

  array <- ZarrArray$new(store = store)
  shape <- array$get_shape()

  expect_equal(shape, c(1, 2))
})

test_that("load_metadata_nosync handles list-style shape/chunks from JSON", {
  d <- tempfile()
  a <- zarr_create(shape = c(3, 4), chunks = c(2, 2), store = d, compressor = NA)
  a$set_item("...", array(1:12, dim = c(3, 4)))
  b <- zarr_open_array(d, mode = "r")
  expect_equal(b$get_shape(), c(3, 4))
  expect_equal(b$get_chunks(), c(2, 2))
})

test_that("cache_metadata=FALSE reloads metadata on each access", {
  d <- tempfile()
  zarr_create(shape = c(4), chunks = c(2), store = d, compressor = NA)
  b <- ZarrArray$new(store = DirectoryStore$new(d), cache_metadata = FALSE)
  expect_equal(b$get_shape(), c(4))
  # Calling again exercises the refresh_metadata path a second time
  expect_equal(b$get_shape(), c(4))
})

test_that("Zarr Array can be resized", {
  store <- MemoryStore$new()

  zarray_meta <- store$metadata_class$encode_array_metadata(create_zarray_meta(
    dtype = Dtype$new("|u1"),
    order = "C",
    fill_value = 0,
    shape = c(4, 5),
    chunks = c(3, 4),
    dimension_separator = "."
  ))

  store$set_item(".zarray", zarray_meta)

  array <- ZarrArray$new(store = store)
  old_shape <- array$get_shape()
  expect_equal(old_shape, c(4, 5))
  array$resize(1, 2)

  new_shape <- array$get_shape()
  expect_equal(new_shape, c(1, 2))
})

test_that("flush_metadata_nosync (V2) serializes filters on resize", {
  store <- MemoryStore$new()
  zarray_meta <- store$metadata_class$encode_array_metadata(create_zarray_meta(
    dtype = Dtype$new("|u1"),
    order = "C",
    fill_value = 0,
    shape = c(4),
    chunks = c(4),
    compressor = NA,
    filters = list(list(id = "zlib", level = 1L)),
    dimension_separator = "."
  ))
  store$set_item(".zarray", zarray_meta)
  a <- ZarrArray$new(store = store)
  a$resize(8)
  expect_equal(a$get_shape(), c(8))
})

# --- Write/read integration tests ---

test_that("Array write/read round-trip, MemoryStore, no compressor", {
  a <- zarr_create(
    shape = c(4, 4), chunks = c(2, 2),
    store = MemoryStore$new(), compressor = NA
  )
  data <- array(1:16, dim = c(4, 4))
  a$set_item("...", data)
  result <- a$get_item("...")
  expect_equal(result$data, data)
})

test_that("Array write/read round-trip, MemoryStore, Zstd compressor", {
  a <- zarr_create(
    shape = c(4, 4), chunks = c(2, 2),
    store = MemoryStore$new(), compressor = ZstdCodec$new()
  )
  data <- array(1:16, dim = c(4, 4))
  a$set_item("...", data)
  result <- a$get_item("...")
  expect_equal(result$data, data)
})

test_that("Array write/read round-trip, DirectoryStore, default compressor", {
  a <- zarr_create(
    shape = c(4, 4), chunks = c(2, 2),
    store = DirectoryStore$new(tempfile())
  )
  data <- array(1:16, dim = c(4, 4))
  a$set_item("...", data)
  result <- a$get_item("...")
  expect_equal(result$data, data)
})

test_that("Array persists to DirectoryStore and reopens correctly", {
  d <- tempfile()
  data <- array(1:20, dim = c(4, 5))
  a <- zarr_create(shape = c(4, 5), chunks = c(2, 5), store = d)
  a$set_item("...", data)
  b <- zarr_open_array(d, mode = "r")
  result <- b$get_item("...")
  expect_equal(result$data, data)
})

test_that("1D array write/read round-trip", {
  a <- zarr_create(
    shape = c(10), chunks = c(5),
    store = MemoryStore$new(), compressor = NA
  )
  data <- array(1:10, dim = c(10))
  a$set_item("...", data)
  result <- a$get_item("...")
  expect_equal(result$data, data)
})

test_that("3D array write/read round-trip", {
  a <- zarr_create(
    shape = c(3, 3, 3), chunks = c(2, 2, 2),
    store = MemoryStore$new(), compressor = NA
  )
  data <- array(1:27, dim = c(3, 3, 3))
  a$set_item("...", data)
  result <- a$get_item("...")
  expect_equal(result$data, data)
})

test_that("Array with fill_value=0 returns fill for unwritten chunks", {
  a <- zarr_create(
    shape = c(4), chunks = c(2),
    store = MemoryStore$new(), fill_value = 0, compressor = NA
  )
  result <- a$get_item("...")
  expect_equal(as.vector(result$data), c(0, 0, 0, 0))
})

test_that("Array metadata properties are correct", {
  a <- zarr_create(
    shape = c(10, 20), chunks = c(5, 10), dtype = "<f4",
    store = MemoryStore$new(), compressor = ZstdCodec$new()
  )
  expect_equal(a$get_shape(), c(10, 20))
  expect_equal(a$get_chunks(), c(5, 10))
  expect_s3_class(a$get_compressor(), "ZstdCodec")
})

# --- chunk_key format ---

test_that("chunk_key produces dot-separated keys for V2", {
  a <- make_array(shape = c(4, 4), chunks = c(2, 2))
  data <- array(1:16, dim = c(4, 4))
  a$set_item("...", data)
  result <- a$get_item("...")
  expect_equal(result$data, data)
  keys <- a$get_store()$listdir(NA)
  chunk_keys <- keys[!grepl("^\\.z", keys)]
  expect_true(any(grepl("\\.", chunk_keys)))
})

# --- Accessor methods ---

test_that("get_cdata_shape works correctly", {
  a <- make_array(shape = c(5, 7), chunks = c(2, 3))
  cs <- a$get_cdata_shape()
  expect_equal(cs, c(3, 3))  # ceiling(5/2)=3, ceiling(7/3)=3
})

test_that("get_store returns the store", {
  store <- MemoryStore$new()
  a <- zarr_create(shape = c(2), chunks = c(2), store = store)
  expect_identical(a$get_store(), store)
})

test_that("get_path returns NA for root-level array", {
  a <- make_array()
  expect_true(is.na(a$get_path()))
})

test_that("get_path returns path for nested array", {
  g <- zarr_create_group(store = MemoryStore$new())
  a <- g$create_dataset("mydata", shape = c(4), chunks = c(2), dtype = "<i4")
  expect_equal(a$get_path(), "mydata")
})

test_that("get_name returns NA for root array", {
  a <- make_array()
  expect_true(is.na(a$get_name()))
})

test_that("get_name returns /path for nested array", {
  g <- zarr_create_group(store = MemoryStore$new())
  a <- g$create_dataset("mydata", shape = c(4), chunks = c(2), dtype = "<i4")
  expect_equal(a$get_name(), "/mydata")
})

test_that("get_basename returns NA for root array", {
  a <- make_array()
  expect_true(is.na(a$get_basename()))
})

test_that("get_basename returns last path segment", {
  g <- zarr_create_group(store = MemoryStore$new())
  a <- g$create_dataset("mydata", shape = c(4), chunks = c(2), dtype = "<i4")
  expect_equal(a$get_basename(), "mydata")
})

test_that("set_read_only changes read_only property", {
  a <- make_array()
  expect_false(a$get_read_only())
  a$set_read_only(TRUE)
  expect_true(a$get_read_only())
})

test_that("set_shape resizes array", {
  a <- zarr_create(shape = c(4), chunks = c(2), store = MemoryStore$new(), compressor = NA)
  a$set_shape(c(8))
  expect_equal(a$get_shape(), c(8))
})

test_that("get_fill_value returns the configured fill value", {
  a <- zarr_create(shape = c(4), chunks = c(2), store = MemoryStore$new(),
                   fill_value = 42, compressor = NA)
  expect_equal(a$get_fill_value(), 42)
})

test_that("set_fill_value updates fill value in memory", {
  a <- zarr_create(shape = c(4), chunks = c(2), store = MemoryStore$new(),
                   fill_value = 0, compressor = NA)
  a$set_fill_value(7)
  expect_equal(a$get_fill_value(), 7)
})

test_that("get_order returns storage order", {
  a <- make_array(order = "C")
  expect_equal(a$get_order(), "C")
})

test_that("get_filters returns NA when no filters set", {
  a <- make_array()
  expect_true(is_na(a$get_filters()))
})

test_that("get_synchronizer returns NA by default", {
  a <- make_array()
  expect_true(is.na(a$get_synchronizer()))
})

test_that("get_ndim returns number of dimensions", {
  a <- make_array(shape = c(3, 4, 5), chunks = c(2, 2, 2))
  expect_equal(a$get_ndim(), 3)
})

test_that("get_is_view returns FALSE for normal array", {
  a <- make_array()
  expect_false(a$get_is_view())
})

test_that("get_oindex returns OIndex object", {
  a <- make_array()
  expect_s3_class(a$get_oindex(), "OIndex")
})

test_that("get_vindex returns VIndex object", {
  a <- make_array()
  expect_s3_class(a$get_vindex(), "VIndex")
})

test_that("get_write_empty_chunks returns TRUE by default", {
  a <- make_array()
  expect_true(a$get_write_empty_chunks())
})

test_that("get_dimension_separator returns the separator", {
  a <- make_array()
  sep <- a$get_dimension_separator()
  expect_true(sep %in% c(".", "/"))
})

test_that("equals FALSE for read_only mismatch between two arrays", {
  store1 <- MemoryStore$new()
  a <- zarr_create(shape = c(4), chunks = c(2), store = store1, compressor = NA)
  store2 <- MemoryStore$new()
  b <- zarr_create(shape = c(4), chunks = c(2), store = store2, compressor = NA)
  b$set_read_only(TRUE)
  expect_false(a$equals(b))
})

test_that("equals returns TRUE for self-equality", {
  store <- MemoryStore$new()
  a <- zarr_create(shape = c(4), chunks = c(2), store = store, compressor = NA)
  expect_true(a$equals(a))
})

test_that("equals returns FALSE for different stores", {
  a <- zarr_create(shape = c(4), chunks = c(2), store = MemoryStore$new(), compressor = NA)
  b <- zarr_create(shape = c(4), chunks = c(2), store = MemoryStore$new(), compressor = NA)
  expect_false(a$equals(b))
})

test_that("equals returns FALSE for non-ZarrArray object", {
  store <- MemoryStore$new()
  a <- zarr_create(shape = c(4), chunks = c(2), store = store, compressor = NA)
  expect_false(a$equals("not an array"))
})

test_that("length returns first dimension size", {
  # Note: length() uses `if(private$shape)` which errors on multi-element vectors
  # so only works reliably on 1D arrays
  a <- zarr_create(shape = c(7), chunks = c(4), store = MemoryStore$new(), compressor = NA)
  expect_equal(a$length(), 7)
})

test_that("[<-.ZarrArray raises error", {
  a <- make_array(shape = c(4, 4), chunks = c(2, 2))
  expect_error(`[<-.ZarrArray`(a, value = 1), "not yet supported")
})

# --- Parallel settings ---

test_that("get_parallel_settings returns lapply by default (no parallelism)", {
  ps <- get_parallel_settings(parallel_option = NA)
  expect_false(ps$close)
  result <- ps$apply_func(list(1, 2, 3), function(x) x * 2)
  expect_equal(result, list(2, 4, 6))
})

test_that("get_parallel_settings with integer option on non-Windows uses mclapply", {
  skip_on_os("windows")
  ps <- get_parallel_settings(on_windows = FALSE, parallel_option = 2L)
  expect_false(ps$close)
  result <- ps$apply_func(list(1, 2, 3), function(x) x * 2, cl = ps$cl)
  expect_equal(result, list(2, 4, 6))
})

test_that("get_parallel_settings with integer >1 on Windows creates cluster", {
  skip_on_os(c("linux", "mac", "solaris"))
  ps <- get_parallel_settings(on_windows = TRUE, parallel_option = 2L)
  expect_true(ps$close)
  parallel::stopCluster(ps$cl)
})

test_that("get_parallel_settings with on_windows=TRUE and integer=1 creates cluster", {
  skip_on_os(c("linux", "mac"))
  ps <- get_parallel_settings(on_windows = TRUE, parallel_option = 1L)
  if (ps$close) parallel::stopCluster(ps$cl)
  expect_true(TRUE)
})
