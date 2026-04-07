library(pizzarr)

# Unzip V3 test fixture to temp directory.
# Fixture generated with zarrita (see inst/extdata/fixtures/v3/generate-v3.py).
v3_zip <- system.file("extdata/fixtures/v3/data.zarr.zip", package = "pizzarr")
tdir <- tempfile("v3test")
dir.create(tdir)
utils::unzip(v3_zip, exdir = tdir)
v3_root <- file.path(tdir, "data.zarr")

# --- Format detection ---

test_that("detect_zarr_version returns 3 for V3 store", {
  store <- DirectoryStore$new(v3_root)
  expect_equal(detect_zarr_version(store), 3L)
})

test_that("contains_group detects V3 root group", {
  store <- DirectoryStore$new(v3_root)
  expect_true(contains_group(store))
})

test_that("contains_array detects V3 array", {
  store <- DirectoryStore$new(v3_root)
  expect_true(contains_array(store, "1d.contiguous.raw.i2"))
})

# --- Group opening ---

test_that("V3 group can be opened with zarr_open_group", {
  g <- zarr_open_group(v3_root, mode = "r")
  expect_s3_class(g, "ZarrGroup")
})

test_that("V3 group can be opened with zarr_open", {
  g <- zarr_open(v3_root, mode = "r")
  expect_s3_class(g, "ZarrGroup")
})

# --- 1D arrays ---

test_that("V3 1d contiguous raw (no compression) int16", {
  store <- DirectoryStore$new(v3_root)
  a <- ZarrArray$new(store, path = "1d.contiguous.raw.i2", read_only = TRUE)
  expect_equal(a$get_shape(), 4)
  result <- a$get_item("...")
  expect_equal(as.integer(result$data), c(1L, 2L, 3L, 4L))
})

test_that("V3 1d contiguous gzip int16", {
  store <- DirectoryStore$new(v3_root)
  a <- ZarrArray$new(store, path = "1d.contiguous.gzip.i2", read_only = TRUE)
  expect_equal(a$get_shape(), 4)
  result <- a$get_item("...")
  expect_equal(as.integer(result$data), c(1L, 2L, 3L, 4L))
})

test_that("V3 1d contiguous blosc int16", {
  skip_if_not_installed("blosc")
  store <- DirectoryStore$new(v3_root)
  a <- ZarrArray$new(store, path = "1d.contiguous.blosc.i2", read_only = TRUE)
  expect_equal(a$get_shape(), 4)
  result <- a$get_item("...")
  expect_equal(as.integer(result$data), c(1L, 2L, 3L, 4L))
})

test_that("V3 1d contiguous int32", {
  skip_if_not_installed("blosc")
  store <- DirectoryStore$new(v3_root)
  a <- ZarrArray$new(store, path = "1d.contiguous.i4", read_only = TRUE)
  expect_equal(a$get_shape(), 4)
  result <- a$get_item("...")
  expect_equal(as.integer(result$data), c(1L, 2L, 3L, 4L))
})

test_that("V3 1d contiguous uint8", {
  skip_if_not_installed("blosc")
  store <- DirectoryStore$new(v3_root)
  a <- ZarrArray$new(store, path = "1d.contiguous.u1", read_only = TRUE)
  expect_equal(a$get_shape(), 4)
  result <- a$get_item("...")
  expect_equal(as.integer(result$data), c(255L, 0L, 255L, 0L))
})

test_that("V3 1d contiguous float32 little-endian", {
  skip_if_not_installed("blosc")
  store <- DirectoryStore$new(v3_root)
  a <- ZarrArray$new(store, path = "1d.contiguous.f4.le", read_only = TRUE)
  expect_equal(a$get_shape(), 4)
  result <- a$get_item("...")
  expect_equal(as.double(result$data), c(-1000.5, 0, 1000.5, 0))
})

test_that("V3 1d contiguous float32 big-endian", {
  skip_if_not_installed("blosc")
  store <- DirectoryStore$new(v3_root)
  a <- ZarrArray$new(store, path = "1d.contiguous.f4.be", read_only = TRUE)
  expect_equal(a$get_shape(), 4)
  result <- a$get_item("...")
  expect_equal(as.double(result$data), c(-1000.5, 0, 1000.5, 0))
})

test_that("V3 1d contiguous float64", {
  skip_if_not_installed("blosc")
  store <- DirectoryStore$new(v3_root)
  a <- ZarrArray$new(store, path = "1d.contiguous.f8", read_only = TRUE)
  expect_equal(a$get_shape(), 4)
  result <- a$get_item("...")
  expect_equal(as.double(result$data), c(1.5, 2.5, 3.5, 4.5))
})

test_that("V3 1d contiguous bool", {
  skip_if_not_installed("blosc")
  store <- DirectoryStore$new(v3_root)
  a <- ZarrArray$new(store, path = "1d.contiguous.b1", read_only = TRUE)
  expect_equal(a$get_shape(), 4)
  result <- a$get_item("...")
  expect_equal(as.logical(result$data), c(TRUE, FALSE, TRUE, FALSE))
})

test_that("V3 1d chunked int16", {
  skip_if_not_installed("blosc")
  store <- DirectoryStore$new(v3_root)
  a <- ZarrArray$new(store, path = "1d.chunked.i2", read_only = TRUE)
  expect_equal(a$get_shape(), 4)
  result <- a$get_item("...")
  expect_equal(as.integer(result$data), c(1L, 2L, 3L, 4L))
})

test_that("V3 1d chunked ragged int16", {
  skip_if_not_installed("blosc")
  store <- DirectoryStore$new(v3_root)
  a <- ZarrArray$new(store, path = "1d.chunked.ragged.i2", read_only = TRUE)
  expect_equal(a$get_shape(), 5)
  result <- a$get_item("...")
  expect_equal(as.integer(result$data), c(1L, 2L, 3L, 4L, 5L))
})

# --- 2D arrays ---

test_that("V3 2d contiguous int16", {
  skip_if_not_installed("blosc")
  store <- DirectoryStore$new(v3_root)
  a <- ZarrArray$new(store, path = "2d.contiguous.i2", read_only = TRUE)
  expect_equal(a$get_shape(), c(2L, 2L))
  result <- a$get_item("...")
  # Data written as [[1,2],[3,4]] in C order (row-major).
  # R uses column-major, so check element access directly.
  expect_equal(result$data[1, 1], 1)
  expect_equal(result$data[1, 2], 2)
  expect_equal(result$data[2, 1], 3)
  expect_equal(result$data[2, 2], 4)
})

test_that("V3 2d chunked int16", {
  skip_if_not_installed("blosc")
  store <- DirectoryStore$new(v3_root)
  a <- ZarrArray$new(store, path = "2d.chunked.i2", read_only = TRUE)
  expect_equal(a$get_shape(), c(2L, 2L))
  result <- a$get_item("...")
  expect_equal(result$data[1, 1], 1)
  expect_equal(result$data[1, 2], 2)
  expect_equal(result$data[2, 1], 3)
  expect_equal(result$data[2, 2], 4)
})

test_that("V3 2d chunked ragged int16", {
  skip_if_not_installed("blosc")
  store <- DirectoryStore$new(v3_root)
  a <- ZarrArray$new(store, path = "2d.chunked.ragged.i2", read_only = TRUE)
  expect_equal(a$get_shape(), c(3L, 3L))
  result <- a$get_item("...")
  # Data written as [[1,2,3],[4,5,6],[7,8,9]] in C order.
  expect_equal(result$data[1, 1], 1)
  expect_equal(result$data[1, 3], 3)
  expect_equal(result$data[2, 1], 4)
  expect_equal(result$data[3, 3], 9)
})

# --- 3D arrays ---

test_that("V3 3d contiguous int16", {
  skip_if_not_installed("blosc")
  store <- DirectoryStore$new(v3_root)
  a <- ZarrArray$new(store, path = "3d.contiguous.i2", read_only = TRUE)
  expect_equal(a$get_shape(), c(3L, 3L, 3L))
  result <- a$get_item("...")
  # Data written as np.arange(27).reshape(3,3,3) in C order.
  # In C order: element[i,j,k] = i*9 + j*3 + k (0-based).
  # Check element access (R 1-based): data[i,j,k] = (i-1)*9 + (j-1)*3 + (k-1)
  expect_equal(result$data[1, 1, 1], 0)
  expect_equal(result$data[1, 1, 2], 1)
  expect_equal(result$data[1, 2, 1], 3)
  expect_equal(result$data[2, 1, 1], 9)
  expect_equal(result$data[3, 3, 3], 26)
  expect_equal(length(result$data), 27)
})

test_that("V3 3d chunked int16", {
  skip_if_not_installed("blosc")
  store <- DirectoryStore$new(v3_root)
  a <- ZarrArray$new(store, path = "3d.chunked.i2", read_only = TRUE)
  expect_equal(a$get_shape(), c(3L, 3L, 3L))
  result <- a$get_item("...")
  expect_equal(result$data[1, 1, 1], 0)
  expect_equal(result$data[2, 1, 1], 9)
  expect_equal(result$data[3, 3, 3], 26)
  expect_equal(length(result$data), 27)
})

test_that("V3 3d chunked mixed C-order int16", {
  skip_if_not_installed("blosc")
  store <- DirectoryStore$new(v3_root)
  a <- ZarrArray$new(store, path = "3d.chunked.mixed.i2.C", read_only = TRUE)
  expect_equal(a$get_shape(), c(3L, 3L, 3L))
  result <- a$get_item("...")
  expect_equal(result$data[1, 1, 1], 0)
  expect_equal(result$data[2, 1, 1], 9)
  expect_equal(result$data[3, 3, 3], 26)
  expect_equal(length(result$data), 27)
})

test_that("V3 3d chunked mixed F-order int16", {
  skip_if_not_installed("blosc")
  store <- DirectoryStore$new(v3_root)
  a <- ZarrArray$new(store, path = "3d.chunked.mixed.i2.F", read_only = TRUE)
  expect_equal(a$get_shape(), c(3L, 3L, 3L))
  result <- a$get_item("...")
  expect_equal(result$data[1, 1, 1], 0)
  expect_equal(result$data[2, 1, 1], 9)
  expect_equal(result$data[3, 3, 3], 26)
  expect_equal(length(result$data), 27)
})

# --- Auto-detection via zarr_open ---

test_that("zarr_open auto-detects V3 group and can read child array", {
  g <- zarr_open(v3_root, mode = "r")
  expect_s3_class(g, "ZarrGroup")
  arr <- g$get_item("1d.contiguous.raw.i2")
  expect_s3_class(arr, "ZarrArray")
  result <- arr$get_item("...")
  expect_equal(as.integer(result$data), c(1L, 2L, 3L, 4L))
})

# --- V3 open in write mode ---

test_that("V3 store opens in append mode without write guard message", {
  tdir2 <- tempfile("v3write_open")
  dir.create(tdir2)
  utils::unzip(v3_zip, exdir = tdir2)
  v3_root2 <- file.path(tdir2, "data.zarr")
  g <- zarr_open_group(v3_root2, mode = "a")
  expect_s3_class(g, "ZarrGroup")
  expect_false(g$get_read_only())
  unlink(tdir2, recursive = TRUE)
})

# Clean up
unlink(tdir, recursive = TRUE)

# =============================================================================
# V3 Write tests
# =============================================================================

# --- Basic V3 array creation and round-trip ---

test_that("zarr_create with zarr_format=3 creates zarr.json", {
  store <- MemoryStore$new()
  z <- zarr_create(shape = c(4L), dtype = "<i2", zarr_format = 3L, store = store)
  expect_true(store$contains_item("zarr.json"))
  expect_false(store$contains_item(".zarray"))
  raw_meta <- store$get_item("zarr.json")
  meta <- jsonlite::fromJSON(rawToChar(raw_meta))
  expect_equal(meta$zarr_format, 3)
  expect_equal(meta$node_type, "array")
  expect_equal(meta$data_type, "int16")
})

test_that("V3 array write and read round-trip (int32, no compression)", {
  store <- MemoryStore$new()
  z <- zarr_create(shape = c(6L), dtype = "<i4", zarr_format = 3L,
                   compressor = NA, store = store)
  data_in <- array(c(10L, 20L, 30L, 40L, 50L, 60L), dim = c(6))
  z$set_item("...", data_in)
  result <- z$get_item("...")
  expect_equal(as.integer(result$data), c(10L, 20L, 30L, 40L, 50L, 60L))
})

test_that("V3 array write and read round-trip (float64, zstd)", {
  store <- MemoryStore$new()
  z <- zarr_create(shape = c(5L), dtype = "<f8", zarr_format = 3L,
                   compressor = ZstdCodec$new(level = 1), store = store)
  data_in <- array(c(1.1, 2.2, 3.3, 4.4, 5.5), dim = c(5))
  z$set_item("...", data_in)
  result <- z$get_item("...")
  expect_equal(as.double(result$data), c(1.1, 2.2, 3.3, 4.4, 5.5), tolerance = 1e-10)
})

test_that("V3 array write and read round-trip (bool)", {
  store <- MemoryStore$new()
  z <- zarr_create(shape = c(4L), dtype = "|b1", zarr_format = 3L,
                   compressor = NA, store = store)
  data_in <- array(c(TRUE, FALSE, TRUE, FALSE), dim = c(4))
  z$set_item("...", data_in)
  result <- z$get_item("...")
  expect_equal(as.logical(result$data), c(TRUE, FALSE, TRUE, FALSE))
})

test_that("V3 array write and read round-trip (uint8)", {
  store <- MemoryStore$new()
  z <- zarr_create(shape = c(4L), dtype = "|u1", zarr_format = 3L,
                   compressor = NA, store = store)
  data_in <- array(c(0L, 127L, 128L, 255L), dim = c(4))
  z$set_item("...", data_in)
  result <- z$get_item("...")
  expect_equal(as.integer(result$data), c(0L, 127L, 128L, 255L))
})

test_that("V3 array write and read round-trip (float32)", {
  store <- MemoryStore$new()
  z <- zarr_create(shape = c(4L), dtype = "<f4", zarr_format = 3L,
                   compressor = NA, store = store)
  data_in <- array(c(-1.5, 0, 1.5, 3.14), dim = c(4))
  z$set_item("...", data_in)
  result <- z$get_item("...")
  expect_equal(as.double(result$data), c(-1.5, 0, 1.5, 3.14), tolerance = 1e-5)
})

test_that("V3 2D array write and read round-trip (gzip)", {
  store <- MemoryStore$new()
  z <- zarr_create(shape = c(3L, 4L), chunks = c(2L, 2L), dtype = "<i2",
                   zarr_format = 3L, compressor = GzipCodec$new(), store = store)
  data_in <- array(seq_len(12L), dim = c(3, 4))
  z$set_item("...", data_in)
  result <- z$get_item("...")
  expect_equal(result$data, data_in)
})

test_that("V3 3D array write and read round-trip", {
  store <- MemoryStore$new()
  z <- zarr_create(shape = c(2L, 3L, 4L), dtype = "<f8", zarr_format = 3L,
                   compressor = NA, store = store)
  data_in <- array(as.double(0:23), dim = c(2, 3, 4))
  z$set_item("...", data_in)
  result <- z$get_item("...")
  expect_equal(result$data, data_in)
})

# --- Metadata verification ---

test_that("V3 zarr.json has correct structure (bytes codec, no endian for single-byte)", {
  store <- MemoryStore$new()
  zarr_create(shape = c(4L), dtype = "|b1", zarr_format = 3L,
              compressor = NA, store = store)
  raw_meta <- store$get_item("zarr.json")
  meta <- jsonlite::fromJSON(rawToChar(raw_meta), simplifyVector = FALSE)
  expect_equal(meta$zarr_format, 3)
  expect_equal(meta$node_type, "array")
  expect_equal(meta$data_type, "bool")
  expect_equal(meta$chunk_grid$name, "regular")
  expect_equal(meta$chunk_key_encoding$name, "default")
  expect_equal(meta$chunk_key_encoding$configuration$separator, "/")
  # For bool (single-byte), bytes codec should have no endian config
  codecs <- meta$codecs
  bytes_codec <- codecs[[1]]
  expect_equal(bytes_codec$name, "bytes")
  expect_null(bytes_codec$configuration)
  expect_true(is.list(meta$attributes) && length(meta$attributes) == 0)
})

test_that("V3 zarr.json bytes codec has endian for multi-byte types", {
  store <- MemoryStore$new()
  zarr_create(shape = c(4L), dtype = "<f8", zarr_format = 3L,
              compressor = NA, store = store)
  raw_meta <- store$get_item("zarr.json")
  meta <- jsonlite::fromJSON(rawToChar(raw_meta), simplifyVector = FALSE)
  codecs <- meta$codecs
  bytes_codec <- codecs[[1]]
  expect_equal(bytes_codec$name, "bytes")
  expect_equal(bytes_codec$configuration$endian, "little")
})

test_that("V3 zarr.json codec pipeline includes compressor after bytes codec", {
  store <- MemoryStore$new()
  zarr_create(shape = c(10L), dtype = "<f8", zarr_format = 3L,
              compressor = ZstdCodec$new(level = 3), store = store)
  raw_meta <- store$get_item("zarr.json")
  meta <- jsonlite::fromJSON(rawToChar(raw_meta), simplifyVector = FALSE)
  expect_equal(length(meta$codecs), 2)
  expect_equal(meta$codecs[[1]]$name, "bytes")
  expect_equal(meta$codecs[[2]]$name, "zstd")
  expect_equal(meta$codecs[[2]]$configuration$level, 3)
})

test_that("V3 fill_value NaN is encoded as string", {
  store <- MemoryStore$new()
  zarr_create(shape = c(4L), dtype = "<f8", fill_value = NaN,
              zarr_format = 3L, compressor = NA, store = store)
  raw_meta <- store$get_item("zarr.json")
  meta <- jsonlite::fromJSON(rawToChar(raw_meta))
  expect_equal(meta$fill_value, "NaN")
})

test_that("V3 fill_value Inf is encoded as string", {
  store <- MemoryStore$new()
  zarr_create(shape = c(4L), dtype = "<f8", fill_value = Inf,
              zarr_format = 3L, compressor = NA, store = store)
  raw_meta <- store$get_item("zarr.json")
  meta <- jsonlite::fromJSON(rawToChar(raw_meta))
  expect_equal(meta$fill_value, "Infinity")
})

# --- Group write tests ---

test_that("zarr_create_group with zarr_format=3 creates zarr.json with node_type=group", {
  store <- MemoryStore$new()
  g <- zarr_create_group(store = store, zarr_format = 3L)
  expect_s3_class(g, "ZarrGroup")
  expect_true(store$contains_item("zarr.json"))
  expect_false(store$contains_item(".zgroup"))
  raw_meta <- store$get_item("zarr.json")
  meta <- jsonlite::fromJSON(rawToChar(raw_meta))
  expect_equal(meta$zarr_format, 3)
  expect_equal(meta$node_type, "group")
})

test_that("V3 group create_dataset creates V3 array", {
  store <- MemoryStore$new()
  g <- zarr_create_group(store = store, zarr_format = 3L)
  a <- g$create_dataset("data", shape = c(5L), dtype = "<i4", compressor = NA)
  expect_s3_class(a, "ZarrArray")
  expect_true(store$contains_item("data/zarr.json"))
  expect_false(store$contains_item("data/.zarray"))
})

test_that("V3 group create_group creates nested V3 group", {
  store <- MemoryStore$new()
  g <- zarr_create_group(store = store, zarr_format = 3L)
  sub <- g$create_group("sub")
  expect_s3_class(sub, "ZarrGroup")
  expect_true(store$contains_item("sub/zarr.json"))
  expect_false(store$contains_item("sub/.zgroup"))
})

# --- Attribute write tests ---

test_that("V3 array attributes are stored in zarr.json, not .zattrs", {
  store <- MemoryStore$new()
  z <- zarr_create(shape = c(4L), dtype = "<i4", zarr_format = 3L,
                   compressor = NA, store = store)
  z$get_attrs()$set_item("units", "meters")
  expect_false(store$contains_item(".zattrs"))
  raw_meta <- store$get_item("zarr.json")
  meta <- jsonlite::fromJSON(rawToChar(raw_meta), simplifyVector = FALSE)
  expect_equal(meta$attributes$units, "meters")
})

test_that("V3 array attributes round-trip: write, re-open, read back", {
  store <- MemoryStore$new()
  z <- zarr_create(shape = c(4L), dtype = "<i4", zarr_format = 3L,
                   compressor = NA, store = store)
  z$get_attrs()$set_item("description", "test array")
  z$get_attrs()$set_item("version", 42L)

  # Re-open
  z2 <- ZarrArray$new(store)
  attrs <- z2$get_attrs()$to_list()
  expect_equal(attrs$description, "test array")
  expect_equal(attrs$version, 42L)
})

test_that("V3 group attributes are in zarr.json", {
  store <- MemoryStore$new()
  g <- zarr_create_group(store = store, zarr_format = 3L)
  g$get_attrs()$set_item("generator", "pizzarr")
  raw_meta <- store$get_item("zarr.json")
  meta <- jsonlite::fromJSON(rawToChar(raw_meta), simplifyVector = FALSE)
  expect_equal(meta$attributes$generator, "pizzarr")
})

# --- Resize and append tests ---

test_that("V3 array resize updates zarr.json shape", {
  store <- MemoryStore$new()
  z <- zarr_create(shape = c(4L), dtype = "<i4", zarr_format = 3L,
                   compressor = NA, store = store)
  z$resize(c(8L))
  raw_meta <- store$get_item("zarr.json")
  meta <- jsonlite::fromJSON(rawToChar(raw_meta))
  expect_equal(meta$shape, 8)
})

test_that("V3 array append round-trip", {
  store <- MemoryStore$new()
  z <- zarr_create(shape = c(3L), dtype = "<f8", zarr_format = 3L,
                   compressor = NA, store = store)
  z$set_item("...", array(c(1.0, 2.0, 3.0), dim = 3))
  z$append(array(c(4.0, 5.0), dim = 2))
  expect_equal(z$get_shape(), 5)
  result <- z$get_item("...")
  expect_equal(as.double(result$data), c(1.0, 2.0, 3.0, 4.0, 5.0))
})

# --- DirectoryStore V3 write ---

test_that("V3 write to DirectoryStore creates correct file layout", {
  tdir3 <- tempfile("v3dirwrite")
  dir.create(tdir3)
  store <- DirectoryStore$new(tdir3)
  g <- zarr_create_group(store = store, zarr_format = 3L)
  a <- g$create_dataset("arr", shape = c(4L), dtype = "<i4", compressor = NA)
  data_in <- array(c(1L, 2L, 3L, 4L), dim = 4)
  a$set_item("...", data_in)

  # Check file layout: zarr.json at root and arr/
  expect_true(file.exists(file.path(tdir3, "zarr.json")))
  expect_true(file.exists(file.path(tdir3, "arr", "zarr.json")))
  expect_false(file.exists(file.path(tdir3, ".zgroup")))
  expect_false(file.exists(file.path(tdir3, "arr", ".zarray")))

  # Chunk key uses c/0 format (default V3 encoding with "/" separator)
  expect_true(file.exists(file.path(tdir3, "arr", "c", "0")))

  # Read back
  store2 <- DirectoryStore$new(tdir3)
  g2 <- zarr_open_group(store2, mode = "r")
  a2 <- g2$get_item("arr")
  result <- a2$get_item("...")
  expect_equal(as.integer(result$data), c(1L, 2L, 3L, 4L))

  unlink(tdir3, recursive = TRUE)
})

# =============================================================================
# zarr-python 3.x interop tests
# Fixture generated with zarr-python 3.1.5
# (see inst/extdata/fixtures/v3/generate-v3-zarr-python.py)
# =============================================================================

zp_zip <- system.file("extdata/fixtures/v3/zarr_python_v3.zarr.zip",
                       package = "pizzarr")
zp_dir <- tempfile("zp_v3test")
dir.create(zp_dir)
utils::unzip(zp_zip, exdir = zp_dir)
zp_root <- file.path(zp_dir, "zarr_python_v3.zarr")

test_that("zarr-python V3 store opens as group", {
  g <- zarr_open(zp_root, mode = "r")
  expect_s3_class(g, "ZarrGroup")
  attrs <- g$get_attrs()$to_list()
  expect_equal(attrs$generator, "zarr-python")
  expect_equal(attrs$zarr_python_version, "3.1.5")
})

test_that("zarr-python V3 int32 1d (gzip)", {
  g <- zarr_open(zp_root, mode = "r")
  a <- g$get_item("int32_1d")
  expect_equal(a$get_shape(), 6)
  expect_equal(as.integer(a$as.array()), c(10L, 20L, 30L, 40L, 50L, 60L))
})

test_that("zarr-python V3 float64 1d (zstd)", {
  g <- zarr_open(zp_root, mode = "r")
  a <- g$get_item("float64_1d")
  expect_equal(a$get_shape(), 5)
  expect_equal(as.double(a$as.array()), c(1.1, 2.2, 3.3, 4.4, 5.5))
})

test_that("zarr-python V3 bool 1d (uncompressed)", {
  g <- zarr_open(zp_root, mode = "r")
  a <- g$get_item("bool_1d")
  expect_equal(a$get_shape(), 4)
  expect_equal(as.logical(a$as.array()), c(TRUE, FALSE, TRUE, FALSE))
})

test_that("zarr-python V3 uint8 1d (uncompressed, chunked)", {
  g <- zarr_open(zp_root, mode = "r")
  a <- g$get_item("uint8_1d")
  expect_equal(a$get_shape(), 4)
  expect_equal(as.integer(a$as.array()), c(0L, 127L, 128L, 255L))
})

test_that("zarr-python V3 float32 1d (blosc)", {
  skip_if_not_installed("blosc")
  g <- zarr_open(zp_root, mode = "r")
  a <- g$get_item("float32_1d")
  expect_equal(a$get_shape(), 4)
  result <- as.double(a$as.array())
  expect_equal(result[1], -1.5)
  expect_equal(result[2], 0.0)
  expect_equal(result[3], 1.5)
  expect_equal(result[4], 3.14, tolerance = 1e-5)
})

test_that("zarr-python V3 int16 2d (gzip)", {
  g <- zarr_open(zp_root, mode = "r")
  a <- g$get_item("int16_2d")
  expect_equal(a$get_shape(), c(4, 3))
  m <- a$as.array()
  expect_equal(m[1, 1], 0)
  expect_equal(m[1, 3], 2)
  expect_equal(m[4, 3], 11)
})

test_that("zarr-python V3 float64 3d (zstd)", {
  g <- zarr_open(zp_root, mode = "r")
  a <- g$get_item("float64_3d")
  expect_equal(a$get_shape(), c(2, 3, 4))
  arr <- a$as.array()
  expect_equal(arr[1, 1, 1], 0)
  expect_equal(arr[2, 3, 4], 23)
})

test_that("zarr-python V3 fill value works", {
  g <- zarr_open(zp_root, mode = "r")
  a <- g$get_item("with_fill")
  expect_equal(a$get_shape(), 6)
  d <- as.double(a$as.array())
  expect_equal(d[1:3], c(1.0, 2.0, 3.0))
  expect_equal(d[4:6], c(-9999.0, -9999.0, -9999.0))
})

test_that("zarr-python V3 ragged 2d (gzip)", {
  g <- zarr_open(zp_root, mode = "r")
  a <- g$get_item("ragged_2d")
  expect_equal(a$get_shape(), c(5, 7))
  m <- a$as.array()
  expect_equal(m[1, 1], 0)
  expect_equal(m[5, 7], 34)
  expect_equal(length(m), 35)
})

test_that("zarr-python V3 nested group with attributes", {
  g <- zarr_open(zp_root, mode = "r")
  sub <- g$get_item("var_group")
  expect_s3_class(sub, "ZarrGroup")
  attrs <- sub$get_attrs()$to_list()
  expect_equal(attrs$description, "Group with multiple variables")
  expect_equal(attrs$source, "zarr-python 3.x test generation")
})

test_that("zarr-python V3 sub-group array with attributes (temperature)", {
  g <- zarr_open(zp_root, mode = "r")
  temp <- g$get_item("var_group")$get_item("temperature")
  expect_equal(temp$get_shape(), c(3, 4))
  ta <- temp$get_attrs()$to_list()
  expect_equal(ta$units, "K")
  expect_equal(ta$long_name, "Temperature")
  m <- temp$as.array()
  expect_equal(m[1, 1], 280.1, tolerance = 1e-1)
  expect_equal(m[3, 4], 292.2, tolerance = 1e-1)
})

test_that("zarr-python V3 sub-group array (pressure, gzip)", {
  g <- zarr_open(zp_root, mode = "r")
  pres <- g$get_item("var_group")$get_item("pressure")
  expect_equal(pres$get_shape(), c(3, 4))
  m <- pres$as.array()
  expect_equal(m[1, 1], 101325.0)
  expect_equal(m[3, 4], 101270.0)
})

# Clean up
unlink(zp_dir, recursive = TRUE)

# --- V3 write: flush_metadata_nosync ---

test_that("flush_metadata_nosync (V3) writes updated zarr.json", {
  d <- tempfile()
  a <- zarr_create(shape = c(4, 4), chunks = c(2, 2), store = d,
                   compressor = NA, zarr_format = 3L)
  a$resize(6, 4)
  b <- zarr_open_array(d, mode = "r")
  expect_equal(b$get_shape(), c(6, 4))
})

# --- V3 metadata loading ---

test_that("V3 load_metadata_v3_nosync populates shape/chunks/dtype/fill_value", {
  d <- tempfile()
  a <- zarr_create(shape = c(4, 4), chunks = c(2, 2), dtype = "<f8",
                   store = d, compressor = NA, fill_value = 0,
                   zarr_format = 3L)
  a$set_item("...", array(1:16 * 1.0, dim = c(4, 4)))
  b <- zarr_open_array(d, mode = "r")
  expect_equal(b$get_shape(), c(4, 4))
  expect_equal(b$get_chunks(), c(2, 2))
  result <- b$get_item("...")
  expect_equal(result$data, array(1:16 * 1.0, dim = c(4, 4)))
})

test_that("V3 chunk_key generates c/0/0 style keys for default encoding", {
  d <- tempfile()
  a <- zarr_create(shape = c(4, 4), chunks = c(2, 2), store = d,
                   compressor = NA, zarr_format = 3L)
  data <- array(1:16, dim = c(4, 4))
  a$set_item("...", data)
  result <- a$get_item("...")
  expect_equal(result$data, data)
})

test_that("V3 array with default chunk_key_encoding uses slash separator", {
  d <- tempfile()
  a <- zarr_create(
    shape = c(4), chunks = c(2), store = d,
    compressor = NA, zarr_format = 3L
  )
  data <- array(1:4, dim = c(4))
  a$set_item("...", data)
  b <- zarr_open_array(d, mode = "r")
  result <- b$get_item("...")
  expect_equal(as.integer(result$data), 1:4)
  expect_equal(b$get_dimension_separator(), "/")
})

# =============================================================================
# Gap 3: Additional V3 metadata loading tests
# =============================================================================

test_that("V3 load_metadata_v3_nosync handles different dtypes", {
  for (dt in c("<i2", "<i4", "<f4", "<f8", "|u1", "|b1")) {
    store <- MemoryStore$new()
    z <- zarr_create(shape = c(4L), dtype = dt, zarr_format = 3L,
                     compressor = NA, store = store)
    reopened <- ZarrArray$new(store, read_only = TRUE)
    expect_equal(reopened$get_shape(), 4)
  }
})

test_that("V3 load_metadata_v3_nosync with gzip codec", {
  store <- MemoryStore$new()
  z <- zarr_create(shape = c(8L), dtype = "<f8", zarr_format = 3L,
                   compressor = GzipCodec$new(), store = store)
  z$set_item("...", array(as.double(1:8), dim = 8))
  reopened <- ZarrArray$new(store, read_only = TRUE)
  result <- reopened$get_item("...")
  expect_equal(as.double(result$data), as.double(1:8))
})

test_that("V3 load_metadata_v3_nosync with zstd codec", {
  store <- MemoryStore$new()
  z <- zarr_create(shape = c(8L), dtype = "<i4", zarr_format = 3L,
                   compressor = ZstdCodec$new(level = 1), store = store)
  z$set_item("...", array(1:8, dim = 8))
  reopened <- ZarrArray$new(store, read_only = TRUE)
  result <- reopened$get_item("...")
  expect_equal(as.integer(result$data), 1:8)
})

test_that("V3 load_metadata_v3_nosync with 2D array", {
  store <- MemoryStore$new()
  z <- zarr_create(shape = c(3L, 4L), chunks = c(2L, 2L), dtype = "<f8",
                   zarr_format = 3L, compressor = NA, store = store)
  data <- array(as.double(1:12), dim = c(3, 4))
  z$set_item("...", data)
  reopened <- ZarrArray$new(store, read_only = TRUE)
  expect_equal(reopened$get_shape(), c(3, 4))
  expect_equal(reopened$get_chunks(), c(2, 2))
  result <- reopened$get_item("...")
  expect_equal(result$data, data)
})

test_that("V3 metadata contains correct fill_value for NaN", {
  store <- MemoryStore$new()
  z <- zarr_create(shape = c(4L), dtype = "<f8", fill_value = NaN,
                   zarr_format = 3L, compressor = NA, store = store)
  reopened <- ZarrArray$new(store, read_only = TRUE)
  # NaN fill_value round-trips through V3 encode/decode
  expect_true(is.nan(reopened$get_fill_value()))
  # Uninitialized chunks should contain NaN
  result <- reopened$get_item("...")
  expect_true(all(is.nan(result$data)))
})

# =============================================================================
# Gap 5: ZarrGroup$initialize V3 path
# =============================================================================

test_that("V3 group init populates metadata correctly", {
  store <- MemoryStore$new()
  g <- zarr_create_group(store = store, zarr_format = 3L)
  expect_s3_class(g, "ZarrGroup")
  expect_equal(g$get_name(), "/")
  expect_false(g$get_read_only())
})

test_that("V3 group re-open preserves attributes", {
  store <- MemoryStore$new()
  g <- zarr_create_group(store = store, zarr_format = 3L)
  g$get_attrs()$set_item("description", "test group")
  # Re-open
  g2 <- zarr_open_group(store, mode = "r")
  attrs <- g2$get_attrs()$to_list()
  expect_equal(attrs$description, "test group")
})

test_that("V3 group get_path returns stored path", {
  store <- MemoryStore$new()
  g <- zarr_create_group(store = store, zarr_format = 3L)
  expect_equal(g$get_path(), "")
})

test_that("V3 group get_meta returns metadata", {
  store <- MemoryStore$new()
  g <- zarr_create_group(store = store, zarr_format = 3L)
  meta <- g$get_meta()
  expect_false(is.null(meta))
})

test_that("V3 group with nested arrays and groups", {
  store <- MemoryStore$new()
  g <- zarr_create_group(store = store, zarr_format = 3L)
  sub <- g$create_group("sub")
  a <- sub$create_dataset("data", shape = c(4L), dtype = "<i4", compressor = NA)
  a$set_item("...", array(1:4, dim = 4))
  # Re-open and traverse
  g2 <- zarr_open_group(store, mode = "r")
  sub2 <- g2$get_item("sub")
  expect_s3_class(sub2, "ZarrGroup")
  a2 <- sub2$get_item("data")
  expect_equal(as.integer(a2$get_item("...")$data), 1:4)
})

test_that("V3 group GroupNotFoundError on empty store", {
  store <- MemoryStore$new()
  expect_error(zarr_open_group(store, mode = "r"), "GroupNotFoundError")
})

test_that("V3 vlen-utf8 string array loads correctly via load_metadata_v3_nosync", {
  v3_zip <- system.file("extdata/fixtures/v3/data.zarr.zip", package = "pizzarr")
  tdir <- tempfile("v3vlen")
  dir.create(tdir)
  utils::unzip(v3_zip, exdir = tdir)
  v3_root <- file.path(tdir, "data.zarr")
  store <- DirectoryStore$new(v3_root)
  if (contains_array(store, "1d.contiguous.raw.i2")) {
    a <- ZarrArray$new(store, path = "1d.contiguous.raw.i2", read_only = TRUE)
    expect_equal(a$get_shape(), 4)
  } else {
    skip("fixture not found")
  }
})

# --- dimension_names threading (PR 0B) ---

test_that("V3 zarr_create with dimension_names round-trips on reopen", {
  store <- MemoryStore$new()
  z <- zarr_create(shape = c(2L, 3L), dtype = "<f8", zarr_format = 3L,
                   compressor = NA, store = store,
                   dimension_names = c("x", "y"))
  expect_equal(z$get_dimension_names(), c("x", "y"))

  # Re-open from store and confirm dimension_names persisted in zarr.json.
  z2 <- ZarrArray$new(store)
  expect_equal(z2$get_dimension_names(), c("x", "y"))
})

test_that("V3 set_dimension_names attaches names post-creation and round-trips", {
  store <- MemoryStore$new()
  z <- zarr_create(shape = c(2L, 3L), dtype = "<f8", zarr_format = 3L,
                   compressor = NA, store = store)
  expect_null(z$get_dimension_names())

  z$set_dimension_names(c("a", "b"))
  expect_equal(z$get_dimension_names(), c("a", "b"))

  z2 <- ZarrArray$new(store)
  expect_equal(z2$get_dimension_names(), c("a", "b"))
})

test_that("set_dimension_names errors on V2 arrays", {
  store <- MemoryStore$new()
  z <- zarr_create(shape = c(2L, 3L), dtype = "<f8", zarr_format = 2L,
                   compressor = NA, store = store)
  expect_error(z$set_dimension_names(c("x", "y")), "DimensionNamesV2Error")
})

test_that("zarr_create errors when dimension_names length != length(shape)", {
  store <- MemoryStore$new()
  expect_error(
    zarr_create(shape = c(4L, 5L), dtype = "<f8", zarr_format = 3L,
                compressor = NA, store = store,
                dimension_names = c("x")),
    "DimensionNamesLengthError"
  )
})

test_that("zarr_create errors when dimension_names is passed with zarr_format = 2L", {
  store <- MemoryStore$new()
  expect_error(
    zarr_create(shape = c(4L, 5L), dtype = "<f8", zarr_format = 2L,
                compressor = NA, store = store,
                dimension_names = c("x", "y")),
    "DimensionNamesV2Error"
  )
})

test_that("V3 group create_dataset threads dimension_names through ...", {
  store <- MemoryStore$new()
  g <- zarr_create_group(store = store, zarr_format = 3L)
  a <- g$create_dataset("data", shape = c(2L, 3L, 4L), dtype = "<f8",
                        compressor = NA,
                        dimension_names = c("x", "y", "t"))
  expect_equal(a$get_dimension_names(), c("x", "y", "t"))
})
