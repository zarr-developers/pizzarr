# --- zarrs_set_subset: Rust write path tests ---

test_that("zarrs round-trip write+read 1D float64", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  d <- tempfile("zarrs_wr_1d_")
  z <- zarr_create(store = d, shape = 10L, chunks = 5L, dtype = "<f8")

  # Write via zarrs (through set_item dispatch)
  z$set_item("...", as.double(1:10))

  # Read back via zarrs
  result <- zarrs_get_subset(d, "", list(c(0L, 10L)), NULL)
  expect_equal(result$data, as.double(1:10))

  zarrs_close_store(d)
  unlink(d, recursive = TRUE)
})

test_that("zarrs round-trip write+read 2D int32", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  d <- tempfile("zarrs_wr_2d_")
  z <- zarr_create(store = d, shape = c(4L, 5L), chunks = c(4L, 5L),
                   dtype = "<i4")
  data <- array(1:20, dim = c(4, 5))
  z$set_item("...", data)

  # Read back via zarrs
  result <- zarrs_get_subset(d, "", list(c(0L, 4L), c(0L, 5L)), NULL)
  expect_equal(result$shape, c(4L, 5L))
  # zarrs returns F-order data; reshape directly
  arr <- array(result$data, dim = c(4, 5))
  expect_equal(arr, data)

  zarrs_close_store(d)
  unlink(d, recursive = TRUE)
})

test_that("zarrs write + R-native read cross-path", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  d <- tempfile("zarrs_wr_cross_")
  z <- zarr_create(store = d, shape = 10L, chunks = 5L, dtype = "<f8")

  # Write via zarrs (through set_item)
  z$set_item("...", as.double(11:20))

  # Close zarrs cache so R-native reads from disk
  zarrs_close_store(d)

  # Read via R-native by disabling zarrs
  old <- .pizzarr_env$zarrs_available
  .pizzarr_env$zarrs_available <- FALSE
  z2 <- zarr_open(store = d)
  result <- z2$get_item("...")
  .pizzarr_env$zarrs_available <- old

  expect_equal(as.double(result$data), as.double(11:20))

  unlink(d, recursive = TRUE)
})

test_that("zarrs partial subset overwrite", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  d <- tempfile("zarrs_wr_partial_")
  z <- zarr_create(store = d, shape = 10L, chunks = 5L, dtype = "<f8",
                   fill_value = 0.0)
  # Write full array first
  z$set_item("...", as.double(1:10))

  # Overwrite a slice [3:7] (1-based) = elements 3,4,5,6,7
  z$set_item(list(slice(3L, 7L)), as.double(c(99, 98, 97, 96, 95)))

  # Read back and verify
  result <- z$get_item("...")
  expected <- as.double(c(1, 2, 99, 98, 97, 96, 95, 8, 9, 10))
  expect_equal(as.double(result$data), expected)

  zarrs_close_store(d)
  unlink(d, recursive = TRUE)
})

test_that("zarrs writes int16 with narrowing", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  d <- tempfile("zarrs_wr_i16_")
  z <- zarr_create(store = d, shape = 5L, chunks = 5L, dtype = "<i2")

  # Write R integers to i16 array
  z$set_item("...", 1:5)

  result <- zarrs_get_subset(d, "", list(c(0L, 5L)), NULL)
  expect_type(result$data, "integer")
  expect_equal(result$data, 1:5)

  zarrs_close_store(d)
  unlink(d, recursive = TRUE)
})

test_that("zarrs 2D F-order to C-order conversion on write", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  d <- tempfile("zarrs_wr_forder_")
  z <- zarr_create(store = d, shape = c(3L, 4L), chunks = c(3L, 4L),
                   dtype = "<f8")
  # R array in F-order (column-major)
  data <- array(as.double(1:12), dim = c(3, 4))
  z$set_item("...", data)

  # Read back and compare zarrs vs R-native
  z2 <- zarr_open(store = d)
  zarrs_result <- z2$get_item("...")

  old <- .pizzarr_env$zarrs_available
  .pizzarr_env$zarrs_available <- FALSE
  r_result <- z2$get_item("...")
  .pizzarr_env$zarrs_available <- old

  expect_equal(zarrs_result$data, r_result$data)

  zarrs_close_store(d)
  unlink(d, recursive = TRUE)
})

test_that("zarrs write falls through for MemoryStore", {
  ms <- MemoryStore$new()
  z <- zarr_create(store = ms, shape = 5L, chunks = 5L, dtype = "<f8")

  # This should work via R-native path (MemoryStore has no store_id)
  z$set_item("...", as.double(1:5))
  result <- z$get_item("...")
  expect_equal(as.double(result$data), as.double(1:5))
})

test_that("zarrs write concurrent_target option propagated", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  d <- tempfile("zarrs_wr_ct_")
  z <- zarr_create(store = d, shape = 10L, chunks = 5L, dtype = "<f8")

  # Direct call with concurrent_target
  result <- zarrs_set_subset(d, "", list(c(0L, 10L)),
                               as.double(1:10), 1L)
  expect_true(result)

  # Verify data was written
  read_result <- zarrs_get_subset(d, "", list(c(0L, 10L)), NULL)
  expect_equal(read_result$data, as.double(1:10))

  zarrs_close_store(d)
  unlink(d, recursive = TRUE)
})
