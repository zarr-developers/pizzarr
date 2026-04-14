test_that("get_store_identifier returns path for DirectoryStore", {
  d <- tempfile("zarrs_id_")
  dir.create(d)
  ds <- DirectoryStore$new(d)
  id <- ds$get_store_identifier()
  expect_type(id, "character")
  expect_equal(normalizePath(d, mustWork = FALSE), id)
  unlink(d, recursive = TRUE)
})

test_that("get_store_identifier returns NULL for MemoryStore", {
  ms <- MemoryStore$new()
  expect_null(ms$get_store_identifier())
})

test_that("zarrs_node_exists detects V2 array at root", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  d <- tempfile("zarrs_v2_arr_")
  z <- zarr_create(store = d, shape = 4L, dtype = "<i4")
  z$set_item("...", 1:4)

  result <- zarrs_node_exists(d, "")
  expect_true(result$exists)
  expect_equal(result$node_type, "array")
  expect_equal(result$zarr_format, 2L)

  zarrs_close_store(d)
  unlink(d, recursive = TRUE)
})

test_that("zarrs_node_exists detects V2 group", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  d <- tempfile("zarrs_v2_grp_")
  g <- zarr_create_group(store = d)

  result <- zarrs_node_exists(d, "")
  expect_true(result$exists)
  expect_equal(result$node_type, "group")
  expect_equal(result$zarr_format, 2L)

  zarrs_close_store(d)
  unlink(d, recursive = TRUE)
})

test_that("zarrs_node_exists detects V3 array at subpath", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  d <- tempfile("zarrs_v3_")
  store <- DirectoryStore$new(d)
  g <- zarr_create_group(store = store, zarr_format = 3L)
  a <- g$create_dataset("arr", shape = 4L, dtype = "<i4", compressor = NA)
  a$set_item("...", 1:4)

  result <- zarrs_node_exists(d, "arr")
  expect_true(result$exists)
  expect_equal(result$node_type, "array")
  expect_equal(result$zarr_format, 3L)

  zarrs_close_store(d)
  unlink(d, recursive = TRUE)
})

test_that("zarrs_node_exists returns exists=FALSE for missing path", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  d <- tempfile("zarrs_empty_")
  dir.create(d)

  result <- zarrs_node_exists(d, "nonexistent")
  expect_false(result$exists)
  expect_equal(result$node_type, "none")

  zarrs_close_store(d)
  unlink(d, recursive = TRUE)
})

test_that("zarrs_close_store returns TRUE then FALSE", {
  skip_if(!.pizzarr_env$zarrs_available, "zarrs backend not available")
  d <- tempfile("zarrs_close_")
  dir.create(d)

  # Opening store via zarrs_node_exists caches it.
  zarrs_node_exists(d, "")
  expect_true(zarrs_close_store(d))
  expect_false(zarrs_close_store(d))

  unlink(d, recursive = TRUE)
})
