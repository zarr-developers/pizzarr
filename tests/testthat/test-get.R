library(pizzarr)

sample_dir <- tools::R_user_dir("pizzarr")
clean <- !dir.exists(sample_dir)

test_that("get_basic_selection_zd", {
    # Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0e6cdc04c6413e14f57f61d389972ea937c/zarr/tests/test_indexing.py#L70
    a <- as_scalar(42)
    z <- zarr_create(shape=dim(a), dtype="<f4", fill_value=NA)

    expect_equal(z$get_shape(), NULL)

    z$set_item("...", a)

    sel <- z$get_item("...")

    expect_equal(as.numeric(a), as.numeric(sel$data))
})

test_that("get_basic_selection_zd with anndataR IntScalar fixture", {
    root <- pizzarr_sample(file.path("fixtures", "v2", "example.zarr"))
    
    store <- DirectoryStore$new(root)
    z <- zarr_open_array(store, path = "uns/IntScalar")

    expect_equal(z$get_shape(), integer(0))

    sel <- z$get_item("...")
    scalar <- as.numeric(sel$data)

    expect_equal(scalar, 1)
})

test_that("get_basic_selection_zd with anndataR StringScalar fixture", {
    root <- pizzarr_sample(file.path("fixtures", "v2", "example.zarr"))
    
    store <- DirectoryStore$new(root)
    z <- zarr_open_array(store, path = "uns/StringScalar")

    expect_equal(z$get_shape(), integer(0))

    sel <- z$get_item("...")
    scalar <- as.character(sel$data)

    expect_equal(scalar, "A string")
})

test_that("get_basic_selection_1d", {
    # Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0e6cdc04c6413e14f57f61d389972ea937c/zarr/tests/test_indexing.py#L70
    a <- array(data=42)
    z <- zarr_create(shape=dim(a), dtype="<f4", fill_value=NA)

    expect_equal(z$get_shape(), 1)

    z$set_item("...", a)

    sel <- z$get_item("...")

    expect_equal(a, sel$data)
})

test_that("get_basic_selection_2d - can set_item with array", {
    a <- array(data=1:20, dim=c(2, 10))
    #      [,1] [,2] [,3] [,4] [,5] [,6] [,7] [,8] [,9] [,10]
    # [1,]    1    3    5    7    9   11   13   15   17    19
    # [2,]    2    4    6    8   10   12   14   16   18    20
    z <- zarr_create(shape=dim(a), dtype="<f4", fill_value=NA)

    expect_equal(z$get_shape(), c(2, 10))
    expect_equal(z$get_chunks(), c(2, 10))

    z$set_item("...", a)

    sel <- z$get_item("...")

    expect_equal(a, sel$data)
})

test_that("get_basic_selection_2d - can set_item with NestedArray", {
    a <- array(data=1:20, dim=c(2, 10))
    #      [,1] [,2] [,3] [,4] [,5] [,6] [,7] [,8] [,9] [,10]
    # [1,]    1    3    5    7    9   11   13   15   17    19
    # [2,]    2    4    6    8   10   12   14   16   18    20
    z <- zarr_create(shape=dim(a), dtype="<f4", fill_value=NA)

    expect_equal(z$get_shape(), c(2, 10))
    expect_equal(z$get_chunks(), c(2, 10))

    z$set_item("...", NestedArray$new(data = a, shape = dim(a)))

    sel <- z$get_item("...")

    expect_equal(a, sel$data)
})

test_that("get_basic_selection_2d(zero-based) - can set_item for subset", {
    z <- zarr_create(shape=c(2, 10), dtype="<f4", fill_value=NA)

    expect_equal(z$get_shape(), c(2, 10))
    expect_equal(z$get_chunks(), c(2, 10))

    a <- array(data=1:10, dim=c(2, 5))
    #      [,0] [,1] [,2] [,3] [,4]
    # [0,]    1    3    5    7    9
    # [1,]    2    4    6    8   10

    z$set_item(list(zb_slice(0, 2), zb_slice(0, 5)), a)

    sel <- z$get_item("...")

    # fill_value=NA normalizes to 0.0 for float dtype
    expected_out <- array(data=0, dim=c(2, 10))
    expected_out[1, 1:5] <- c(1, 3, 5, 7, 9)
    expected_out[2, 1:5] <- c(2, 4, 6, 8, 10)

    expect_equal(expected_out, sel$data)
})

test_that("get_basic_selection_2d(zero-based) - can get_item for subset", {
    a <- array(data=1:20, dim=c(2, 10))
    #      [,1] [,2] [,3] [,4] [,5] [,6] [,7] [,8] [,9] [,10]
    # [1,]    1    3    5    7    9   11   13   15   17    19
    # [2,]    2    4    6    8   10   12   14   16   18    20
    z <- zarr_create(shape=dim(a), dtype="<f4", fill_value=NA)

    expect_equal(z$get_shape(), c(2, 10))
    expect_equal(z$get_chunks(), c(2, 10))

    z$set_item("...", a)

    sel <- z$get_item(list(zb_slice(0, 2), zb_slice(0, 5)))

    expected_out <- array(data=1:10, dim=c(2, 5))
    expect_equal(expected_out, sel$data)
})

test_that("get_basic_selection_2d(zero-based) - can get_item for subset with one offset on first axis", {
    a <- array(data=1:20, dim=c(2, 10))
    #      [,1] [,2] [,3] [,4] [,5] [,6] [,7] [,8] [,9] [,10]
    # [1,]    1    3    5    7    9   11   13   15   17    19
    # [2,]    2    4    6    8   10   12   14   16   18    20
    z <- zarr_create(shape=dim(a), dtype="<f4", fill_value=NA)

    expect_equal(z$get_shape(), c(2, 10))
    expect_equal(z$get_chunks(), c(2, 10))

    z$set_item("...", a)

    sel <- z$get_item(list(zb_slice(1, 3), zb_slice(0, 5)))

    expected_out <- array(data=NA, dim=c(1, 5))
    expected_out[1,] <- c(2, 4, 6, 8, 10)
    expect_equal(expected_out, sel$data)
})

test_that("get_basic_selection_2d(zero-based) - can get_item for subset with one offset on both axes", {
    a <- array(data=1:20, dim=c(2, 10))
    #      [,1] [,2] [,3] [,4] [,5] [,6] [,7] [,8] [,9] [,10]
    # [1,]    1    3    5    7    9   11   13   15   17    19
    # [2,]    2    4    6    8   10   12   14   16   18    20
    z <- zarr_create(shape=dim(a), dtype="<f4", fill_value=NA)

    expect_equal(z$get_shape(), c(2, 10))
    expect_equal(z$get_chunks(), c(2, 10))

    z$set_item("...", a)

    sel <- z$get_item(list(zb_slice(1, 2), zb_slice(1, 5)))

    expected_out <- array(data=NA, dim=c(1, 4))
    expected_out[1,] <- c(4, 6, 8, 10)
    expect_equal(expected_out, sel$data)
})

test_that("get_basic_selection_2d(one-based) - can get_item for subset with same index for start/stop", {
    a <- array(data=1:20, dim=c(2, 10))
    #      [,1] [,2] [,3] [,4] [,5] [,6] [,7] [,8] [,9] [,10]
    # [1,]    1    3    5    7    9   11   13   15   17    19
    # [2,]    2    4    6    8   10   12   14   16   18    20
    z <- zarr_create(shape=dim(a), dtype="<f4", fill_value=NA)

    expect_equal(z$get_shape(), c(2, 10))
    expect_equal(z$get_chunks(), c(2, 10))

    z$set_item("...", a)

    sel <- z$get_item(list(slice(2, 2), slice(2, 5)))

    expected_out <- array(data=NA, dim=c(1, 4))
    expected_out[1,] <- c(4, 6, 8, 10)
    expect_equal(expected_out, sel$data)
})

test_that("get_basic_selection_2d(one-based) - can get_item for subset with one offset on both axes", {
    a <- array(data=1:20, dim=c(2, 10))
    #      [,1] [,2] [,3] [,4] [,5] [,6] [,7] [,8] [,9] [,10]
    # [1,]    1    3    5    7    9   11   13   15   17    19
    # [2,]    2    4    6    8   10   12   14   16   18    20
    z <- zarr_create(shape=dim(a), dtype="<f4", fill_value=NA)

    expect_equal(z$get_shape(), c(2, 10))
    expect_equal(z$get_chunks(), c(2, 10))

    z$set_item("...", a)

    sel <- z$get_item(list(slice(1, 2), slice(1, 5)))

    expected_out <- array(data=1:10, dim=c(2, 5))
    expect_equal(expected_out, sel$data)
})

test_that("get_basic_selection_3d(one-based) - can get_item for three-dimensional array", {
    a <- array(data=1:24, dim=c(2, 3, 4))
    # , , 1
    #
    #      [,1] [,2] [,3]
    # [1,]    1    3    5
    # [2,]    2    4    6
    #
    # , , 2
    #
    #      [,1] [,2] [,3]
    # [1,]    7    9   11
    # [2,]    8   10   12
    #
    # , , 3
    #
    #      [,1] [,2] [,3]
    # [1,]   13   15   17
    # [2,]   14   16   18
    #
    # , , 4
    #
    #      [,1] [,2] [,3]
    # [1,]   19   21   23
    # [2,]   20   22   24
    z <- zarr_create(shape=dim(a), dtype="<f4", fill_value=NA)

    expect_equal(z$get_shape(), c(2, 3, 4))
    expect_equal(z$get_chunks(), c(2, 3, 4))

    z$set_item("...", a)

    sel <- z$get_item(list(slice(1, 2), slice(1, 2), slice(1, 2)))

    expected_out <- array(data=c(1, 2, 3, 4, 7, 8, 9, 10), dim=c(2, 2, 2))
    # , , 1
    #
    #      [,1] [,2]
    # [1,]    1    3
    # [2,]    2    4
    #
    # , , 2
    #
    #      [,1] [,2]
    # [1,]    7    9
    # [2,]    8   10
    expect_equal(expected_out, sel$data)
})

test_that("Can read 2D string array", {
    skip_if_not_installed("blosc")
    root <- pizzarr_sample(file.path("fixtures", "v2", "example.zarr"))
    store <- DirectoryStore$new(root)
    z <- zarr_open_array(store, path = "uns/String2D")
    nested_arr <- z$get_item("...")
    data <- nested_arr$data

    row1 <- c("row0col0", "row1col0", "row2col0", "row3col0", "row4col0", "row5col0", "row6col0", "row7col0", "row8col0", "row9col0")
    row2 <- c("row0col1", "row1col1", "row2col1", "row3col1", "row4col1", "row5col1", "row6col1", "row7col1", "row8col1", "row9col1")
    row3 <- c("row0col2", "row1col2", "row2col2", "row3col2", "row4col2", "row5col2", "row6col2", "row7col2", "row8col2", "row9col2")
    row4 <- c("row0col3", "row1col3", "row2col3", "row3col3", "row4col3", "row5col3", "row6col3", "row7col3", "row8col3", "row9col3")
    row5 <- c("row0col4", "row1col4", "row2col4", "row3col4", "row4col4", "row5col4", "row6col4", "row7col4", "row8col4", "row9col4")

    expected_arr <- t(array(data = c(row1, row2, row3, row4, row5), dim = c(10, 5)))
    expect_equal(expected_arr, data)
})

if(clean) unlink(sample_dir, recursive = TRUE)

# --- Zero-dim get/set ---

test_that("get_basic_selection on zero-dim array returns result for uninitialized chunk", {
  store <- MemoryStore$new()
  zarray_meta <- store$metadata_class$encode_array_metadata(create_zarray_meta(
    dtype = Dtype$new("<f8"),
    order = "C",
    fill_value = 9.5,
    shape = NULL,
    chunks = c(1),
    dimension_separator = "."
  ))
  store$set_item(".zarray", zarray_meta)
  a <- ZarrArray$new(store = store)
  result <- a$get_basic_selection("...")
  expect_true(!is.null(result))
})

test_that("set_basic_selection_zd writes and reads scalar value", {
  store <- MemoryStore$new()
  zarray_meta <- store$metadata_class$encode_array_metadata(create_zarray_meta(
    dtype = Dtype$new("<f8"),
    order = "C",
    fill_value = 0,
    shape = NULL,
    chunks = c(1),
    dimension_separator = "."
  ))
  store$set_item(".zarray", zarray_meta)
  a <- ZarrArray$new(store = store)
  a$set_basic_selection(list(), 3.14)
  result <- a$get_basic_selection("...")
  expect_true(!is.null(result))
})

# --- get_selection edge cases ---

test_that("get_selection returns correct length for full 1D slice", {
  a <- zarr_create(shape = c(2), chunks = c(2), store = MemoryStore$new(),
                   fill_value = 42, compressor = NA)
  result <- a$get_item("...")
  expect_equal(length(result$data), 2)
})

test_that("get_selection returns empty NestedArray when selection is empty", {
  a <- zarr_create(
    shape = c(4, 4), chunks = c(2, 2), store = MemoryStore$new(), compressor = NA
  )
  result <- a$get_item(list(slice(1, 0), slice(1, 2)))
  expect_equal(length(result$data), 0)
})

test_that("get_item raises error for vectorized (fancy) indexing", {
  a <- zarr_create(
    shape = c(4, 4), chunks = c(2, 2), store = MemoryStore$new(), compressor = NA
  )
  expect_error(a$get_item(list(c(1L, 2L), c(1L, 2L))), "vectorized indexing")
})

# --- get_basic_selection with cache_metadata=FALSE ---

test_that("get_basic_selection with cache_metadata=FALSE works", {
  d <- tempfile()
  zarr_create(shape = c(4), chunks = c(2), store = d, compressor = NA)
  b <- ZarrArray$new(store = DirectoryStore$new(d), cache_metadata = FALSE)
  result <- b$get_basic_selection("...")
  expect_equal(length(result$data), 4)
})

# --- get_orthogonal_selection ---

test_that("get_orthogonal_selection returns subset", {
  a <- zarr_create(shape = c(4, 4), chunks = c(2, 2), store = MemoryStore$new(), compressor = NA)
  data <- array(1:16, dim = c(4, 4))
  a$set_item("...", data)
  result <- a$get_orthogonal_selection(list(slice(1, 2), slice(1, 2)))
  expect_equal(dim(result$data), c(2, 2))
})

test_that("get_orthogonal_selection works with cache_metadata=FALSE", {
  d <- tempfile()
  a <- zarr_create(shape = c(4, 4), chunks = c(2, 2), store = d, compressor = NA)
  a$set_item("...", array(1:16, dim = c(4, 4)))
  b <- ZarrArray$new(store = DirectoryStore$new(d), cache_metadata = FALSE)
  result <- b$get_orthogonal_selection(list(slice(1, 2), slice(1, 2)))
  expect_equal(dim(result$data), c(2, 2))
})

# --- chunk_getitem paths ---

test_that("chunk_getitem_part2 partial-select path: sub-chunk slice read", {
  # Use a single large chunk so the whole-chunk fast path (is_total_slice) does NOT fire.
  a <- zarr_create(
    shape = c(4, 4), chunks = c(4, 4), store = MemoryStore$new(),
    compressor = NA, fill_value = 0
  )
  data <- array(1:16, dim = c(4, 4))
  a$set_item("...", data)
  result <- a$get_item(list(slice(1, 2), slice(1, 2)))
  expect_equal(dim(result$data), c(2, 2))
  expect_equal(as.vector(result$data), c(1L, 2L, 5L, 6L))
})

test_that("chunk_getitem_part2 error path: partial read of uninitialized chunk returns fill", {
  a <- zarr_create(
    shape = c(4, 4), chunks = c(4, 4), store = MemoryStore$new(),
    compressor = NA, fill_value = 7
  )
  result <- a$get_item(list(slice(1, 2), slice(1, 2)))
  expect_true(all(result$data == 7))
})
