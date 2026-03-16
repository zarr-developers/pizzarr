library(pizzarr)

sample_dir <- tools::R_user_dir("pizzarr")
clean <- !dir.exists(sample_dir)

test_that("demo data", {
  skip_if_not_installed("crul")

  demo_data <- pizzarr_sample()
  
  expect_true(all(dir.exists(demo_data)))
  
  expect_true(all(grepl("zarr$", demo_data)))

  expect_error(pizzarr_sample("borked"), "Dataset not found")
  
  expect_true(grepl("bcsd_v2.zarr", pizzarr_sample("bcsd")))
  expect_true(grepl("bcsd_v3.zarr", pizzarr_sample("bcsd_v3")))
  
})

test_that("create_zarray_meta does not throw error when simple dtype is valid", {
  res <- create_zarray_meta(dtype = Dtype$new("|u1"), order = "C", fill_value = 0, dimension_separator = ".")
  expect_equal(class(res$dtype)[[1]], "scalar")
  expect_equal(as.character(res$dtype), "|u1")
})

test_that("create_zarray_meta throws error when simple dtype byteorder is invalid", {
  f <- function() create_zarray_meta(dtype = Dtype$new(":u1"), order = "C", fill_value = 0, dimension_separator = ".")
  expect_error(f())
})

test_that("create_zarray_meta throws error when simple dtype basic type is invalid", {
  f <- function() create_zarray_meta(dtype = Dtype$new("<Z1"), order = "C", fill_value = 0, dimension_separator = ".")
  expect_error(f())
})

test_that("create_zarray_meta throws error when order is invalid", {
  f <- function() create_zarray_meta(dtype = Dtype$new("|u1"), order = "A", fill_value = 0, dimension_separator = ".")
  expect_error(f())
})

test_that("create_zarray_meta throws error when dimension separator is invalid", {
  f <- function() create_zarray_meta(dtype = Dtype$new("|u1"), order = "C", fill_value = 0, dimension_separator = "___")
  expect_error(f())
})


test_that("create_zarray_meta throws error when fill_value is invalid for float dtype", {
  f <- function() create_zarray_meta(dtype = Dtype$new("<f2"), order = "C", fill_value = "foo")
  expect_error(f())
})


test_that("zip_numeric", {
  res <- zip_numeric(c(1, 2, 3), c(4, 5, 6))
  expect_equal(res, list(
    c(1, 4),
    c(2, 5),
    c(3, 6)
  ))
})

# Reference: https://github.com/gzuidhof/zarr.js/blob/master/test/core/indexing.test.ts#L25
# [expected, selection, shape]

# 1D, single item
test_that("replace_ellipsis [[0], 0, [100]]", {
  res <- replace_ellipsis(as_scalar(0), list(100))
  expect_equal(res, list(0))
})

# 1D
test_that("replace_ellipsis [[null], ellipsis, [100]]", {
  res <- replace_ellipsis("...", list(100))
  expect_equal(res, list(NA))
})

test_that("replace_ellipsis [[null], [null], [100]]", {
  res <- replace_ellipsis(list(NA), list(100))
  expect_equal(res, list(NA))
})

test_that("replace_ellipsis [[null], [null, ellipsis], [100]]", {
  res <- replace_ellipsis(list(NA, "..."), list(100))
  expect_equal(res, list(NA))
})

test_that("replace_ellipsis [[null], [ellipsis, null], [100]]", {
  res <- replace_ellipsis(list("...", NA), list(100))
  expect_equal(res, list(NA))
})

test_that("replace_ellipsis [[zb_slice(0, 5)], zb_slice(0, 5), [100]]", {
  res <- replace_ellipsis(zb_slice(0, 5), list(100))
  expect_equal(res, list(zb_slice(0, 5)))
})

test_that("replace_ellipsis [[zb_slice(null)], zb_slice(:), [100]]", {
  res <- replace_ellipsis(zb_slice(":"), list(100))
  expect_equal(res, list(zb_slice(NA)))
  expect_equal(res[[1]]$start, NA)
  expect_equal(res[[1]]$stop, NA)
  expect_equal(res[[1]]$step, NA)
})

test_that("replace_ellipsis [[zb_slice(null)], zb_slice(:, :), [100]]", {
  res <- replace_ellipsis(zb_slice(":", ":"), list(100))
  expect_equal(res, list(zb_slice(NA)))
})

# 2D, single item
test_that("replace_ellipsis [[0, 0], [0, 0], [100, 100]]", {
  res <- replace_ellipsis(list(0, 0), list(100, 100))
  expect_equal(res, list(0, 0))
})

test_that("replace_ellipsis [[0, 0], [0, 0], [100, 100]] with as_scalar", {
  res <- replace_ellipsis(list(as_scalar(0), as_scalar(0)), list(100, 100))
  expect_equal(res, list(as_scalar(0), as_scalar(0)))
})

test_that("replace_ellipsis [[-1, 1], [-1, 1], [100, 100]]", {
  res <- replace_ellipsis(list(-1, 1), list(100, 100))
  expect_equal(res, list(-1, 1))
})

# 2D, single col/row
test_that("replace_ellipsis [[0, null], [0, null], [100, 100]]", {
  res <- replace_ellipsis(list(0, NA), list(100, 100))
  expect_equal(res, list(0, NA))
})

test_that("replace_ellipsis [[0, zb_slice(null)], [0, zb_slice(null)], [100, 100]]", {
  res <- replace_ellipsis(list(0, zb_slice(NA)), list(100, 100))
  expect_equal(res, list(0, zb_slice(NA)))
})

test_that("replace_ellipsis [[null, 0], [null, 0], [100, 100]]", {
  res <- replace_ellipsis(list(NA, 0), list(100, 100))
  expect_equal(res, list(NA, 0))
})

# 2D
test_that("replace_ellipsis [[null, null], ellipsis, [100, 100]]", {
  res <- replace_ellipsis("...", list(100, 100))
  expect_equal(res, list(NA, NA))
})

test_that("replace_ellipsis [[null, null], [null], [100, 100]]", {
  res <- replace_ellipsis(list(NA), list(100, 100))
  expect_equal(res, list(NA, NA))
})

test_that("replace_ellipsis [[null, null], [null, null], [100, 100]]", {
  res <- replace_ellipsis(list(NA, NA), list(100, 100))
  expect_equal(res, list(NA, NA))
})

test_that("replace_ellipsis [[null, null], [ellipsis, null], [100, 100]]", {
  res <- replace_ellipsis(list("...", NA), list(100, 100))
  expect_equal(res, list(NA, NA))
})

test_that("replace_ellipsis [[null, null], [null, ellipsis], [100, 100]]", {
  res <- replace_ellipsis(list(NA, "..."), list(100, 100))
  expect_equal(res, list(NA, NA))
})

test_that("replace_ellipsis [[null, zb_slice(null)], [ellipsis, zb_slice(null)], [100, 100]]", {
  res <- replace_ellipsis(list("...", zb_slice(NA)), list(100, 100))
  expect_equal(res, list(NA, zb_slice(NA)))
})

test_that("replace_ellipsis [[null, null], [ellipsis, null, null], [100, 100]]", {
  res <- replace_ellipsis(list("...", NA, NA), list(100, 100))
  expect_equal(res, list(NA, NA))
})

test_that("replace_ellipsis [[null, null], [null, ellipsis, null], [100, 100]]", {
  res <- replace_ellipsis(list(NA, "...", NA), list(100, 100))
  expect_equal(res, list(NA, NA))
})

test_that("replace_ellipsis [[null, null], [null, null, ellipsis], [100, 100]]", {
  res <- replace_ellipsis(list(NA, NA, "..."), list(100, 100))
  expect_equal(res, list(NA, NA))
})

# Failure tests for replace_ellipsis
test_that("replace_ellipsis errors with invalid input", {
  f1 <- function() replace_ellipsis(list("...", "..."), list(100, 100))
  expect_error(f1())
  f2 <- function() replace_ellipsis(list(0, 1, 2), list(100, 100))
  expect_error(f2())
})


test_that("get_list_product", {
  in_list <- list(
    list('A', 'B', 'C', 'D'),
    list('x', 'y')
  )
  out_list <- get_list_product(in_list)
  expect_equal(out_list, list(
    list('A', 'x'),
    list('B', 'x'),
    list('C', 'x'),
    list('D', 'x'),
    list('A', 'y'),
    list('B', 'y'),
    list('C', 'y'),
    list('D', 'y')
  ))
})

test_that("NaN in json", {
  check <- try_fromJSON('{"name": NaN}')
  
  expect_equal(check, list(name = NULL))
  
  expect_warning(try_fromJSON("borked", "test warning"), "test warning")
})

if(clean) unlink(sample_dir, recursive = TRUE)