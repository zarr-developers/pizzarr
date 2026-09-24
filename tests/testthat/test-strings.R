library(pizzarr)

test_that("NestedArray can be created from character array", {
    a <- array(data=c("a", "b", "c", "d"), dim=c(2, 2))
    na <- NestedArray$new(data = a, shape = dim(a), dtype = "|S1")

    selection <- na$get(list(slice(1, 2), slice(1, 2)))

    expect_equal(selection$data, a)
})

test_that("char_vec_to_raw works for S dtype, length 1", {
    a <- c("a", "b", "c", "d")
    raw_a <- char_vec_to_raw(a, "S", 1, "little")

    expect_equal(raw_a, as.raw(c(
        0x61,
        0x62,
        0x63,
        0x64
    )))
})

test_that("raw_to_char_vec works for S dtype, length 1", {
    char_vec <- raw_to_char_vec(as.raw(c(
        0x61,
        0x62,
        0x63,
        0x64
    )), "S", 1, "little")

    expect_equal(char_vec, c("a", "b", "c", "d"))
})

test_that("char_vec_to_raw works for S dtype, length 7", {
    a <- c("a", "b", "c", "d")
    raw_a <- char_vec_to_raw(a, "S", 7, "little")

    expect_equal(raw_a, as.raw(c(
        0x61, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x63, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
    )))
})

test_that("raw_to_char_vec works for S dtype, length 7", {
    char_vec <- raw_to_char_vec(as.raw(c(
        0x61, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x63, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
    )), "S", 7, "little")

    expect_equal(char_vec, c("a", "b", "c", "d"))
})

test_that("char_vec_to_raw works for U dtype, little endian", {
    a <- c("a", "b", "c", "d")
    raw_a <- char_vec_to_raw(a, "U", 2, "little")

    expect_equal(raw_a, as.raw(c(
        0x61, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x63, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
    )))
})

test_that("raw_to_char_vec works for U dtype, little endian", {
    char_vec <- raw_to_char_vec(as.raw(c(
        0x61, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x63, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
    )), "U", 2, "little")

    expect_equal(char_vec, c("a", "b", "c", "d"))
})

test_that("char_vec_to_raw works for U dtype, big endian", {
    a <- c("a", "b", "c", "d")
    raw_a <- char_vec_to_raw(a, "U", 2, "big")

    expect_equal(raw_a, as.raw(c(
        0x00, 0x00, 0x00, 0x61, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x62, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x63, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 0x00
    )))
})

test_that("NestedArray of characters can be created from raw array, S dtype", {
    a <- array(data=c("a", "b", "c", "d"), dim=c(4))
    na <- NestedArray$new(data = as.raw(c(
        0x61,
        0x62,
        0x63,
        0x64
    )), shape = dim(a), dtype = "|S1")

    selection <- na$get(list(slice(1, 4)))

    expect_equal(selection$data, a)
})

test_that("NestedArray of characters can be created from raw array, U dtype, little endian", {
    a <- array(data=c("a", "b", "c", "d"), dim=c(4))
    na <- NestedArray$new(data = as.raw(c(
        0x61, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x63, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x64, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
    )), shape = dim(a), dtype = "<U2")

    selection <- na$get(list(slice(1, 4)))

    expect_equal(selection$data, a)
})

test_that("NestedArray of characters can be created from raw array, U dtype, big endian", {
    a <- array(data=c("a", "b", "c", "d"), dim=c(4))
    na <- NestedArray$new(data = as.raw(c(
        0x00, 0x00, 0x00, 0x61, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x62, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x63, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 0x00
    )), shape = dim(a), dtype = ">U2")

    selection <- na$get(list(slice(1, 4)))

    expect_equal(selection$data, a)
})

test_that("NestedArray of strings can be converted to a raw array, S dtype", {
    a <- array(data=c("a", "b", "c", "d"), dim=c(4))
    na <- NestedArray$new(data = a, shape = dim(a), dtype = "|S1")

    na_as_raw <- na$flatten_to_raw()

    expect_equal(na_as_raw, as.raw(c(
        0x61,
        0x62,
        0x63,
        0x64
    )))
})
test_that("char_vec_to_raw leaves empty and NA strings null-filled (#112)", {
  buf <- char_vec_to_raw(c("ab", "", NA, "c"), "S", 3, "little")
  expect_equal(buf, as.raw(c(0x61, 0x62, 0, 0, 0, 0, 0, 0, 0, 0x63, 0, 0)))
  expect_equal(raw_to_char_vec(buf, "S", 3, "little"), c("ab", "", "", "c"))
})

test_that("char_vec_to_raw errors on U strings longer than the dtype", {
  expect_error(char_vec_to_raw("abc", "U", 2, "little"), "too long")
})

test_that("fixed-length string arrays with partial boundary chunks round-trip (#112)", {
  for (dtype in c("<S20", "<U20")) {
    mat <- array(rep("text", 545 * 689), dim = c(545, 689))
    g <- zarr_open(store = MemoryStore$new(), mode = "w")
    g$create_dataset(name = "assay", data = mat, shape = dim(mat), dtype = dtype)
    a <- g$get_item("assay")
    expect_false(all(dim(mat) %% a$get_chunks() == 0))
    expect_equal(a$get_item("...")$data, mat)
  }
})

test_that("dtype byte order is written the way numpy spells it", {
  cases <- c("<S20" = "|S20", ">S20" = "|S20", "|S20" = "|S20",
             "|U20" = "<U20", ">U20" = ">U20",
             "<b1" = "|b1", "<i1" = "|i1", ">u1" = "|u1",
             "|i2" = "<i2", ">f8" = ">f8", "<O" = "|O")
  for (d in names(cases)) expect_equal(canonical_dtype_str(d), cases[[d]], info = d)

  store <- MemoryStore$new()
  z <- zarr_create(shape = 3L, dtype = "<S5", store = store)
  meta <- jsonlite::fromJSON(rawToChar(store$get_item(".zarray")))
  expect_equal(meta$dtype, "|S5")
})
