library(pizzarr)

MockArray <- R6::R6Class("MockArray",
    public = list(
        shape = NULL,
        chunks = NULL,
        initialize = function(shape, chunks) {
            self$shape <- shape
            self$chunks <- chunks
        },
        get_shape = function() {
            return(self$shape)
        },
        get_chunks = function() {
            return(self$chunks)
        }
    )
)

test_that("VLenUTF8 codec can decode - raw to array", {
  codec <- VLenUtf8Codec$new()
  zarr_arr <- MockArray$new(c(4), c(4))
  # ['a', 'b', 'cc', 'd']
  str_arr <- codec$decode(as.raw(c(
    4, 0, 0, 0,
    1, 0, 0, 0,
    97, 1, 0, 0,
    0, 98, 2, 0,
    0, 0, 99, 99,
    1, 0, 0, 0,
    100
  )), zarr_arr)

  expect_equal(str_arr, c("a", "b", "cc", "d"))
})

test_that("VLenUTF8 codec can encode - array to raw", {
  codec <- VLenUtf8Codec$new()
  zarr_arr <- MockArray$new(c(4), c(4))
  # ['a', 'b', 'cc', 'd']
  raw_vec <- codec$encode(c("a", "b", "cc", "d"), zarr_arr)

  expect_equal(raw_vec, as.raw(c(
    4, 0, 0, 0,
    1, 0, 0, 0,
    97, 1, 0, 0,
    0, 98, 2, 0,
    0, 0, 99, 99,
    1, 0, 0, 0,
    100
  )))
})

test_that("VLenUTF8 codec round-trips empty strings", {
  codec <- VLenUtf8Codec$new()
  x <- c("", "a", "", "bc", "")
  raw_vec <- codec$encode(x, NULL)
  expect_equal(length(raw_vec), 4 + 5 * 4 + 3)
  expect_equal(codec$decode(raw_vec, NULL), x)
})

test_that("VLenUTF8 codec marks decoded strings as UTF-8", {
  codec <- VLenUtf8Codec$new()
  x <- codec$decode(codec$encode(c("héllo", "水"), NULL), NULL)
  expect_equal(Encoding(x), c("UTF-8", "UTF-8"))
  expect_equal(x, c("héllo", "水"))
})

test_that("VLenUTF8 codec writes latin1-marked strings as UTF-8", {
  codec <- VLenUtf8Codec$new()
  x <- iconv("hé", from = "UTF-8", to = "latin1")
  expect_equal(Encoding(x), "latin1")
  raw_vec <- codec$encode(x, NULL)
  expect_equal(raw_vec[9:11], as.raw(c(0x68, 0xc3, 0xa9)))
})

test_that("VLenUTF8 codec can encode with high level API", {
  store <- MemoryStore$new()
  object_codec <- VLenUtf8Codec$new()

  string <- LETTERS[1:5]
  dims <- length(string)
  data <- array(data = string, dim = dims)

  z <- pizzarr::zarr_create_array(data, store = store, path = "string", dtype = "|O", object_codec = object_codec, shape = dims)
  
  sel <- z$get_item("...")
  expect_equal(as.character(sel$data), string)
})
