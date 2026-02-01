library(pizzarr)

sample_dir <- tools::R_user_dir("pizzarr")
clean <- !dir.exists(sample_dir)

test_that("Can open Zarr group using convenience function", {
    skip_if_not_installed("qs")

    root <- pizzarr_sample(file.path("fixtures", "v2", "data.zarr"))

    g <- zarr_open_group(root)
    a <- suppressWarnings(g$get_item("1d.contiguous.lz4.i2"))

    expect_equal(a$get_shape(), c(4))
})

test_that("Can open Zarr group or array using convenience function", {
    skip_if_not_installed("qs")

    root <- pizzarr_sample(file.path("fixtures", "v2", "data.zarr"))
    g <- zarr_open(root)
    a <- suppressWarnings(zarr_open(root, path="1d.contiguous.lz4.i2"))

    expect_equal(class(g)[1], "ZarrGroup")
    expect_equal(class(a)[1], "ZarrArray")
})

test_that("Can open Zarr group and read a 1D 2-byte integer array with LZ4 compression", {
    skip_if_not_installed("qs")

    root <- pizzarr_sample(file.path("fixtures", "v2", "data.zarr"))

    store <- DirectoryStore$new(root)
    g <- ZarrGroup$new(store)
    a <- suppressWarnings(g$get_item("1d.contiguous.lz4.i2"))

    expect_equal(a$get_shape(), c(4))
    expect_equal(a$get_chunks(), c(4))

    expect_equal(as.character(a$get_compressor()$get_config()$id), "lz4")

    selection <- a$get_item("...")

    expect_equal(dim(selection$data), c(4))
    expect_equal(selection$data, array(data=c(1, 2, 3, 4), dim=c(4)))
})

test_that("Can open Zarr group and read a 1D 2-byte integer array with Zstd compression", {

    root <- pizzarr_sample(file.path("fixtures", "v2", "data.zarr"))
    
    store <- DirectoryStore$new(root)
    g <- ZarrGroup$new(store)
    a <- g$get_item("1d.contiguous.zstd.i2")

    expect_equal(a$get_shape(), c(4))
    expect_equal(a$get_chunks(), c(4))

    expect_equal(as.character(a$get_compressor()$get_config()$id), "zstd")

    selection <- a$get_item("...")

    expect_equal(dim(selection$data), c(4))
    expect_equal(selection$data, array(data=c(1, 2, 3, 4), dim=c(4)))
})

test_that("Can open Zarr group and read a 1D 2-byte integer array with Blosc compression", {

    root <- pizzarr_sample(file.path("fixtures", "v2", "data.zarr"))
    
    store <- DirectoryStore$new(root)
    g <- ZarrGroup$new(store)

    f <- function() {
        a <- g$get_item("1d.contiguous.blosc.i2")

        expect_equal(a$get_shape(), c(4))
        expect_equal(a$get_chunks(), c(4))

        expect_equal(as.character(a$get_compressor()$get_config()$id), "blosc")

        selection <- a$get_item("...")

        expect_equal(dim(selection$data), c(4))
        expect_equal(selection$data, array(data=c(1, 2, 3, 4), dim=c(4)))
    }

    if(requireNamespace("Rarr", quietly=TRUE)) {
        f()
    } else {
        expect_error(f())
    }
})

test_that("Can open Zarr group and read a 1D 2-byte integer array with Zlib compression", {

    root <- pizzarr_sample(file.path("fixtures", "v2", "data.zarr"))
    
    store <- DirectoryStore$new(root)
    g <- ZarrGroup$new(store)
    a <- g$get_item("1d.contiguous.zlib.i2")

    expect_equal(a$get_shape(), c(4))
    expect_equal(a$get_chunks(), c(4))

    expect_equal(as.character(a$get_compressor()$get_config()$id), "zlib")

    selection <- a$get_item("...")

    expect_equal(dim(selection$data), c(4))
    expect_equal(selection$data, array(data=c(1, 2, 3, 4), dim=c(4)))
})

test_that("Can open Zarr group and read a 1D 2-byte integer array with no compression", {

    root <- pizzarr_sample(file.path("fixtures", "v2", "data.zarr"))
    
    store <- DirectoryStore$new(root)
    g <- ZarrGroup$new(store)
    a <- g$get_item("1d.contiguous.raw.i2")

    expect_equal(a$get_shape(), c(4))
    expect_equal(a$get_chunks(), c(4))

    expect_equal(is.na(a$get_compressor()), TRUE)

    selection <- a$get_item("...")

    expect_equal(dim(selection$data), c(4))
    expect_equal(selection$data, array(data=c(1, 2, 3, 4), dim=c(4)))
})

test_that("Can open Zarr group and read a 1D 4-byte integer array", {

    root <- pizzarr_sample(file.path("fixtures", "v2", "data.zarr"))
    
    store <- DirectoryStore$new(root)
    g <- ZarrGroup$new(store)
    a <- g$get_item("1d.contiguous.i4")

    expect_equal(a$get_shape(), c(4))
    expect_equal(a$get_chunks(), c(4))

    selection <- a$get_item("...")

    expect_equal(dim(selection$data), c(4))
    expect_equal(selection$data, array(data=c(1, 2, 3, 4), dim=c(4)))
})

test_that("Can open Zarr group and read a 1D 1-byte unsigned integer array", {

    root <- pizzarr_sample(file.path("fixtures", "v2", "data.zarr"))
    
    store <- DirectoryStore$new(root)
    g <- ZarrGroup$new(store)
    a <- g$get_item("1d.contiguous.u1")

    expect_equal(a$get_shape(), c(4))
    expect_equal(a$get_chunks(), c(4))

    selection <- a$get_item("...")

    expect_equal(dim(selection$data), c(4))
    expect_equal(selection$data, array(data=c(255, 0, 255, 0), dim=c(4)))
})

test_that("Can open Zarr group and read a 1D 4-byte float array, little endian", {

    root <- pizzarr_sample(file.path("fixtures", "v2", "data.zarr"))
    
    store <- DirectoryStore$new(root)
    g <- ZarrGroup$new(store)
    a <- g$get_item("1d.contiguous.f4.le")

    expect_equal(a$get_shape(), c(4))
    expect_equal(a$get_chunks(), c(4))

    selection <- a$get_item("...")

    expect_equal(dim(selection$data), c(4))
    expect_equal(selection$data, array(data=c(-1000.5, 0, 1000.5, 0), dim=c(4)))
})

test_that("Can open Zarr group and read a 1D 4-byte float array, big endian", {

    root <- pizzarr_sample(file.path("fixtures", "v2", "data.zarr"))
    
    store <- DirectoryStore$new(root)
    g <- ZarrGroup$new(store)
    a <- g$get_item("1d.contiguous.f4.be")

    expect_equal(a$get_shape(), c(4))
    expect_equal(a$get_chunks(), c(4))

    selection <- a$get_item("...")

    expect_equal(dim(selection$data), c(4))
    expect_equal(selection$data, array(data=c(-1000.5, 0, 1000.5, 0), dim=c(4)))
})

test_that("Can open Zarr group and read a 1D 8-byte float array", {

    root <- pizzarr_sample(file.path("fixtures", "v2", "data.zarr"))
    
    store <- DirectoryStore$new(root)
    g <- ZarrGroup$new(store)
    a <- g$get_item("1d.contiguous.f8")

    expect_equal(a$get_shape(), c(4))
    expect_equal(a$get_chunks(), c(4))

    selection <- a$get_item("...")

    expect_equal(dim(selection$data), c(4))
    expect_equal(selection$data, array(data=c(1.5, 2.5, 3.5, 4.5), dim=c(4)))
})

test_that("Can open Zarr group and read a 1D 2-byte float array, 2 chunks", {

    root <- pizzarr_sample(file.path("fixtures", "v2", "data.zarr"))
    
    store <- DirectoryStore$new(root)
    g <- ZarrGroup$new(store)
    a <- g$get_item("1d.chunked.i2")

    expect_equal(a$get_shape(), c(4))
    expect_equal(a$get_chunks(), c(2))

    selection <- a$get_item("...")

    expect_equal(dim(selection$data), c(4))
    expect_equal(selection$data, array(data=c(1, 2, 3, 4), dim=c(4)))
})

test_that("Can open Zarr group and read a 1D 2-byte float array, 2 chunks, ragged", {

    root <- pizzarr_sample(file.path("fixtures", "v2", "data.zarr"))
    
    store <- DirectoryStore$new(root)
    g <- ZarrGroup$new(store)
    a <- g$get_item("1d.chunked.ragged.i2")

    expect_equal(a$get_shape(), c(5))
    expect_equal(a$get_chunks(), c(2))

    selection <- a$get_item("...")

    expect_equal(dim(selection$data), c(5))
    expect_equal(selection$data, array(data=c(1, 2, 3, 4, 5), dim=c(5)))
})

test_that("Can open Zarr group and read a 2D 2-byte integer array", {

    root <- pizzarr_sample(file.path("fixtures", "v2", "data.zarr"))
    
    store <- DirectoryStore$new(root)
    g <- ZarrGroup$new(store)
    a <- g$get_item("2d.contiguous.i2")

    expect_equal(a$get_shape(), c(2, 2))
    expect_equal(a$get_chunks(), c(2, 2))

    selection <- a$get_item("...")

    expect_equal(dim(selection$data), c(2, 2))
    expect_equal(selection$data[1,], c(1, 2))
    expect_equal(selection$data[2,], c(3, 4))
})

test_that("Can open Zarr group and read a 2D 2-byte integer array, 2 chunks", {

    root <- pizzarr_sample(file.path("fixtures", "v2", "data.zarr"))
    
    store <- DirectoryStore$new(root)
    g <- ZarrGroup$new(store)
    a <- g$get_item("2d.chunked.i2")

    expect_equal(a$get_shape(), c(2, 2))
    expect_equal(a$get_chunks(), c(1, 1))

    selection <- a$get_item("...")

    expect_equal(dim(selection$data), c(2, 2))
    expect_equal(selection$data[1,], c(1, 2))
    expect_equal(selection$data[2,], c(3, 4))
})

test_that("Can open Zarr group and read a 2D 2-byte integer array, 2 chunks, ragged", {

    root <- pizzarr_sample(file.path("fixtures", "v2", "data.zarr"))
    
    store <- DirectoryStore$new(root)
    g <- ZarrGroup$new(store)
    a <- g$get_item("2d.chunked.ragged.i2")

    expect_equal(a$get_shape(), c(3, 3))
    expect_equal(a$get_chunks(), c(2, 2))

    selection <- a$get_item("...")

    expect_equal(dim(selection$data), c(3, 3))
    expect_equal(selection$data[1,], c(1, 2, 3))
    expect_equal(selection$data[2,], c(4, 5, 6))
    expect_equal(selection$data[3,], c(7, 8, 9))
})


test_that("Can open Zarr group and read a 1D 1-byte boolean array", {

    root <- pizzarr_sample(file.path("fixtures", "v2", "data.zarr"))
    
    store <- DirectoryStore$new(root)
    g <- ZarrGroup$new(store)
    a <- g$get_item("1d.contiguous.b1")

    expect_equal(a$get_shape(), c(4))
    expect_equal(a$get_chunks(), c(4))

    selection <- a$get_item("...")

    expect_equal(dim(selection$data), c(4))
    expect_equal(selection$data, array(data=c(TRUE, FALSE, TRUE, FALSE), dim=c(4)))
})

test_that("Can open Zarr group and read a 1D S7 string array", {

    root <- pizzarr_sample(file.path("fixtures", "v2", "data.zarr"))
    
    store <- DirectoryStore$new(root)
    g <- ZarrGroup$new(store)
    a <- g$get_item("1d.contiguous.S7")

    expect_equal(a$get_shape(), c(4))
    expect_equal(a$get_chunks(), c(4))

    selection <- a$get_item("...")

    expect_equal(dim(selection$data), c(4))
    expect_equal(selection$data, array(data=c("a", "b", "cc", "d"), dim=c(4)))
})

test_that("Can open Zarr group and read a 1D U7 string array", {

    root <- pizzarr_sample(file.path("fixtures", "v2", "data.zarr"))
    
    store <- DirectoryStore$new(root)
    g <- ZarrGroup$new(store)
    a <- g$get_item("1d.contiguous.U7")

    expect_equal(a$get_shape(), c(4))
    expect_equal(a$get_chunks(), c(4))

    selection <- a$get_item("...")

    expect_equal(dim(selection$data), c(4))
    expect_equal(selection$data, array(data=c("a", "b", "cc", "d"), dim=c(4)))
})

test_that("Can open Zarr group and read a 1D U13 little endian string array", {

    root <- pizzarr_sample(file.path("fixtures", "v2", "data.zarr"))
    
    store <- DirectoryStore$new(root)
    g <- ZarrGroup$new(store)
    a <- g$get_item("1d.contiguous.U13.le")

    expect_equal(a$get_shape(), c(4))
    expect_equal(a$get_chunks(), c(4))

    selection <- a$get_item("...")

    expect_equal(dim(selection$data), c(4))
    expect_equal(selection$data, array(data=c("a", "b", "cc", "d"), dim=c(4)))
})

test_that("Can open Zarr group and read a 1D U13 big endian string array", {

    root <- pizzarr_sample(file.path("fixtures", "v2", "data.zarr"))
    
    store <- DirectoryStore$new(root)
    g <- ZarrGroup$new(store)
    a <- g$get_item("1d.contiguous.U13.be")

    expect_equal(a$get_shape(), c(4))
    expect_equal(a$get_chunks(), c(4))

    selection <- a$get_item("...")

    expect_equal(dim(selection$data), c(4))
    expect_equal(selection$data, array(data=c("a", "b", "cc", "d"), dim=c(4)))
})

test_that("Can open Zarr group and read a 2D U7 string array", {

    root <- pizzarr_sample(file.path("fixtures", "v2", "data.zarr"))
    
    store <- DirectoryStore$new(root)
    g <- ZarrGroup$new(store)
    a <- g$get_item("2d.chunked.U7")

    expect_equal(a$get_shape(), c(2, 2))
    expect_equal(a$get_chunks(), c(1, 1))

    selection <- a$get_item("...")

    expect_equal(dim(selection$data), c(2, 2))
    expect_equal(selection$data, array(data=c("a", "cc", "b", "d"), dim=c(2, 2)))
})

test_that("Can open Zarr group and read a 1D VLen-UTF8 string array with no compression", {

    root <- pizzarr_sample(file.path("fixtures", "v2", "data.zarr"))
    
    store <- DirectoryStore$new(root)
    g <- ZarrGroup$new(store)
    a <- g$get_item("1d.contiguous.raw.vlen-utf8")

    expect_equal(a$get_shape(), c(4))
    expect_equal(a$get_chunks(), c(4))

    selection <- a$get_item("...")

    expect_equal(dim(selection$data), c(4))
    expect_equal(selection$data, array(data=c("a", "b", "cc", "d"), dim=c(4)))
})

test_that("Can open Zarr group and read a 1D VLen-UTF8 string array with Blosc compression", {

    root <- pizzarr_sample(file.path("fixtures", "v2", "data.zarr"))
    
    store <- DirectoryStore$new(root)
    g <- ZarrGroup$new(store)
    a <- g$get_item("1d.contiguous.blosc.vlen-utf8")

    expect_equal(a$get_shape(), c(4))
    expect_equal(a$get_chunks(), c(4))

    selection <- a$get_item("...")

    expect_equal(dim(selection$data), c(4))
    expect_equal(selection$data, array(data=c("a", "b", "cc", "d"), dim=c(4)))
})

test_that("Can open Zarr group and read a 2D VLen-UTF8 string array with no compression", {
    root <- pizzarr_sample(file.path("fixtures", "v2", "data.zarr"))
    
    store <- DirectoryStore$new(root)
    g <- ZarrGroup$new(store)
    a <- g$get_item("2d.chunked.raw.vlen-utf8")

    expect_equal(a$get_shape(), c(2, 2))
    expect_equal(a$get_chunks(), c(1, 1))

    selection <- a$get_item("...")

    expect_equal(dim(selection$data), c(2, 2))
    expect_equal(selection$data, array(data=c("a", "cc", "b", "d"), dim=c(2, 2)))
})

test_that("Can open Zarr group and read a 2D VLen-UTF8 string array with Blosc compression", {
    root <- pizzarr_sample(file.path("fixtures", "v2", "data.zarr"))
    
    store <- DirectoryStore$new(root)
    g <- ZarrGroup$new(store)
    a <- g$get_item("2d.chunked.blosc.vlen-utf8")

    expect_equal(a$get_shape(), c(2, 2))
    expect_equal(a$get_chunks(), c(1, 1))

    selection <- a$get_item("...")

    expect_equal(dim(selection$data), c(2, 2))
    expect_equal(selection$data, array(data=c("a", "cc", "b", "d"), dim=c(2, 2)))
})

test_that("DirectoryStore can listdir", {
    root <- pizzarr_sample(file.path("fixtures", "v2", "data.zarr"))
    
    store <- DirectoryStore$new(root)
    
    expect_equal(store$listdir("1d.chunked.i2"), c(".zarray", "0", "1"))
})

test_that("Can create 0-element 1D array", {

    store <- MemoryStore$new()
    value <- character(0)
    dims <- c(0)
    name <- "my-0-element-array-1d"

    object_codec <- VLenUtf8Codec$new()
    data <- array(data = value, dim = dims)
    a <- zarr_create_array(data, store = store, path = name, dtype = "|O", object_codec = object_codec, shape = dims)

    expect_equal(a$get_shape(), c(0))

    sel <- a$get_item("...")
    
    expect_equal(is.character(sel$data), TRUE)
    expect_equal(length(sel$data), 0)
    expect_equal(dim(sel$data), c(0))
})

test_that("Can create 0-element 2D array", {

    store <- MemoryStore$new()
    value <- character(0)
    dims <- c(0, 0)
    name <- "my-0-element-array-2d"

    object_codec <- VLenUtf8Codec$new()
    data <- array(data = value, dim = dims)
    a <- zarr_create_array(data, store = store, path = name, dtype = "|O", object_codec = object_codec, shape = dims)

    expect_equal(a$get_shape(), c(0, 0))

    sel <- a$get_item("...")
    
    expect_equal(is.character(sel$data), TRUE)
    expect_equal(length(sel$data), 0)
    expect_equal(dim(sel$data), c(0, 0))
})

if(clean) unlink(sample_dir, recursive = TRUE)
