library(pizzarr)

test_that("can convert a NestedArray to a raw vector with lz4 compression", {
    skip_if_not_installed("qs")
    a <- array(data=1:10, dim=c(2, 5))
    na <- NestedArray$new(data = a, shape = dim(a), dtype = "<u4")

    # Get un-compressed raw vector.
    na_raw <- na$flatten_to_raw()

    expect_equal(length(na_raw), 40)
    expected_out <- as.raw(c(
        0x01, 0x00, 0x00, 0x00,
        0x02, 0x00, 0x00, 0x00,
        0x03, 0x00, 0x00, 0x00,
        0x04, 0x00, 0x00, 0x00,
        0x05, 0x00, 0x00, 0x00,
        0x06, 0x00, 0x00, 0x00,
        0x07, 0x00, 0x00, 0x00,
        0x08, 0x00, 0x00, 0x00,
        0x09, 0x00, 0x00, 0x00,
        0x0a, 0x00, 0x00, 0x00
    ))
    expect_equal(na_raw, expected_out)

    # Compress the raw vector.
    compressor <- suppressWarnings(Lz4Codec$new(acceleration = 1))

    compressed_raw <- compressor$encode(na_raw)

    # Un-compress the compressed vector.
    uncompressed_raw <- compressor$decode(compressed_raw)

    expect_equal(uncompressed_raw, expected_out)
})

test_that("can convert a NestedArray to a raw vector with zstd compression", {
    a <- array(data=1:10, dim=c(2, 5))
    na <- NestedArray$new(data = a, shape = dim(a), dtype = "<u4")

    # Get un-compressed raw vector.
    na_raw <- na$flatten_to_raw()

    expect_equal(length(na_raw), 40)
    expected_out <- as.raw(c(
        0x01, 0x00, 0x00, 0x00,
        0x02, 0x00, 0x00, 0x00,
        0x03, 0x00, 0x00, 0x00,
        0x04, 0x00, 0x00, 0x00,
        0x05, 0x00, 0x00, 0x00,
        0x06, 0x00, 0x00, 0x00,
        0x07, 0x00, 0x00, 0x00,
        0x08, 0x00, 0x00, 0x00,
        0x09, 0x00, 0x00, 0x00,
        0x0a, 0x00, 0x00, 0x00
    ))
    expect_equal(na_raw, expected_out)

    # Compress the raw vector.
    compressor <- ZstdCodec$new(level = 1)

    compressed_raw <- compressor$encode(na_raw)

    # Un-compress the compressed vector.
    uncompressed_raw <- compressor$decode(compressed_raw)

    expect_equal(uncompressed_raw, expected_out)
})

test_that("zarr_create with non-default compressor", {
    skip_if_not_installed("qs")

    compressor <- suppressWarnings(Lz4Codec$new(acceleration = 1))

    a <- array(data=1:20, dim=c(2, 10))
    #      [,1] [,2] [,3] [,4] [,5] [,6] [,7] [,8] [,9] [,10]
    # [1,]    1    3    5    7    9   11   13   15   17    19
    # [2,]    2    4    6    8   10   12   14   16   18    20
    z <- zarr_create(shape=dim(a), dtype="<f4", fill_value=NA, compressor = compressor)

    expect_equal(z$get_shape(), c(2, 10))
    expect_equal(z$get_chunks(), c(2, 10))

    expect_equal(as.character(z$get_compressor()$get_config()$id), "lz4")

    z$set_item("...", a)

    sel <- z$get_item(list(slice(1, 2), slice(1, 5)))

    expected_out <- array(data=1:10, dim=c(2, 5))
    expect_equal(expected_out, sel$data)
})