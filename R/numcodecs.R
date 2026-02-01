#' Abstract compressor for Zarr
#' @title Codec Class
#' @docType class
#' @description
#' Abstract class representing a compressor.
#'
#' @rdname Codec
#' @export
Codec <- R6::R6Class("Codec",
   public = list(
     #' @description
     #' Compress data.
     #' @param buf The un-compressed data.
     #' @param zarr_arr The ZarrArray instance.
     #' @return Compressed data.
     encode = function(buf, zarr_arr) {
       return(buf)
     },
     #' @description
     #' Decompress data.
     #' @param buf The compressed data.
     #' @param zarr_arr The ZarrArray instance.
     #' @return Un-compressed data.
     decode = function(buf, zarr_arr) {
       return(buf)
     },
    #' @description
    #' Get Configuration
     get_config = function() {
       return(jsonlite::unbox(NA))
     }
   )
)

#' @keywords internal
warn_if_unk_args <- function(unk_args, compressor_name) {
  if(length(unk_args) > 0) {
    unk_args_str <- paste(names(unk_args), collapse = ", ")
    warning(paste(compressor_name, "received unrecognized compressor config parameters:", unk_args_str))
  }
}

#' ZSTD compressor for Zarr
#' @title ZstdCodec Class
#' @docType class
#' @description
#' Class representing a ZSTD compressor

#' @rdname ZstdCodec
#' @importFrom qs2 zstd_compress_raw zstd_decompress_raw
#' @export
ZstdCodec <- R6::R6Class("ZstdCodec",
  inherit = Codec,
  public = list(
    #' @field level The compression level.
    #' @keywords internal
    level = NULL,
    #' @description
    #' Create a new ZSTD compressor.
    #' @param level The compression level, between 1 and 22.
    #' @return A new `ZstdCodec` object.
    initialize = function(level = 1, ...) {
      self$level <- level
      warn_if_unk_args(list(...), "ZstdCodec")
    },
    #' @description
    #' Compress data.
    #' @param buf The un-compressed data.
    #' @param zarr_arr The ZarrArray instance.
    #' @return Compressed data.
    encode = function(buf, zarr_arr) {
      # Reference: https://github.com/traversc/qs/blob/84e30f4/R/RcppExports.R#L16
      result <- zstd_compress_raw(buf, self$level)
      return(result)
    },
    #' @description
    #' Decompress data.
    #' @param buf The compressed data.
    #' @param zarr_arr The ZarrArray instance.
    #' @return Un-compressed data.
    decode = function(buf, zarr_arr) {
      result <- zstd_decompress_raw(buf)
      return(result)
    },
    #' @description
    #' Get Configuration
    get_config = function() {
      meta <- list(
        id = jsonlite::unbox("zstd"),
        level = jsonlite::unbox(self$level)
      )
      return(meta)
    }
  )
)

#' LZ4 compressor for Zarr
#' @title Lz4Codec Class
#' @docType class
#' @description
#' Class representing a LZ4 compressor
#'
#' @section Deprecation Notice:
#' This codec is deprecated because the underlying `qs` package was archived on CRAN.
#' For LZ4 compression, use `BloscCodec$new(cname = "lz4")` instead.
#' This class will be removed in a future version.
#'
#' @rdname Lz4Codec
#' @export
Lz4Codec <- R6::R6Class("Lz4Codec",
   inherit = Codec,
   public = list(
     #' @field acceleration The compression level.
     #' @keywords internal
     acceleration = NULL,
     #' @description
     #' Create a new LZ4 compressor.
     #' @param acceleration The compression level.
     #' @return A new `Lz4Codec` object.
     initialize = function(acceleration = 1, ...) {
       .Deprecated(
         new = "BloscCodec$new(cname = 'lz4')",
         old = "Lz4Codec$new()",
         msg = paste(
           "Lz4Codec is deprecated because the 'qs' package was archived on CRAN.",
           "Use BloscCodec$new(cname = 'lz4') for LZ4 compression instead.",
           "Lz4Codec will be removed in a future version of pizzarr."
         )
       )
       self$acceleration <- acceleration
       warn_if_unk_args(list(...), "Lz4Codec")
     },
     #' @description
     #' Compress data.
     #' @param buf The un-compressed data.
     #' @param zarr_arr The ZarrArray instance.
     #' @return Compressed data.
     encode = function(buf, zarr_arr) {
       # Reference: https://github.com/traversc/qs/blob/84e30f4/R/RcppExports.R#L24
       if (!requireNamespace("qs", quietly = TRUE)) {
         stop(
           "Lz4Codec requires the archived 'qs' package. ",
           "Use BloscCodec$new(cname = 'lz4') instead for LZ4 compression."
         )
       }
       body <- qs::lz4_compress_raw(buf, self$acceleration)

       # The compressed output includes a 4-byte header storing the original size
       # of the decompressed data as a little-endian 32-bit integer.
       # Reference: https://numcodecs.readthedocs.io/en/stable/lz4.html#numcodecs.lz4.compress
       orig_size <- length(buf)
       header <- writeBin(orig_size, con = raw(), size = 4, endian = "little")

       result <- c(header, body)

       return(result)
     },
     #' @description
     #' Decompress data.
     #' @param buf The compressed data.
     #' @param zarr_arr The ZarrArray instance.
     #' @return Un-compressed data.
     decode = function(buf, zarr_arr) {
       if (!requireNamespace("qs", quietly = TRUE)) {
         stop(
           "Lz4Codec requires the archived 'qs' package. ",
           "Use BloscCodec$new(cname = 'lz4') instead for LZ4 compression."
         )
       }
       body <- buf[5:length(buf)]

       result <- qs::lz4_decompress_raw(body)
       return(result)
     },
     #' @description
     #' Get Configuration
     get_config = function() {
       meta <- list(
         id = jsonlite::unbox("lz4"),
         acceleration = jsonlite::unbox(self$acceleration)
       )
       return(meta)
     }
   )
)

#' Zlib compressor for Zarr
#' @title ZlibCodec Class
#' @docType class
#' @description
#' Class representing a zlib compressor
#'
#' @rdname ZlibCodec
#' @export
ZlibCodec <- R6::R6Class("ZlibCodec",
   inherit = Codec,
   public = list(
      #' @field level The compression level.
    level = NULL,
     #' @description
     #' Create a new Zlib compressor.
     #' @param level The compression level, between 1 and 22.
     #' @return A new `ZlibCodec` object.
     initialize = function(level = 6, ...) {
      self$level <- level
      # No config options for zlib.
      warn_if_unk_args(list(...), "ZlibCodec")
     },
    #' @description
    #' Compress data.
    #' @param buf The un-compressed data.
    #' @param zarr_arr The ZarrArray instance.
    #' @return Compressed data.
     encode = function(buf, zarr_arr) {
      if(self$level != 6) {
        stop("Only system default compression level (normally 6) is enabled for writing.")
      }
       # References:
       # - https://github.com/grimbough/Rarr/blob/684541a86b0313a6f354282b60a08dd0ea0a747d/R/write_data.R#L385
       # - https://stat.ethz.ch/R-manual/R-devel/library/base/html/memCompress.html
      result <- memCompress(buf, type = "gzip")
      return(result)
     },
    #' @description
    #' Decompress data.
    #' @param buf The compressed data.
    #' @param zarr_arr The ZarrArray instance.
    #' @return Un-compressed data.
     decode = function(buf, zarr_arr) {
      # References:
       # - https://github.com/grimbough/Rarr/blob/684541a86b0313a6f354282b60a08dd0ea0a747d/R/read_data.R#L356C5-L370C6
       # - https://stat.ethz.ch/R-manual/R-devel/library/base/html/memCompress.html
      result <- memDecompress(buf, type = "gzip", asChar = FALSE)
      return(result)
     },
    #' @description
    #' Get Configuration
     get_config = function() {
       meta <- list(
         id = jsonlite::unbox("zlib"),
         level = jsonlite::unbox(as.integer(self$level))
       )
       return(meta)
     }
   )
)

#' Gzip compressor for Zarr
#' @title GzipCodec Class
#' @docType class
#' @description
#' Class representing a gzip compressor
#'
#' @rdname GzipCodec
#' @export
GzipCodec <- R6::R6Class("GzipCodec",
   inherit = Codec,
   public = list(
      #' @field level The compression level.
      level = NULL,
     #' @description
     #' Create a new Gzip compressor.
     #' @param level The compression level, between 1 and 22.
     #' @return A new `GzipCodec` object.
     initialize = function(level = 6, ...) {
      # No config options for gzip.
      self$level <- level
      warn_if_unk_args(list(...), "GzipCodec")
     },
     #' @description
     #' Compress data.
     #' @param buf The un-compressed data.
     #' @param zarr_arr The ZarrArray instance.
     #' @return Compressed data.
     encode = function(buf, zarr_arr) {
      if(self$level != 6) {
        stop("Only system default compression level (normally 6) is enabled for writing.")
      }
       # References:
       # - https://github.com/grimbough/Rarr/blob/684541a86b0313a6f354282b60a08dd0ea0a747d/R/read_data.R#L356C5-L370C6
       # - https://stat.ethz.ch/R-manual/R-devel/library/base/html/memCompress.html
      result <- memCompress(buf, type = "gzip")
      return(result)
     },
     #' @description
     #' Decompress data.
     #' @param buf The compressed data.
     #' @param zarr_arr The ZarrArray instance.
     #' @return Un-compressed data.
     decode = function(buf, zarr_arr) {
      result <- memDecompress(buf, type = "gzip", asChar = FALSE)
      return(result)
     },
     #' @description
     #' Get Configuration
     get_config = function() {
       meta <- list(
          id = jsonlite::unbox("gzip"),
          level = jsonlite::unbox(as.integer(self$level))
       )
       return(meta)
     }
   )
)

#' Bz2 compressor for Zarr
#' @title Bz2Codec Class
#' @docType class
#' @description
#' Class representing a bz2 compressor
#'
#' @rdname Bz2Codec
#' @export
Bz2Codec <- R6::R6Class("Bz2Codec",
   inherit = Codec,
   public = list(
    #' @field level The compression level.
      level = NULL,
     #' @description
     #' Create a new Bz2 compressor.
     #' @param level The compression level, between 1 and 22.
     #' @return A new `Bz2Codec` object.
     initialize = function(level = 6, ...) {
      # No config options for bz2.
      self$level <- level
      warn_if_unk_args(list(...), "Bz2Codec")
     },
     #' @description
     #' Compress data.
     #' @param buf The un-compressed data.
     #' @param zarr_arr The ZarrArray instance.
     #' @return Compressed data.
     encode = function(buf, zarr_arr) {
       # References:
       # - https://github.com/grimbough/Rarr/blob/684541a86b0313a6f354282b60a08dd0ea0a747d/R/read_data.R#L356C5-L370C6
       # - https://stat.ethz.ch/R-manual/R-devel/library/base/html/memCompress.html
      if(self$level != 6) {
        stop("Only bzip2 compression level 6 is enabled for writing.")
      }
      result <- memCompress(buf, type = "bzip2")
      return(result)
     },
     #' @description
     #' Decompress data.
     #' @param buf The compressed data.
     #' @param zarr_arr The ZarrArray instance.
     #' @return Un-compressed data.
     decode = function(buf, zarr_arr) {
      result <- memDecompress(buf, type = "bzip2", asChar = FALSE)
      return(result)
     },
     #' @description
     #' Get Configuration
     get_config = function() {
       meta <- list(
         id = jsonlite::unbox("bz2"),
         level = jsonlite::unbox(as.integer(self$level))
       )
       return(meta)
     }
   )
)

#' Lzma compressor for Zarr
#' @title LzmaCodec Class
#' @docType class
#' @description
#' Class representing a lzma compressor
#'
#' @rdname LzmaCodec
#' @export
LzmaCodec <- R6::R6Class("LzmaCodec",
   inherit = Codec,
   public = list(
      #' @field level The compression level.
      level = NULL,
      #' @field format The compression format.
      format = NULL,
     #' @description
     #' Create a new lzma compressor.
     #' @param level The compression level, between 1 and 22.
     #' @param format only 1 is supported
     #' @return A new `LzmaCodec` object.
     initialize = function(level = 9, format = 1, ...) {
      # No config options for lzma.
      self$level <- level
      self$format <- format
      if(format != 1) {
        stop("Only format 1 is supported for lzma compression")
      }
      warn_if_unk_args(list(...), "LzmaCodec")
     },
     #' @description
     #' Compress data.
     #' @param buf The un-compressed data.
     #' @param zarr_arr The ZarrArray instance.
     #' @return Compressed data.
     encode = function(buf, zarr_arr) {
       # References:
       # - https://github.com/grimbough/Rarr/blob/684541a86b0313a6f354282b60a08dd0ea0a747d/R/read_data.R#L356C5-L370C6
      if(self$level != 9) {
        stop("Only lzma compression level 9 is enabled for writing.")
      }
       # - https://stat.ethz.ch/R-manual/R-devel/library/base/html/memCompress.html
      result <- memCompress(buf, type = "xz")
      return(result)
     },
     #' @description
     #' Decompress data.
     #' @param buf The compressed data.
     #' @param zarr_arr The ZarrArray instance.
     #' @return Un-compressed data.
     decode = function(buf, zarr_arr) {
      result <- memDecompress(buf, type = "xz", asChar = FALSE)
      return(result)
     },
     #' @description
     #' Get Configuration
     get_config = function() {
       meta <- list(
         id = jsonlite::unbox("lzma"),
          level = jsonlite::unbox(as.integer(self$level)),
          format = jsonlite::unbox(as.integer(self$format))
       )
       return(meta)
     }
   )
)

#' Blosc compressor for Zarr
#' @title BloscCodec Class
#' @docType class
#' @description
#' Class representing a Blosc compressor
#'
#' @rdname BloscCodec
#' @export
BloscCodec <- R6::R6Class("BloscCodec",
  inherit = Codec,
  public = list(
    #' @field cname The compression algorithm to use.
    cname = NULL,
    #' @field clevel The compression level.
    clevel = NULL,
    #' @field shuffle The shuffle filter to use.
    shuffle = NULL,
    #' @field blocksize The block size.
    blocksize = NULL,
    #' @description
    #' Create a new Blosc compressor.
    #' @param cname The compression algorithm to use.
    #' @param clevel The compression level.
    #' @param shuffle The shuffle filter to use.
    #' @param blocksize The block size.
    #' @return A new `BloscCodec` object.
    initialize = function(cname = "lz4", clevel = 5, shuffle = TRUE, blocksize = NA, ...) {
      self$cname <- cname
      self$clevel <- clevel
      self$shuffle <- shuffle
      self$blocksize <- blocksize # TODO: use
      # No config options for blosc.
      if (!require("Rarr", quietly = TRUE)) {
        stop("Rarr package must be installed to use the Blosc codec. Install with BiocManager::install('Rarr')")
      }
      warn_if_unk_args(list(...), "BloscCodec")
    },
    #' @description
    #' Compress data.
    #' @param buf The un-compressed data.
    #' @param zarr_arr The ZarrArray instance.
    #' @return Compressed data.
    encode = function(buf, zarr_arr) {
      if(self$cname != "lz4" || self$clevel != 5) {
        stop("Only lz4 compression level 5 is enabled for writing.")
      }
      dtype_str <- zarr_arr$get_dtype()
      dtype_size <- get_dtype_numbytes(dtype_str)
       result <- .Call(
        "compress_chunk_BLOSC",
        buf,
        as.integer(dtype_size),
        PACKAGE = "Rarr"
      )
      return(result)
    },
    #' @description
    #' Decompress data.
    #' @param buf The compressed data.
    #' @param zarr_arr The ZarrArray instance.
    #' @return Un-compressed data.
    decode = function(buf, zarr_arr) {
      result <- .Call(
        "decompress_chunk_BLOSC",
        buf,
        PACKAGE = "Rarr"
      )
      return(result)
    },
    #' @description
    #' Get Configuration
    get_config = function() {
       meta <- list(
         id = jsonlite::unbox("blosc"),
         cname = jsonlite::unbox(as.character(self$cname)),
         clevel = jsonlite::unbox(as.integer(self$clevel)),
         shuffle = jsonlite::unbox(as.integer(self$shuffle))
       )
       return(meta)
    }
  )
)

#' Variable-length UTF-8 codec for Zarr
#' @title VLenUtf8Codec Class
#' @docType class
#' @description
#' Class representing a VLenUtf8 compressor
#'
#' @rdname VLenUtf8Codec
#' @export
VLenUtf8Codec <- R6::R6Class("VLenUtf8Codec",
  inherit = Codec,
  public = list(
    #' @description
    #' Compress data.
    #' @param vec_of_strings The un-compressed data.
    #' @param zarr_arr The ZarrArray instance.
    #' @return Compressed data.
    encode = function(vec_of_strings, zarr_arr) {
      # Kind: array to bytes
      # Reference: https://github.com/zarr-developers/numcodecs/blob/cb155432e36536e17a2d054c8c24b7bf6f4a7347/numcodecs/vlen.pyx#L74

      num_strings <- length(vec_of_strings)

      encoded_values <- list()
      encoded_lengths <- integer(num_strings)

      data_length <- 0

      # first iteration to convert to bytes
      for(i in seq_len(num_strings)) {
        orig_str <- vec_of_strings[i]
        if(is.na(orig_str) || is.null(orig_str)) {
          # treat these as missing value, normalize
          orig_str <- ""
        }
        encoded_str <- charToRaw(orig_str)
        encoded_str_len <- length(encoded_str)
        encoded_values[[i]] <- encoded_str
        encoded_lengths[i] <- encoded_str_len
        data_length <- data_length + encoded_str_len + 4 # 4 bytes to store item length
      }

      # setup output
      total_length <- 4 + data_length # 4 bytes to store number of items in header
      out <- raw(total_length)

      # write header
      out[1:4] <- writeBin(
        num_strings,
        con = raw(),
        size = 4,
        endian = "little"
      )
      
      # second iteration, store data
      pos <- 4
      for(i in seq_len(num_strings)) {
        l <- encoded_lengths[i]
        out[(pos+1):(pos+4)] <- writeBin(
          l,
          con = raw(),
          size = 4,
          endian = "little"
        )
        pos <- pos + 4
        out[(pos+1):(pos+l)] <- encoded_values[[i]]
        pos <- pos + l
      }

      return(out)
    },
    #' @description
    #' Decompress data.
    #' @param buf The compressed data.
    #' @param zarr_arr The ZarrArray instance.
    #' @return Un-compressed data.
    decode = function(buf, zarr_arr) {
      # Kind: bytes to array
      # References:
      # - https://github.com/manzt/zarrita.js/blob/050d128265af14ff3c82e125315f3f527112887d/packages/core/src/codecs/vlen-utf8.ts
      # - https://github.com/zarr-developers/numcodecs/blob/cb155432e36536e17a2d054c8c24b7bf6f4a7347/numcodecs/vlen.pyx#L132

      num_strings <- readBin(
        con = buf,
        what = integer(),
        size = 4,
        n = 1,
        signed = TRUE,
        endian = "little"
      )

      vec_of_strings <- rep(NA, times = num_strings)

      pos <- 4
      for(i in seq_len(num_strings)) {
        num_chars <- readBin(
          con = buf[(pos+1):(pos+4)],
          what = integer(),
          size = 4,
          n = 1,
          signed = TRUE,
          endian = "little"
        )
        pos <- pos + 4
        vec_of_strings[i] <- rawToChar(buf[(pos+1):(pos+num_chars)])
        pos <- pos + num_chars
      }

      return(vec_of_strings)
    },
    #' @description
    #' Get Configuration
    get_config = function() {
       meta <- list(
         id = jsonlite::unbox("vlen-utf8")
       )
       return(meta)
    }
  )
)

#' Get a codec instance from the registry.
#'
#' @param config A codec config as a named list.
#' @return The instance of the codec.
get_codec <- function(config) {
  result <- Codec$new()
  if(!is_na(config)) {
    codec_id <- config$id
    config$id <- NULL
    if(codec_id == "lz4") {
      result <- do.call(Lz4Codec$new, config)
    } else if(codec_id == "zstd") {
      result <- do.call(ZstdCodec$new, config)
    } else if(codec_id == "zlib") {
      result <- do.call(ZlibCodec$new, config)
    } else if(codec_id == "gzip") {
      result <- do.call(GzipCodec$new, config)
    } else if(codec_id == "lzma") {
      result <- do.call(LzmaCodec$new, config)
    } else if(codec_id == "blosc") {
      result <- do.call(BloscCodec$new, config)
    } else if(codec_id == "vlen-utf8") {
      result <- do.call(VLenUtf8Codec$new, config)
    } else {
      stop(paste("Unknown codec", codec_id))
    }
  }
  return(result)
}

#' Get the default compressor.
#' @keywords internal
#' @returns A Zstd compressor instance.
get_default_compressor <- function() {
  return(ZstdCodec$new())
}