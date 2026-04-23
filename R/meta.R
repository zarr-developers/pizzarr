
# Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0e6cdc04c6413e14f57f61d389972ea937c/zarr/meta.py#L14

#' @title Zarr V2 Metadata Codec
#'
#' @description
#' Handles encoding and decoding of Zarr V2 metadata. Provides methods
#' to parse raw JSON bytes into R lists and to serialize metadata back
#' to JSON bytes, for both array and group nodes. Array metadata includes
#' shape, chunks, dtype, compressor, fill_value, order, and filters.
#' Group metadata contains only the zarr_format field.
#'
#' @format [R6::R6Class] object.
#'
#' @family Metadata classes
#' @keywords internal
Metadata2 <- R6::R6Class("Metadata2",
    private = list(
        ZARR_FORMAT = 2
    ),
    public = list(
        #' @description
        #' Parse metadata from raw bytes or return as-is if already a list.
        #'
        #' @param s (`raw()` | `list()` | `NULL`)\cr
        #'   Raw JSON bytes, a pre-parsed list, or `NULL`.
        #' @param auto_unbox (`logical(1)`)\cr
        #'   Passed to [jsonlite::toJSON()]. Default `FALSE`.
        #' @return `list()` or `NULL`.
        decode_metadata = function(s, auto_unbox=FALSE) {
            if(is.list(s) || is.null(s)) {
                return(s)
            } else {
                return(try_fromJSON(rawToChar(s), simplifyVector = FALSE))
            }
        },
        #' @description
        #' Encode metadata list to raw JSON bytes.
        #'
        #' @param meta (`list()`)\cr
        #'   Metadata to encode.
        #' @param auto_unbox (`logical(1)`)\cr
        #'   Passed to [jsonlite::toJSON()]. Default `FALSE`.
        #' @return `raw()`.
        encode_metadata = function(meta, auto_unbox=FALSE) {
            return(charToRaw(jsonlite::toJSON(meta, auto_unbox = auto_unbox)))
        },
        #' @description
        #' Decode and validate V2 array metadata.
        #'
        #' @param s (`raw()` | `list()` | `NULL`)\cr
        #'   Raw JSON bytes or pre-parsed list.
        #' @return `list()` or `NULL`.
        decode_array_metadata = function(s) {
            meta <- self$decode_metadata(s)
            if(!is.null(meta)) validate_v2_meta(meta)
            return(meta)
        },
        #' @description
        #' Decode and validate V2 group metadata.
        #'
        #' @param s (`raw()` | `list()` | `NULL`)\cr
        #'   Raw JSON bytes or pre-parsed list.
        #' @return `list()` or `NULL`.
        decode_group_metadata = function(s) {
            meta <- self$decode_metadata(s)
            if(!is.null(meta)) validate_v2_meta(meta)
            return(meta)
        },
        #' @description
        #' Encode array metadata to raw JSON bytes, setting zarr_format to 2.
        #'
        #' @param meta (`list()`)\cr
        #'   Array metadata list.
        #' @return `raw()`.
        encode_array_metadata = function(meta) {
            clean_meta <- meta
            clean_meta[['zarr_format']] <- jsonlite::unbox(private$ZARR_FORMAT)
            return(self$encode_metadata(clean_meta))
        },
        #' @description
        #' Encode group metadata to raw JSON bytes, setting zarr_format to 2.
        #'
        #' @param meta (`list()` | `NA`)\cr
        #'   Ignored; a fresh group metadata list is created.
        #' @return `raw()`.
        encode_group_metadata = function(meta = NA) {
            meta <- obj_list()
            meta[['zarr_format']] <- jsonlite::unbox(private$ZARR_FORMAT)
            return(self$encode_metadata(meta))
        }
    )
)

validate_v2_meta <- function(meta) {
  if(meta$zarr_format != 2) stop("unsupported zarr format ", meta$zarr_format)
}

#' @keywords internal
try_from_zmeta <- function(key, store) {
  store$get_consolidated_metadata()$metadata[[key]]
}

#' Encode a fill_value for V3 zarr.json.
#' V3 requires special float values as JSON strings: NaN, Infinity, -Infinity.
#' Normal numeric values are returned as unboxed scalars.
#' @param fill_value The fill value to encode.
#' @return A value suitable for inclusion in a jsonlite::toJSON call.
#' @keywords internal
decode_fill_value_v3 <- function(fill_value) {
  if (is.character(fill_value) && length(fill_value) == 1) {
    if (fill_value == "NaN") return(NaN)
    if (fill_value == "Infinity") return(Inf)
    if (fill_value == "-Infinity") return(-Inf)
  }
  fill_value
}

encode_fill_value_v3 <- function(fill_value) {
  if (is.numeric(fill_value) && length(fill_value) == 1) {
    # NOTE: in R, is.na(NaN) == TRUE, so check is.nan() first
    if (is.nan(fill_value)) return(jsonlite::unbox("NaN"))
    if (is.infinite(fill_value) && fill_value > 0) return(jsonlite::unbox("Infinity"))
    if (is.infinite(fill_value) && fill_value < 0) return(jsonlite::unbox("-Infinity"))
    if (is.na(fill_value)) return(jsonlite::unbox(0))
    return(jsonlite::unbox(fill_value))
  }
  if (is.logical(fill_value) && length(fill_value) == 1) {
    return(jsonlite::unbox(fill_value))
  }
  jsonlite::unbox(fill_value)
}

try_fromJSON <- function(json, warn_message = "Error parsing json was",
                         simplifyVector = FALSE) {
  tryCatch(
    jsonlite::fromJSON(json, simplifyVector),
    error = \(e) {
      # Bare (unquoted) NaN is invalid JSON but appears in some V2 metadata.
      # Only replace unquoted NaN — quoted "NaN" is valid JSON (V3 fill_value).
      if (grepl("(?<!\")NaN(?!\")", json, perl = TRUE)) {
        tryCatch(
          jsonlite::fromJSON(gsub("(?<!\")NaN(?!\")", "null", json, perl = TRUE),
                             simplifyVector),
          error = \(e2) { warning("\n\n", warn_message, "\n\n", e2); NULL }
        )
      } else {
        warning("\n\n", warn_message, "\n\n", e)
        NULL
      }
    }
  )
}

#' @title Zarr V3 Metadata Codec
#'
#' @description
#' Handles decoding of Zarr V3 metadata. In V3, each node has a single
#' `zarr.json` file containing `zarr_format`, `node_type`, and all
#' metadata (shape, data_type, codecs, chunk_grid, chunk_key_encoding,
#' fill_value, attributes, etc.). This class is read-only; no encode
#' methods are provided.
#'
#' @format [R6::R6Class] object.
#'
#' @family Metadata classes
#' @keywords internal
Metadata3 <- R6::R6Class("Metadata3",
  private = list(
    ZARR_FORMAT = 3
  ),
  public = list(
    #' @description
    #' Parse metadata from raw bytes or return as-is if already a list.
    #'
    #' @param s (`raw()` | `list()` | `NULL`)\cr
    #'   Raw JSON bytes, a pre-parsed list, or `NULL`.
    #' @return `list()` or `NULL`.
    decode_metadata = function(s) {
      if (is.list(s) || is.null(s)) {
        return(s)
      } else {
        return(try_fromJSON(rawToChar(s), simplifyVector = FALSE))
      }
    },
    #' @description
    #' Decode and validate V3 array metadata from `zarr.json`.
    #' V3 arrays have `node_type = "array"` and contain shape, data_type,
    #' chunk_grid, chunk_key_encoding, codecs, fill_value, and attributes.
    #'
    #' @param s (`raw()` | `list()` | `NULL`)\cr
    #'   Raw JSON bytes or pre-parsed list.
    #' @return `list()` or `NULL`.
    decode_array_metadata = function(s) {
      meta <- self$decode_metadata(s)
      if (!is.null(meta)) validate_v3_meta(meta, expected_node_type = "array")
      return(meta)
    },
    #' @description
    #' Decode and validate V3 group metadata from `zarr.json`.
    #' V3 groups have `node_type = "group"` and may contain attributes.
    #'
    #' @param s (`raw()` | `list()` | `NULL`)\cr
    #'   Raw JSON bytes or pre-parsed list.
    #' @return `list()` or `NULL`.
    decode_group_metadata = function(s) {
      meta <- self$decode_metadata(s)
      if (!is.null(meta)) validate_v3_meta(meta, expected_node_type = "group")
      return(meta)
    },
    #' @description
    #' Encode V3 array metadata to raw JSON bytes.
    #' Expects a list built by `create_v3_array_meta()` with all scalars
    #' already wrapped in `jsonlite::unbox()`.
    #'
    #' @param meta (`list()`)\cr
    #'   V3 array metadata list.
    #' @return `raw()`.
    encode_array_metadata = function(meta) {
      return(charToRaw(jsonlite::toJSON(meta, auto_unbox = FALSE)))
    },
    #' @description
    #' Encode V3 group metadata to raw JSON bytes.
    #' Produces `{"zarr_format":3,"node_type":"group","attributes":{}}`.
    #'
    #' @param meta (`list()` | `NA`)\cr
    #'   Ignored; a fresh group metadata list is created.
    #' @param attributes (`list()`)\cr
    #'   Attributes to embed in zarr.json. Default empty.
    #' @return `raw()`.
    encode_group_metadata = function(meta = NA, attributes = NULL) {
      group_meta <- list(
        zarr_format = jsonlite::unbox(3L),
        node_type = jsonlite::unbox("group"),
        attributes = if (is.null(attributes)) obj_list() else attributes
      )
      return(charToRaw(jsonlite::toJSON(group_meta, auto_unbox = FALSE)))
    }
  )
)

# Validate basic V3 metadata requirements.
# @param meta Parsed metadata list from zarr.json.
# @param expected_node_type Optional. "array" or "group".
validate_v3_meta <- function(meta, expected_node_type = NULL) {
  if (is.null(meta$zarr_format) || meta$zarr_format != 3) {
    stop("Expected zarr_format=3 but got ", meta$zarr_format)
  }
  if (!is.null(expected_node_type) && meta$node_type != expected_node_type) {
    stop("Expected node_type='", expected_node_type,
         "' but got '", meta$node_type, "'")
  }
}

#' Create a list of zarray metadata.
#' @inheritParams zarr_create
#' @return A list.
#' @keywords internal
create_zarray_meta <- function(shape = NA, chunks = NA, dtype = NA, compressor = NA, fill_value = NA, order = NA, filters = NA, dimension_separator = NA) {
  # Reference: https://zarr.readthedocs.io/en/stable/spec/v2.html#metadata
  if(is.na(dimension_separator)) {
    dimension_separator <- "."
  } else if(!(dimension_separator %in% c(".", "/"))) {
    stop("dimension_separator must be '.' or '/'.")
  }
  if(is_na(compressor)) {
    compressor <- jsonlite::unbox(compressor)
  } else if(!is_na(compressor) && !("id" %in% names(compressor))) {
    stop("compressor must contain an 'id' property when not null.")
  }
  if(is_na(filters)) {
    filters <- jsonlite::unbox(filters)
  }
  if(!(order %in% c("C", "F"))) {
    stop("order must be 'C' or 'F'.")
  }
  is_simple_dtype <- (!dtype$is_structured)
  dtype_str <- dtype$dtype
  if(is_simple_dtype) {
    dtype_byteorder <- dtype$byte_order
    dtype_basictype <- dtype$basic_type
    # Validation occurs in Dtype constructor.

    if(dtype_basictype == "f") {
      if(!is.numeric(fill_value) && !(fill_value %in% c("NaN", "Infinity", "-Infinity"))) {
        stop("fill_value must be NaN, Infinity, or -Infinity when dtype is float")
      }
    }
    if(dtype_basictype == "S" && !is.na(fill_value)) {
      if(!is.character(fill_value)) {
        stop("fill_value must be a character string for string dtype")
      }
    }
  } else {
    # Structured dtypes are not yet supported for validation.
  }

  if(is.null(shape)) {
    shape <-jsonlite::unbox(NA)
  }

  if(!is.null(shape) && !all(is.na(shape))) {
    if(!is.numeric(shape) || any(shape < 0)) {
      stop("shape must be a numeric vector with non-negative values")
    }
  }
  if(!is.null(chunks) && !all(is.na(chunks))) {
    if(!is.numeric(chunks) || any(chunks <= 0)) {
      stop("chunks must be a numeric vector with positive values")
    }
  }
  zarray_meta <- list(
    zarr_format = jsonlite::unbox(2),
    shape = shape,
    chunks = chunks,
    dtype = jsonlite::unbox(dtype_str),
    compressor = compressor,
    fill_value = jsonlite::unbox(fill_value),
    order = jsonlite::unbox(order),
    filters = filters,
    dimension_separator = jsonlite::unbox(dimension_separator)
  )
  return(zarray_meta)
}

#' Create a V3 array metadata list.
#' Parallel to create_zarray_meta() for V2. Builds the zarr.json metadata
#' for a V3 array node. All scalar values are wrapped with jsonlite::unbox()
#' so the result can be encoded with toJSON(auto_unbox = FALSE).
#'
#' @param shape Integer vector. Array shape.
#' @param chunks Integer vector. Chunk shape.
#' @param dtype Character. V2-style dtype string (e.g., "<f8", "|b1").
#' @param compressor Codec object or NA.
#' @param fill_value Fill value (numeric, logical, or NA).
#' @param order Character. "C" or "F" (unused in V3 codec pipeline but stored).
#' @param filters List of Codec objects or NA.
#' @param chunk_key_encoding List or NULL. If NULL, defaults to default "/" encoding.
#' @param attributes List or NULL. Embedded attributes. Default empty.
#' @param dimension_names Character vector or NULL. Omitted from zarr.json if NULL.
#' @return A list ready for jsonlite::toJSON encoding.
#' @keywords internal
create_v3_array_meta <- function(shape, chunks, dtype, compressor, fill_value,
                                  order, filters, chunk_key_encoding = NULL,
                                  attributes = NULL, dimension_names = NULL) {
  codecs <- build_v3_codec_pipeline(compressor, filters, dtype)
  dtype_info <- v2_dtype_to_v3_dtype(dtype)

  if (is.null(chunk_key_encoding)) {
    chunk_key_encoding <- list(
      name = jsonlite::unbox("default"),
      configuration = list(separator = jsonlite::unbox("/"))
    )
  }

  meta <- list(
    zarr_format = jsonlite::unbox(3L),
    node_type = jsonlite::unbox("array"),
    shape = shape,
    data_type = jsonlite::unbox(dtype_info$data_type),
    chunk_grid = list(
      name = jsonlite::unbox("regular"),
      configuration = list(chunk_shape = chunks)
    ),
    chunk_key_encoding = chunk_key_encoding,
    codecs = codecs,
    fill_value = encode_fill_value_v3(fill_value),
    attributes = if (is.null(attributes)) obj_list() else attributes
  )

  if (!is.null(dimension_names)) {
    meta$dimension_names <- dimension_names
  }

  meta
}