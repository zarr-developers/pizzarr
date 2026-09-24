# Internal utility functions for converting between Zarr and R data types.

#' @keywords internal
get_dtype_from_array <- function(a) {
  RTYPE_DTYPE_MAPPING <- list(
    "logical" = "|b1",
    "integer" = "<i4",
    "double" = "<f8",
    "character" = "|S8" # TODO: how many bytes to use here?
  )
  rtype_str <- typeof(a)
  return(RTYPE_DTYPE_MAPPING[[rtype_str]])
}

#' @keywords internal
is_structured_dtype <- function(dtype) {
  if(is.character(dtype) && length(dtype) == 1) {
    return(FALSE)
  }
  return(TRUE)
}

#' @keywords internal
get_dtype_parts <- function(dtype) {
  # TODO: support object dtype (without digits required in regex)
  dtype_regex <- "^(\\||>|<)(b|i|u|f|c|m|M|S|U|V|O)(\\d+)?"
  if(stringr::str_detect(dtype, dtype_regex)) {
    dtype_matches <- stringr::str_match(dtype, dtype_regex)
    basic_type <- dtype_matches[1,3]
    if(basic_type == "U") {
      byte_multiplier <- 4
    } else {
      byte_multiplier <- 1
    }
    if(!is.null(dtype_matches[1,4]) && !is.na(dtype_matches[1,4])) {
      num_items <- as.integer(dtype_matches[1,4])
      num_bytes <- num_items * byte_multiplier
    } else {
      # Support object dtype
      num_items <- NA
      num_bytes <- 4
    }
    result <- list(
      dtype_str = dtype,
      byte_order = dtype_matches[1,2],
      basic_type = dtype_matches[1,3],
      num_bytes = num_bytes,
      num_items = num_items
    )
    return(result)
  } else {
    return(NA)
  }
}

#' @keywords internal
check_dtype_support <- function(dtype_parts) {
  if(!is_na(dtype_parts) && dtype_parts$basic_type %in% c("b", "i", "u", "f", "S", "U", "O")) {
    return(TRUE)
  }
  stop(paste("Unsupported dtype:", dtype_parts))
  return(FALSE)
}

#' @keywords internal
get_dtype_rtype <- function(basic_type, num_bytes = NULL) {

  # 64-bit integers: use bit64::integer64 if available, else double
  if (basic_type %in% c("i", "u") && !is.null(num_bytes) && num_bytes == 8) {
    if (has_bit64()) {
      return(bit64::integer64())
    } else {
      warning("bit64 package not installed; reading 64-bit integers as double ",
              "(precision loss possible for values > 2^53)")
      return(double())
    }
  }

  # Reference: https://github.com/gzuidhof/zarr.js/blob/292804/src/nestedArray/types.ts#L32
  BASICTYPE_RTYPE_MAPPING <- list(
    "b" = logical(),
    "u" = integer(),
    "i" = integer(),
    "f" = double(),
    "S" = character(),
    "U" = character(),
    "O" = character() # TODO: will object always be character?
  )

  return(BASICTYPE_RTYPE_MAPPING[[basic_type]])
}

#' @keywords internal
get_dtype_endianness <- function(dtype) {
  dtype_parts <- get_dtype_parts(dtype)

  DTYPE_ENDIANNESS_MAPPING <- list(
    "|" = "nr",
    "<" = "little",
    ">" = "big"
  )
  return(DTYPE_ENDIANNESS_MAPPING[[dtype_parts$byte_order]])
}

#' @keywords internal
get_dtype_numbytes <- function(dtype) {
  dtype_parts <- get_dtype_parts(dtype)
  return(dtype_parts$num_bytes)
}

#' @keywords internal
get_dtype_signed <- function(dtype) {
  dtype_parts <- get_dtype_parts(dtype)

  DTYPE_SIGNED_MAPPING <- list(
    "b" = FALSE,
    "u" = FALSE,
    "i" = TRUE,
    "f" = TRUE,
    "S" = FALSE, # TODO: is this correct?
    "U" = FALSE,  # TODO: is this correct?
    "O" = FALSE
  )
  return(DTYPE_SIGNED_MAPPING[[dtype_parts$basic_type]])
}

#' @keywords internal
get_dtype_asrtype <- function(dtype) {
  dtype_parts <- get_dtype_parts(dtype)
  check_dtype_support(dtype_parts)

  # 64-bit integers: use bit64::as.integer64 if available, else as.double
  if (dtype_parts$basic_type %in% c("i", "u") && dtype_parts$num_bytes == 8) {
    if (has_bit64()) {
      return(bit64::as.integer64)
    } else {
      return(as.double)
    }
  }

  # Reference: https://github.com/gzuidhof/zarr.js/blob/292804/src/nestedArray/types.ts#L32
  DTYPE_RTYPE_MAPPING <- list(
    "b" = as.logical,
    "u" = as.integer,
    "i" = as.integer,
    "f" = as.double,
    "S" = as.character,
    "U" = as.character,
    "O" = as.character
  )
  return(DTYPE_RTYPE_MAPPING[[dtype_parts$basic_type]])
}

# Convert V3 data_type string to V2 numpy-style dtype string.
# Reference: https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#data-types
# V3 uses simple string names like "float64"; we convert to numpy-style strings
# like "<f8" so the existing Dtype class works unchanged.
#
# @param v3_dtype Character. V3 data type name (e.g., "float64", "int16", "bool").
# @param endian Character. Byte order from the V3 "endian"/"bytes" codec.
#   Either "little" (default) or "big". Single-byte types ignore this.
# @return Character. V2-style dtype string (e.g., "<f8", "|b1").
# @keywords internal
v3_dtype_to_v2_dtype <- function(v3_dtype, endian = "little") {
  # Variable-length strings: V2 object dtype, decoded by the vlen-utf8 codec.
  if (identical(v3_dtype, "string")) return("|O")

  # Mapping: V3 name -> list(v2 = base string, fixed_order = byte order or NULL)
  # Single-byte types have fixed byte order "|" (not relevant).
  # Multi-byte types get byte order prefix from the endian codec.
  V3_DTYPE_MAP <- list(
    "bool"    = list(v2 = "b1",  fixed_order = "|"),
    "int8"    = list(v2 = "i1",  fixed_order = "|"),
    "int16"   = list(v2 = "i2",  fixed_order = NULL),
    "int32"   = list(v2 = "i4",  fixed_order = NULL),
    "int64"   = list(v2 = "i8",  fixed_order = NULL),
    "uint8"   = list(v2 = "u1",  fixed_order = "|"),
    "uint16"  = list(v2 = "u2",  fixed_order = NULL),
    "uint32"  = list(v2 = "u4",  fixed_order = NULL),
    "uint64"  = list(v2 = "u8",  fixed_order = NULL),
    "float32" = list(v2 = "f4",  fixed_order = NULL),
    "float64" = list(v2 = "f8",  fixed_order = NULL)
  )

  entry <- V3_DTYPE_MAP[[v3_dtype]]
  if (is.null(entry)) {
    stop("Unsupported V3 data_type: '", v3_dtype,
         "'. Supported types: ", paste(names(V3_DTYPE_MAP), collapse = ", "))
  }

  # Single-byte types use fixed "|" prefix; multi-byte types use endian codec
  prefix <- if (!is.null(entry$fixed_order)) {
    entry$fixed_order
  } else if (endian == "big") {
    ">"
  } else {
    "<"
  }

  paste0(prefix, entry$v2)
}

# Convert V2 numpy-style dtype string to V3 data_type name and endian.
# Inverse of v3_dtype_to_v2_dtype.
#
# @param dtype Character. V2 dtype string (e.g., "<f8", "|b1", ">i4").
# @return Named list with:
#   $data_type - V3 data type string (e.g., "float64", "bool")
#   $endian    - "little", "big", or NA_character_ (for single-byte types)
# @keywords internal
v2_dtype_to_v3_dtype <- function(dtype) {
  dtype_parts <- get_dtype_parts(dtype)

  # Object dtype is only writable with a vlen-utf8 object codec.
  if (!is_na(dtype_parts) && dtype_parts$basic_type == "O") {
    return(list(data_type = "string", endian = NA_character_))
  }

  V2_TO_V3_MAP <- list(
    "b1" = "bool",
    "i1" = "int8",
    "i2" = "int16",
    "i4" = "int32",
    "i8" = "int64",
    "u1" = "uint8",
    "u2" = "uint16",
    "u4" = "uint32",
    "u8" = "uint64",
    "f4" = "float32",
    "f8" = "float64"
  )

  key <- paste0(dtype_parts$basic_type, dtype_parts$num_bytes)
  v3_name <- V2_TO_V3_MAP[[key]]
  if (is.null(v3_name)) {
    stop("Cannot convert V2 dtype '", dtype, "' to V3 data_type: unsupported type '", key, "'")
  }

  # "|" prefix means byte order not applicable (single-byte types: bool, int8, uint8)
  endian_str <- get_dtype_endianness(dtype)
  endian <- if (endian_str == "nr") NA_character_ else endian_str

  list(data_type = v3_name, endian = endian)
}

#' The Zarr Dtype class.
#' @title Dtype Class
#' @docType class
#' @description
#'  A data type object (an instance of Dtype class) describes how 
#'  the bytes in the fixed-size block of memory corresponding to an array 
#'  item should be interpreted.
#' @references https://numpy.org/doc/stable/reference/arrays.dtypes.html
#' @format [R6::R6Class]
#' @rdname Dtype
#' @export
Dtype <- R6::R6Class("Dtype",
  public = list(
    #' @field dtype The original dtype string, like "<f4".
    dtype = NULL,
    #' @field byte_order The byte order of the dtype, either "little", "big", or "nr".
    byte_order = NULL,
    #' @field basic_type The basic type of the dtype, like "f".
    basic_type = NULL,
    #' @field num_bytes The number of bytes of the dtype.
    num_bytes = NULL,
    #' @field num_items The number of items of the dtype.
    num_items = NULL,
    #' @field is_signed Whether the dtype is signed. Logical/boolean.
    is_signed = NULL,
    #' @field is_structured Whether the dtype is structured. Logical/boolean.
    is_structured = NULL,
    #' @field is_object Whether the dtype is an object. Logical/boolean.
    is_object = NULL,
    #' @field object_codec The object codec instance.
    object_codec = NULL,
    #' @description
    #' Create a new Dtype instance.
    #' @param dtype The original dtype string, like "<f4".
    #' @param object_codec The object codec instance.
    #' @return A `Dtype` instance.
    initialize = function(dtype, object_codec = NA) {
      self$dtype <- dtype

      dtype_parts <- get_dtype_parts(dtype)
      check_dtype_support(dtype_parts)
      self$byte_order <-  get_dtype_endianness(dtype)
      self$basic_type <- dtype_parts$basic_type
      self$num_bytes <- dtype_parts$num_bytes
      self$num_items <- dtype_parts$num_items

      self$is_signed <- get_dtype_signed(dtype)
      self$is_structured <- is_structured_dtype(dtype)
      self$is_object <- (self$basic_type == "O")

      if (self$basic_type == "u" && self$num_bytes == 8) {
        warning("uint64 dtype: bit64 only supports signed int64; ",
                "values >= 2^63 will overflow")
      }

      self$object_codec <- object_codec
    },
    #' @description
    #' Get the R coercion function name for this dtype.
    #' @return Character string (e.g., `"as.double"`).
    get_asrtype = function() {
      return(get_dtype_asrtype(self$dtype))
    },
    #' @description
    #' Get the R base type for this dtype.
    #' @return An R prototype value (e.g., `integer()`, `double()`, or `bit64::integer64()`).
    get_rtype = function() {
      return(get_dtype_rtype(self$basic_type, self$num_bytes))
    },
    #' @description
    #' Get a constructor function for typed arrays of this dtype.
    #' @return A function that takes `dim` and returns an array.
    get_typed_array_ctr = function() {
      rtype <- self$get_rtype()
      if (inherits(rtype, "integer64")) {
        return(function(dim) {
          a <- array(data = double(), dim = dim)
          class(a) <- "integer64"
          a
        })
      }
      return(function(dim) array(data = rtype, dim = dim))
    }
  )
)
