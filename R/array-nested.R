#' @keywords internal
zero_based_to_one_based <- function(selection, shape) {

  # drop this since we could do it for arbitrary indices
  # if(!all(vapply(selection, is_slice, logical(length = 1)))) 
  #   stop("selection must be a list of slices")
  
  selection_list <- list()
  
  for(i in seq_len(length(selection))) {
    
    # get selection
    sel <- selection[[i]]
    
    # for slice
    if(inherits(sel, "Slice")){
     
      # We assume the selection uses zero-based indexing,
      # and internally convert to R-based / 1-based indexing
      # before accessing data on the internal self$data.
      sel_start <- sel$start + 1 # Add one, since R indexing is zero-based.
      sel_stop <- sel$stop # Do not subtract one, since R indexing is inclusive.
      sel_step <- sel$step
      if(is.na(sel_step)) sel_step <- 1
      if(sel_start < 1) {
        sel_start <- 1
        warning("IndexError: slice start index out of bounds (too low), clamped to 1")
      }
      if(sel_start > shape[i]) {
        sel_start <- shape[i]
        warning("IndexError: slice start index out of bounds (too high), clamped to ", shape[i])
      }
      if(sel_stop < 1) {
        sel_stop <- 1
        warning("IndexError: slice stop index out of bounds (too low), clamped to 1")
      }
      if(sel_stop > shape[i]) {
        sel_stop <- shape[i]
        warning("IndexError: slice stop index out of bounds (too high), clamped to ", shape[i])
      }
      selection_list <- append(selection_list, list(seq(from = sel_start, 
                                                        to = sel_stop, 
                                                        by = sel_step))) 
    } else if(is.numeric(sel)) {
      sel <- sel + 1
      selection_list <- append(selection_list, list(sel))
    } else {
      stop("Unsupported selection type")
    }
  }
  return(selection_list)
}

# Reference: https://github.com/gzuidhof/zarr.js/blob/292804/src/nestedArray/index.ts#L9

#' The Zarr NestedArray class.
#' @title NestedArray Class
#' @docType class
#' @description
#' Represents a multi-dimensional array that can be
#' accessed and subsetted via list of Slice instances.
#' @format [R6::R6Class]
#' @rdname NestedArray
#' @importFrom R6 R6Class
#' @export
NestedArray <- R6::R6Class("NestedArray",
  private = list(
    is_zero_dim = NULL,
    dtype_basic_type = NULL,
    dtype_byte_order = NULL,
    dtype_num_bytes = NULL,
    dtype_num_items = NULL
  ),
  public = list(
    #' @field shape The shape of the array.
    shape = NULL,
    #' @field dtype The Zarr dtype of the array, as a string like ">f8".
    dtype = NULL,
    #' @field dtype_obj The Zarr dtype of the array, as a Dtype instance.
    dtype_obj = NULL,
    #' @field data The array contents as a base R array.
    data = NULL,
    #' @description
    #' Create a new NestedArray instance.
    #' @param data The data to initialize the array with.
    #' Either NULL, base R array, base R vector (numeric/logical),
    #' scalar, or raw vector.
    #' @param shape The shape of the array.
    #' @param dtype The Zarr dtype of the array, as a string like ">f8".
    #' @param order The order of the array, either "C" or "F". Only used
    #' when `data` is a raw vector. Optional.
    #' @return A `NestedArray` instance.
    initialize = function(data, shape = NA, dtype = NA, order = NA) {
      if(is.null(shape) || (!is.list(shape) && is_na(shape))) {
        if(is.raw(data)) {
          stop("Cannot infer shape from raw data, please provide shape explicitly")
        }
        shape <- dim(data)
      } else {
        shape <- normalize_shape(shape)
      }
      if(is_na(dtype) && (is.numeric(data) || is.logical(data))) {
        self$dtype_obj <- Dtype$new(get_dtype_from_array(data))
      } else if("Dtype" %in% class(dtype)) {
        self$dtype_obj <- dtype
      } else if(is.character(dtype)) {
        self$dtype_obj <- Dtype$new(dtype)
        if(self$dtype_obj$is_object) {
          stop("Object dtype was initialized from string in NestedArray, so object_codec is missing.")
        }
      } else {
        stop("dtype must be NA, string/character vector, or Dtype instance")
      }
      self$shape <- shape

      private$is_zero_dim <- (is.null(shape) || length(shape) == 0)

      if(is.null(data)) {
        # Create empty array.

        dtype_rtype <- self$dtype_obj$get_rtype()

        if (inherits(dtype_rtype, "integer64")) {
          self$data <- array(data = double(), dim = shape)
          class(self$data) <- "integer64"
        } else {
          self$data <- array(data = dtype_rtype, dim = shape)
        }
      } else if(!is.raw(data) && is.null(self$shape)) {
        # Create zero-dimensional array.

        self$data <- data # TODO?
      } else if(!is.raw(data) && (is.array(data) || is.vector(data) || inherits(data, "integer64")) && is.atomic(data)) {
        # Create array from R atomic vector or array().
        num_shape_elements <- compute_size(shape)
        # Check that data array has same shape as expected
        if(!is.null(dim(data)) && all(ensure_integer_vec(dim(data)) == ensure_integer_vec(shape))) {
          self$data <- data
        } else {
          astype_func <- self$dtype_obj$get_asrtype()
          is_i64 <- inherits(data, "integer64") || is_int64_dtype(self$dtype_obj)
          coerced <- astype_func(data)

          # Data array did not have the expected shape, so we need to reshape it.
          if(!is_na(order) && order == "C") {
            ordered_shape <- shape[rev(seq_len(length(shape)))]
            array_from_vec <- array(data = unclass(coerced), dim = ordered_shape)
            array_from_vec <- aperm(array_from_vec, rev(seq_len(length(shape))))
          } else {
            array_from_vec <- array(data = unclass(coerced), dim = shape)
          }
          if (is_i64 && has_bit64()) class(array_from_vec) <- "integer64"
          self$data <- array_from_vec
        }
      } else if(is.raw(data)) {
        # Create array from a raw vector.

        num_shape_elements <- compute_size(shape)

        # Reference: https://github.com/gzuidhof/zarr.js/blob/292804/src/nestedArray/index.ts#L134
        buf <- data
        # Create from ArrayBuffer or Buffer
        
        dtype_size <- self$dtype_obj$num_bytes
        num_data_elements <- length(buf) / dtype_size
        if (num_shape_elements != num_data_elements) {
          stop('Buffer has ${numDataElements} of dtype ${dtype}, shape is too large or small')
        }

        dtype_rtype <- self$dtype_obj$get_rtype()
        dtype_signed <- self$dtype_obj$is_signed
        if(!dtype_signed && !(dtype_size == 1 || dtype_size == 2)) {
          # readBin will warn "signed = FALSE is only valid for integers of sizes 1 and 2"
          dtype_signed <- TRUE
        }

        endian <- self$dtype_obj$byte_order
        # Normalize to only "little" or "big" since this is what writeBin accepts.
        if(endian == "nr") {
          endian <- "little"
        }

        if(self$dtype_obj$basic_type %in% c("S", "U")) {
          vec_from_raw <- raw_to_char_vec(
            buf,
            self$dtype_obj$basic_type,
            self$dtype_obj$num_items,
            endian
          )
        } else if(is_int64_dtype(self$dtype_obj) && has_bit64()) {
          vec_from_raw <- raw_to_integer64(buf, num_shape_elements, endian)
        } else if(is_int64_dtype(self$dtype_obj)) {
          vec_from_raw <- raw_to_double_from_int64(buf, num_shape_elements,
                                                    endian, dtype_signed)
        } else {
          vec_from_raw <- readBin(
            con = buf,
            what = dtype_rtype,
            size = dtype_size,
            n = num_shape_elements,
            signed = dtype_signed,
            endian = endian
          )
        }
        
        is_i64 <- inherits(vec_from_raw, "integer64")

        if(private$is_zero_dim) {
          array_from_vec <- array(data = unclass(vec_from_raw), dim = c(1))
        } else {
          if(!is_na(order) && order == "C") {
            # Either "C" or "F", defining the layout of bytes within each chunk of the array.
            # "C" means row-major order, i.e., the last dimension varies fastest;
            # "F" means column-major order, i.e., the first dimension varies fastest.
            # Reference: https://zarr.readthedocs.io/en/stable/spec/v2.html#metadata
            ordered_shape <- shape[rev(seq_len(length(shape)))]
            array_from_vec <- array(data = unclass(vec_from_raw), dim = ordered_shape)
            array_from_vec <- aperm(array_from_vec, rev(seq_len(length(shape))))
          } else {
            array_from_vec <- array(data = unclass(vec_from_raw), dim = shape)
          }
        }

        if (is_i64) class(array_from_vec) <- "integer64"
        self$data <- array_from_vec
      } else if(is_scalar(data)) {
        # Create array from a scalar value.
        astype_func <- self$dtype_obj$get_asrtype()
        dtype_rtype <- self$dtype_obj$get_rtype()
        if(private$is_zero_dim) {
          self$data <- array(data=dtype_rtype, dim=c(1))
        } else {
          self$data <- array(data=dtype_rtype, dim=shape)
        }
        self$data[] <- astype_func(data)
      } else {
        #buf_len <- compute_size(shape) * get_dtype_numbytes(dtype) 
        #buf <- raw(length = buf_len)
        # TODO?
        stop("Unexpected type for data in NestedArray$initialize()")
      }
    },
    #' @description
    #' Subset the array.
    #' @param selection A list of slices.
    #' @return A new NestedArray (potentially a subset) representing the selection.
    get = function(selection) {
      selection_list <- zero_based_to_one_based(selection, self$shape)
      
      # Using do.call here seems to work the same as `abind::asub(self$data, selection_list)`
      # so we can use do.call to avoid the extra dependency.
      subset_arr <- do.call("[", append(list(self$data), selection_list))
      subset_nested_array <- NestedArray$new(subset_arr, shape = dim(subset_arr), dtype = self$dtype_obj)
      return(subset_nested_array)
    },
    #' @description
    #' Set a subset of the array.
    #' @param selection A list of slices.
    #' @param value A NestedArray or a base R array.
    #' @return `NULL` (called for side effects, modifies `self$data` in place).
    set = function(selection, value) {
      # value should be a NestedArray.
      selection_list <- zero_based_to_one_based(selection, self$shape)

      if("NestedArray" %in% class(value)) {
        value_data <- value$data
      } else if(is_scalar(value) || is.array(value)) {
        value_data <- value
      } else {
        message(value)
        stop("Got unexpected type for value in NestedArray$set()")
      }

      # Only set values if the array is not meant to be empty.
      if(sum(length(value_data)) > 0 || sum(dim(value_data)) > 0) {
        self$data <- do.call("[<-", c(list(self$data), selection_list, list(value = value_data)))
      }
    },
    #' @description
    #' Flatten the array contents.
    #' @param order Either "C", "F", or NA.
    #' @return The data as a flat vector.
    flatten = function(order = NA) {
      # Transpose first (if needed, based on the ordering).

      # "C" means row-major order, i.e., the last dimension varies fastest;
      # "F" means column-major order, i.e., the first dimension varies fastest.
      # Reference: https://zarr.readthedocs.io/en/stable/spec/v2.html#metadata

      is_i64 <- inherits(self$data, "integer64")

      if(!is_na(order) && order == "C" && !private$is_zero_dim) {
        ordered_data <- aperm(self$data, rev(seq_len(length(self$shape))))
      } else {
        ordered_data <- self$data
      }
      result <- as.vector(ordered_data)
      if (is_i64) class(result) <- "integer64"
      return(result)
    },
    #' @description
    #' Flatten the array contents and convert to a raw vector.
    #' @param order Either "C", "F", or NA.
    #' @return The data as a flat `raw()` vector (or plain vector for object dtypes).
    flatten_to_raw = function(order = NA) {
      data_as_vec <- self$flatten(order = order)

      if(self$dtype_obj$is_object) {
        # The object_codec in filters will handle the conversion to raw.
        return(data_as_vec)
      }

      endian <- self$dtype_obj$byte_order
      # Normalize to only "little" or "big" since this is what writeBin accepts.
      if(endian == "nr") {
        endian <- "little"
      }

      # "If writeBin is called with con a raw vector, it is just an indication that a raw vector should be returned."
      # Reference: https://stat.ethz.ch/R-manual/R-devel/library/base/html/readBin.html
      if(self$dtype_obj$basic_type %in% c("S", "U")) {
        buf <- char_vec_to_raw(
          data_as_vec,
          self$dtype_obj$basic_type,
          self$dtype_obj$num_items,
          endian
        )
      } else if(is_int64_dtype(self$dtype_obj) && inherits(data_as_vec, "integer64")) {
        buf <- integer64_to_raw(data_as_vec, endian)
      } else {
        buf <- writeBin(
          data_as_vec,
          con = raw(),
          size = self$dtype_obj$num_bytes,
          endian = endian
        )
      }
      return(buf)
    },
    #' @description
    #' Convert NestedArray to a base R array.
    #' @return `array()`.
    as.array = function() {
      # Consider using drop() to simplify dimensions of 1
      return(self$data)
    }
  )
)

#' S3 method for as.array
#'
#' @param x Object.
#' @param ... Not used.
#' @keywords internal
#' @export
as.array.NestedArray = function(x, ...) {
  x$as.array()
}
