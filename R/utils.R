#' pizzarr demo data
#' @details
#' For directory stores, unzips the store to a temporary directory
#' and returns the resulting path.
#' 
#' @param dataset character defining which demo dataset is desired, 
#' If NULL, all are returned
#' @param outdir character directory path to store sample zarr stores
#' 
#' @return path to ready to use zarr store
#' @export
#' @examples
#' 
#' sample_dir <- tools::R_user_dir("pizzarr")
#' 
#' clean <- !dir.exists(sample_dir)
#' 
#' zarr_samples <- pizzarr_sample(outdir = sample_dir)
#' 
#' #printing without system path for example
#' gsub(sample_dir, "...", zarr_samples, fixed = TRUE)
#' 
#' # clean up if you don't want to keep them for next time
#' if(clean) unlink(sample_dir, recursive = TRUE)
#' 
pizzarr_sample <- function(dataset = NULL, 
                           outdir = file.path(tools::R_user_dir("pizzarr"), 
                                              "pizzarr_sample")) {
  # will unzip here
  tdir <- outdir
  dir.create(tdir, showWarnings = FALSE, recursive = TRUE)
  
  # source data
  sdir <- system.file("extdata", package = "pizzarr")
  zarr_zips <- list.files(sdir, pattern = ".zarr.zip", 
                         full.names = TRUE, recursive = TRUE)
  
  avail <- gsub(paste0(sdir, "/"), "", gsub(".zip", "", zarr_zips))
  
  # if dataset is specified select it
  if(!is.null(dataset)) {
    f <- grepl(dataset, zarr_zips, fixed = TRUE)
    zarr_zips <- zarr_zips[f]
    
    # if dataset not found, stop and print available datasets
    if(length(zarr_zips) == 0) {
      stop("Dataset not found\n\tMust be one of:\n\t  \"",
           paste(avail, collapse = "\"\n\t  \""), "\"")
    }
    
    avail <- avail[f]
  }
  
  # in case zarr_zips is all, loop over them and unzip
  for(z in seq_along(zarr_zips)) {
    
    need <- !file.exists(file.path(tdir, avail[z]))
    
    if(file.size(zarr_zips[z]) == 0 & need) {
      
      new_z <- file.path(tdir, basename(zarr_zips[z]))

      utils::download.file(paste0("https://github.com/zarr-developers/pizzarr/raw/refs/heads/main/docs/data/", 
                                           basename(zarr_zips[z])), mode = "wb",
                                    new_z)
      
      zarr_zips[z] <- new_z
    }
     
    if(need)
      utils::unzip(zarr_zips[z], exdir = file.path(tdir, dirname(avail[z])))
  }
  
  return(file.path(tdir, avail))
  
}

#' Convert a string into a character vector.
#' 
#' @param s The string.
#' @return A vector where each element is an individual character.
#' @keywords internal
str_to_vec <- function(s) {
  return(stringr::str_split(s, pattern = "")[[1]])
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
      # TODO: validate that fill_value is encoded as an ASCII string using the standard Base64 alphabet.
    }
  } else {
    # TODO: validate structured dtypes
  }

  if(is.null(shape)) {
    shape <-jsonlite::unbox(NA)
  }

  
  # TODO: validate shape param
  # TODO: validate chunks param
  # TODO: validate filters param
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

#' Create an empty named list
#'
#' A helper function to construct an empty list which converts to a JSON object rather than a JSON array.
#'
#' @param ... A variable number of list entries.
#' @return A named list.
#'
#' @keywords internal
#' @export
#' @examples
#' default_window <- obj_list(
#'   min = 0,
#'   max = 255
#' )
obj_list <- function(...) {
  retval <- stats::setNames(list(), character(0))
  param_list <- list(...)
  for(key in names(param_list)) {
    retval[[key]] = param_list[[key]]
  }
  retval
}

#' @keywords internal
zip_numeric <- function(a, b) {
  result <- list()
  for(i in seq_len(length(a))) {
    result <- append(result, list(c(a[i], b[i])))
  }
  return(result)
}

#' @keywords internal
check_selection_length <- function(selection, shape) {
  if(length(selection) > length(shape)) {
    stop('TooManyIndicesError')
  }
}

# Returns both the sliceIndices per dimension and the output shape after slicing.
# Reference: https://github.com/gzuidhof/zarr.js/blob/master/src/core/indexing.ts#L22
#' @keywords internal
selection_to_slice_indices <- function(selection, shape) {
  slice_indices_result <- list()
  out_shape <- c()

  for(i in seq_along(selection)) {
    s <- selection[i]
    if(is.numeric(s)) {
      slice_indices_result <- append(slice_indices_result, s)
    } else {
      x <- s$indices(shape[i])
      dim_length <- x[4]
      out_shape <- c(out_shape, dim_length)
      slice_indices_result <- append(slice_indices_result, x)
    }
  }
  return(list(
    slice_indices_result = slice_indices_result,
    out_shape = out_shape
  ))
}

#' @keywords internal
filter_list <- function(l, pred) {
  result <- list()
  for(item in l) {
    if(pred(item)) {
      result <- append(result, item)
    }
  }
  return(result)
}

#' Convert user selections, potentially containing "...", to a list of slices
#' that can be used internally.
#' @param selection The user-provided selection list.
#' @param shape The shape of the array, to be used to fill in ellipsis values.
#' @returns A list of selections with ellipsis values converted to NA.
#' @keywords internal
replace_ellipsis <- function(selection, shape) {
  # Reference: https://github.com/gzuidhof/zarr.js/blob/master/src/core/indexing.ts#L67

  selection <- ensure_list(selection)
  
  ellipsis_index <- 0
  num_ellipsis <- 0
  for(i in seq_along(selection)) {
    if(selection[i] == "...") {
      ellipsis_index <- i
      num_ellipsis <- num_ellipsis + 1
    }
  }

  if(num_ellipsis > 1) {
    stop("RangeError(an index can only have a single ellipsis ('...'))")
  }

  if(num_ellipsis == 1) {
    # Count how many items to left and right of ellipsis
    num_items_left <- ellipsis_index - 1
    num_items_right <- length(selection) - (num_items_left + 1)
    num_items <- length(selection) - 1 # All non-ellipsis items
    if(num_items >= length(shape)) {
      # Ellipsis does nothing, just remove it
      selection <- filter_list(selection, function(item) {
        if(is.na(item) || item != "...") {
          return(TRUE)
        }
        return(FALSE)
      })
    } else {
      # Replace ellipsis with as many slices are needed for the number of dims
      num_new_items <- length(shape) - num_items
      new_item <- selection[seq_len(num_items_left)]
      for(i in seq_len(num_new_items)) {
        new_item <- append(new_item, NA)
      }
      if(num_items_right > 0) {
        new_item_right <- selection[seq(from = length(selection) - num_items_right + 1, to = length(selection))]
        for(i in seq_along(new_item_right)) {
          new_item <- append(new_item, new_item_right[i])
        }
      }
      selection <- new_item
    }
  }
  # Fill out seleciton if not  completely specified
  if(length(selection) < length(shape)) {
    num_missing <- length(shape) - length(selection)
    for(i in seq_len(num_missing)) {
      selection <- append(selection, NA)
    }
  }

  check_selection_length(selection, shape)
  return(selection)
}

#' Compute Size
#' @param shape A shape vector
#' @returns The product of shape elements.
#' @keywords internal
compute_size <- function(shape) {
  result <- 1
  for(val in shape) {
    result <- result * val
  }
  return(result)
}

#' Check if a value, potentially a vector, is NA
#'
#' @param val The value to check
#' @return Whether the value is NA
#' @keywords internal
is_na <- function(val) {
  if(length(val) != 1) {
    # Including when val is integer(0), character(0), etc.
    return(FALSE)
  } else {
    return(is.na(val))
  }
}

#' Fill in an R array with a single scalar value.
#' @keywords internal
#' @param chunk The R array to fill.
#' @param value The scalar value (after is_scalar() check).
chunk_fill <- function(chunk, value) {
  # Chunk is an R array()
  # Value is a scalar (after is_scalar() check)

  # Need to do equivalent of chunk.fill(value) in JS
  chunk[] <- value
}

#' Check if an error is a KeyError.
#' @param e The error to check.
#' @return TRUE if the error is a KeyError, FALSE otherwise.
#' @export
is_key_error <- function(e) {
  return(grepl("KeyError", e$message))
}

#' @keywords internal
get_list_product_aux <- function(dim_indexer_iterables, i, partial_results) {
  dim_results <- dim_indexer_iterables[[i]]
  result <- list()
  for(d in dim_results) {
    if(length(partial_results) == 0) {
      result <- append(result, list(d))
    } else {
      for(p in partial_results) {
        result <- append(result, list(append(p, list(d))))
      }
    }
  }
  return(result)
}

#' Generate a product of lists.
#' @param dim_indexer_iterables A list of lists.
#' @return A list of lists.
#' @keywords internal
get_list_product <- function(dim_indexer_iterables) {
  # Reference: https://docs.python.org/3/library/itertools.html#itertools.product
  partial_results <- list()
  for(i in seq_len(length(dim_indexer_iterables))) {
    partial_results <- get_list_product_aux(dim_indexer_iterables, i, partial_results)
  }
  return(partial_results)
}

#' @keywords internal
item_to_key <- function(item) {
  # Remove leading slash if necessary.
  if(substr(item, 1, 1) == "/") {
    key <- substr(item, 2, length(item))
  } else {
    key <- item
  }
  key
}

#' @keywords internal
is_truthy_parallel_option <- function(val) {
  if(is.na(val)) return(FALSE)
  
  if(inherits(val, "cluster")) return(TRUE)
  
  if(val == "future") return(TRUE)
  
  return(as.logical(as.integer(val)))
}

try_from_zmeta <- function(key, store) {
  store$get_consolidated_metadata()$metadata[[key]]
}

try_fromJSON <- function(json, warn_message = "Error parsing json was", 
                         simplifyVector = FALSE) {
  out <- tryCatch({
    jsonlite::fromJSON(json, simplifyVector)
  }, error = \(e) {
    if(grepl("NaN", e)) {
      tryCatch({
        jsonlite::fromJSON(gsub("NaN", "null", json), simplifyVector)
      }, error = \(e) {
        warning("\n\n", warn_message, "\n\n", e)
        NULL
      })
    } else {
      warning("\n\n", warn_message, "\n\n", e)
      NULL
    }
  })
}
