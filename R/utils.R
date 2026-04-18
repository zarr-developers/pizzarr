utils::globalVariables(c(
  "volcano",
  # zarrs functions — called inside is_zarrs_available() guards,
  # defined in extendr-wrappers.R (r-universe) or absent (CRAN)
  "zarrs_create_array", "zarrs_runtime_info",
  "zarrs_set_nthreads", "zarrs_set_codec_concurrent_target",
  "zarrs_set_http_batch_range_requests"
))

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
#' @examplesIf requireNamespace("crul", quietly = TRUE)
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

    # If multiple matches and query doesn't specify a version,
    # prefer the v2 variant for backward compatibility.
    if(length(zarr_zips) > 1 && !grepl("_v[0-9]", dataset)) {
      v2 <- grepl("_v2", zarr_zips, fixed = TRUE)
      if(any(v2)) {
        zarr_zips <- zarr_zips[v2]
        avail <- avail[v2]
      }
    }
  }
  
  # in case zarr_zips is all, loop over them and unzip
  for(z in seq_along(zarr_zips)) {
    
    need <- !file.exists(file.path(tdir, avail[z]))
    
    if(file.size(zarr_zips[z]) == 0 & need) {

      if(!requireNamespace("crul", quietly = TRUE)) {
        stop("Downloading sample data requires the 'crul' package.")
      }

      new_z <- file.path(tdir, basename(zarr_zips[z]))

      url <- paste0("https://github.com/zarr-developers/pizzarr/raw/refs/heads/main/docs/data/",
                     basename(zarr_zips[z]))
      client <- crul::HttpClient$new(url = url)
      res <- client$get(disk = new_z)
      res$raise_for_status()

      zarr_zips[z] <- new_z
    }
     
    if(need)
      utils::unzip(zarr_zips[z], exdir = file.path(tdir, dirname(avail[z])))
  }
  
  return(file.path(tdir, avail))
  
}

#' Create a demo Zarr group containing R's volcano dataset
#'
#' Writes the \code{\link[datasets]{volcano}} matrix into a temporary
#' DirectoryStore as a Zarr array named \code{"volcano"} and returns the
#' opened group.
#'
#' @return A \code{ZarrGroup} containing a single array called \code{"volcano"}.
#' @export
#' @examples
#' g <- zarr_volcano()
#' v <- g$get_item("volcano")
#' image(v$get_item("...")$data, main = "Maunga Whau Volcano")
zarr_volcano <- function() {
  dir <- file.path(tempdir(TRUE), "volcano.zarr")

  unlink(dir, recursive = TRUE, force = TRUE)

  z <- DirectoryStore$new(dir)

  za <- zarr_create(dim(volcano), path = "volcano", store = z, overwrite = TRUE)

  za$set_item("...", volcano)

  g <- zarr_open_group(z)

  g$get_attrs()$set_item("tile", "volcano")

  g
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
  if(is.environment(val)) {
    return(FALSE)
  }
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
  chunk
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

#' Check if the bit64 package is available
#' @return Logical.
#' @keywords internal
has_bit64 <- function() {
  requireNamespace("bit64", quietly = TRUE)
}

#' Check if a dtype represents a 64-bit integer type
#' @param dtype_obj A Dtype instance.
#' @return Logical.
#' @keywords internal
is_int64_dtype <- function(dtype_obj) {
  dtype_obj$basic_type %in% c("i", "u") && dtype_obj$num_bytes == 8
}

#' Convert raw bytes to integer64 vector
#' @param buf Raw vector of 8-byte little-endian integers.
#' @param n Number of elements to read.
#' @param endian "little" or "big".
#' @return A bit64::integer64 vector.
#' @keywords internal
raw_to_integer64 <- function(buf, n, endian = "little") {
  # integer64 stores int64 bit patterns in double's 8 bytes
  vec <- readBin(con = buf, what = double(), size = 8, n = n, endian = endian)
  class(vec) <- "integer64"
  vec
}

#' Convert raw bytes to double from int64 (fallback when bit64 unavailable)
#' @param buf Raw vector of 8-byte integers.
#' @param n Number of elements to read.
#' @param endian "little" or "big".
#' @param signed Logical; if TRUE, treat as signed int64.
#' @return A double vector (precision loss possible for values > 2^53).
#' @keywords internal
raw_to_double_from_int64 <- function(buf, n, endian = "little", signed = TRUE) {
  # Read as signed 4-byte ints (unsigned flag only valid for sizes 1-2)
  vals <- readBin(con = buf, what = integer(), size = 4, n = n * 2,
                  signed = TRUE, endian = endian)
  # vals are interleaved: lo[1], hi[1], lo[2], hi[2], ...
  idx_lo <- seq(1, n * 2, by = 2)
  idx_hi <- seq(2, n * 2, by = 2)
  # Convert to unsigned double: negative int32 means high bit set
  lo_vals <- ifelse(vals[idx_lo] < 0,
                    as.double(vals[idx_lo]) + 2^32,
                    as.double(vals[idx_lo]))
  hi_vals <- ifelse(vals[idx_hi] < 0,
                    as.double(vals[idx_hi]) + 2^32,
                    as.double(vals[idx_hi]))
  result <- lo_vals + hi_vals * 2^32
  if (signed) {
    neg <- hi_vals >= 2^31
    if (any(neg)) {
      result[neg] <- result[neg] - 2^64
    }
  }
  result
}

#' Convert integer64 vector to raw bytes
#' @param x A bit64::integer64 vector.
#' @param endian "little" or "big".
#' @return A raw vector.
#' @keywords internal
integer64_to_raw <- function(x, endian = "little") {
  # Strip class so writeBin sees plain doubles (which hold the int64 bits)
  writeBin(unclass(x), con = raw(), size = 8, endian = endian)
}

