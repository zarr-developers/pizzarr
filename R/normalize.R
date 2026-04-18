#' Convert a string into a character vector.
#'
#' @param s The string.
#' @return A vector where each element is an individual character.
#' @keywords internal
str_to_vec <- function(s) {
  return(stringr::str_split(s, pattern = "")[[1]])
}

#' @keywords internal
normalize_list_selection <- function(selection, shape, convert_integer_selection_to_slices = TRUE) {
  # Reference: https://github.com/gzuidhof/zarr.js/blob/292804/src/core/indexing.ts#L45
  selection <- replace_ellipsis(selection, shape)

  for(i in seq_along(selection)) {
    dim_sel <- selection[[i]]
    if(is_integer(dim_sel)){
      if(convert_integer_selection_to_slices) {
        selection[[i]] <- zb_slice(dim_sel, dim_sel + 1, 1)
      } else {
        selection[[i]] <- normalize_integer_selection(dim_sel, shape[i])
      }
    } else if(is_integer_vec(dim_sel)) {
      selection[[i]] <- sapply(dim_sel, normalize_integer_selection, dim_len = shape[i])
    } else if(is_slice(dim_sel)) {
      selection[[i]] <- dim_sel
    } else if(is_bool_vec(dim_sel)) {
      selection[[i]] <- selection[[i]]
    } else if(!is.null(dim_sel) && !is.environment(dim_sel) &&
              (is.na(dim_sel) || dim_sel == ":")) {
      selection[[i]] <- zb_slice(NA, NA, 1)
    }
  }
  return(selection)
}

#' @keywords internal
normalize_integer_selection <- function(dim_sel, dim_len) {
  # Reference: https://github.com/gzuidhof/zarr.js/blob/master/src/core/indexing.ts#L110

  # Normalize type to int
  dim_sel <- as_scalar(dim_sel)

  # TODO: do we really need this for R type array indexing ?
  # handle wraparound
  if(dim_sel < 0) {
    dim_sel <- dim_len + dim_sel
  }

  # TODO: do we need to normalize R indexing or Python indexing here ?
  # handle out of bounds
  if(dim_sel >= dim_len || dim_sel < 0) {
  # if(dim_sel > dim_len || dim_sel < 1) { # pre zb_int implementation
    stop('BoundsCheckError(dim_len)')
  }

  return(dim_sel)
}

#' @keywords internal
normalize_resize_args <- function(old_shape, args) {
  if(length(args) == 1) {
    new_shape <- args[1]
  } else {
    new_shape <- args
  }
  new_shape <- as.list(new_shape)
  if(length(new_shape) != length(old_shape)) {
    stop('new shape must have same number of dimensions')
  }
  return(new_shape)
}

#' @keywords internal
normalize_storage_path <- function(path) {
  # Reference: https://github.com/gzuidhof/zarr.js/blob/29280463ff2f275c31c1fa0f002daa947b8f09b2/src/util.ts#L32

  if(!is.na(path)) {
    # convert backslash to forward slash
    path <- gsub("\\\\", "/", path)
    # strip leading and trailing slashes
    path <- gsub("^/+|/+$", "", path)
    # collapse any repeated slashes
    path <- gsub("/+", "/", path)

    # don't allow path segments with just '.' or '..'
    if (nchar(path) > 0) {
      segments <- strsplit(path, "/", fixed = TRUE)[[1]]
      if (any(segments %in% c(".", ".."))) {
        stop("path containing '.' or '..' segment not allowed")
      }
    }
  } else {
    path <- ""
  }
  return(path)
}

#' @keywords internal
normalize_shape <- function(shape) {
  # Reference: https://github.com/gzuidhof/zarr.js/blob/292804/src/util.ts#L69
  if(!is.null(shape)) {
    shape <- ensure_integer_vec(shape)
    return(floor(shape))
  }
  return(shape)
}

#' @keywords internal
normalize_dtype <- function(dtype, object_codec = NA) {
  # Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0e6cdc04c6413e14f57f61d389972ea937c/zarr/util.py#L152

  if(is_na(dtype)) {
    # np.dtype(None) returns 'float64'
    if(!is_na(object_codec)) {
      stop("expected object_codec to be NA due to NA dtype")
    }
    return(Dtype$new("<f8"))
  }

  # Construct Dtype instance.
  # convenience API for object arrays
  if("Dtype" %in% class(dtype)) {
    return(dtype)
  }
  
  if(is.character(dtype)) {
    # Filter list was NA but there could be non-NA object_codec parameter.
    return(Dtype$new(dtype, object_codec = object_codec))
  }

  stop("dtype must be NA, string/character vector, or Dtype instance")
}

#' @keywords internal
guess_chunks <- function(shape, typesize) {
  # Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0e6cdc04c6413e14f57f61d389972ea937c/zarr/util.py#L69

  # Guess an appropriate chunk layout for an array, given its shape and
  # the size of each element in bytes.  Will allocate chunks only as large
  # as MAX_SIZE.  Chunks are generally close to some power-of-2 fraction of
  # each axis, slightly favoring bigger values for the last index.
  # Undocumented and subject to change without warning.

  CHUNK_BASE <- 256*1024  # Multiplier by which chunks are adjusted
  CHUNK_MIN <- 128*1024  # Soft lower limit (128k)
  CHUNK_MAX <- 64*1024*1024  # Hard upper limit

  ndims <- length(shape)
  # require chunks to have non-zero length for all dimensions
  chunks <- rep(1, ndims)
  for(i in seq_along(shape)) {
    if(shape[i] >= 1) {
      chunks[i] <- shape[i]
    }
  }

  # Determine the optimal chunk size in bytes using a PyTables expression.
  # This is kept as a float.
  dset_size <- prod(chunks) * typesize
  target_size <- CHUNK_BASE * (2 ** log10(dset_size / (1024. * 1024)))

  if (target_size > CHUNK_MAX) {
      target_size <- CHUNK_MAX
  } else if (target_size < CHUNK_MIN) {
      target_size <- CHUNK_MIN
  }

  idx <- 0
  while (TRUE) {
      # Repeatedly loop over the axes, dividing them by 2.  Stop when:
      # 1a. We're smaller than the target chunk size, OR
      # 1b. We're within 50% of the target chunk size, AND
      # 2. The chunk is smaller than the maximum chunk size

      chunk_bytes <- prod(chunks)*typesize

      if ((chunk_bytes < target_size || abs(chunk_bytes-target_size)/target_size < 0.5) && chunk_bytes < CHUNK_MAX) {
        break
      }

      if (prod(chunks) == 1) {
        break  # Element size larger than CHUNK_MAX
      }

      chunks[idx %% ndims + 1] <- ceiling(chunks[idx %% ndims + 1] / 2.0)
      idx <- idx + 1
  }

  return(as.integer(chunks))
}

#' @keywords internal
normalize_chunks <- function(chunks, shape, typesize) {
  # Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0e6cdc04c6413e14f57f61d389972ea937c/zarr/util.py#L115

  # N.B., expect shape already normalized

  # handle auto-chunking
  if(is_na(chunks) || isTRUE(chunks)) {
    return(guess_chunks(shape, typesize))
  }

  # handle no chunking
  if (isFALSE(chunks)) {
    return(shape)
  }

  # handle 1D convenience form
  if(is_scalar(chunks)) {
    chunks <- rep(as.integer(chunks), length(shape))
  }

  # handle bad dimensionality
  if(length(chunks) > length(shape)) {
    stop("too many dimensions in chunks")
  }

  # handle underspecified chunks
  if(length(chunks) < length(shape)) {
    # assume chunks across remaining dimensions
    chunks <- c(chunks, shape[(length(chunks)+1):length(shape)])
  }

  # handle None or -1 in chunks
  if (-1 %in% chunks || NA %in% chunks) {
    for(i in seq_along(chunks)) {
      if(chunks[i] == -1 || is.na(chunks[i])) {
        chunks[i] <- shape[i]
      }
    }
  }

  if(!is.numeric(chunks)) {
    stop("chunks must be numeric")
  }

  return(chunks)
}

#' @keywords internal
normalize_store_arg <- function(store, storage_options=NA, mode=NA) {
  if(is.na(mode)) {
    mode <- "r"
  }

  if(is_na(store)) {
    return(MemoryStore$new())
  }

  if(is.character(store)) {
    if(grepl("^https?://", store)) {
      return(HttpStore$new(store))
    }
    if(grepl("://", store, fixed=TRUE) || grepl("::", store, fixed=TRUE)) {
      # TODO: return FSStore
    }
    if(endsWith(store, ".zip")) {
      # TODO: return ZipStore
      # return(ZipStore$new(store, mode=mode))
    }
    return(DirectoryStore$new(store))
  } else {
    if(!("Store" %in% class(store))) {
      stop("store must be NA, a string, or a Store instance")
    }
  }
  return(store)
}

#' @keywords internal
normalize_order <- function(order) {
  order <- toupper(order)
  if (!(order %in% c('C', 'F'))) {
    stop("order must be either 'C' or 'F'")
  }
  return(order)
}

#' @keywords internal
normalize_object_codec <- function(dtype, object_codec) {
  if (dtype$is_object) {
    if (is_na(object_codec)) {
      stop("missing object_codec for object array")
    }
  } else if (!is_na(object_codec)) {
    warning("an object_codec is only needed for object arrays")
  }
  return(object_codec)
}

#' @keywords internal
normalize_fill_value <- function(fill_value, dtype) {
  if (is_na(fill_value)) { # or dtype.hasobject:
        # no fill value
        # pass
  } else {
    rtype <- dtype$get_rtype()
    if (fill_value == 0) {
      if(is.logical(rtype)) {
        fill_value <- FALSE
      } else if(inherits(rtype, "integer64")) {
        fill_value <- bit64::as.integer64(0)
      } else if(is.integer(rtype)) {
        fill_value <- 0L
      } else if(is.double(rtype)) {
        fill_value <- 0.0
      }
    } else if(is.character(rtype)) {
      if(!is.character(fill_value)) {
        stop("fill_value {!r} is not valid for dtype {}; must be a 'unicode string'")
      }
    } else {
      if(is.logical(rtype)) {
        fill_value <- TRUE
      } else if(inherits(rtype, "integer64")) {
        fill_value <- bit64::as.integer64(fill_value)
      } else if(is.integer(rtype)) {
        fill_value <- as.integer(fill_value)
      } else if(is.double(rtype)) {
        fill_value <- as.double(fill_value)
      } else {
        stop("fill_value {!r} is not valid for dtype {}")
      }
    }
  }
  return(fill_value)
}
