# Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0/zarr/core.py#L51

#' @keywords internal
format_codec <- function(codec) {
  if (is.null(codec) || is_na(codec)) return("None")
  if (is.character(codec)) return(codec)
  if (inherits(codec, "R6")) return(class(codec)[1])
  as.character(codec)
}

#' The Zarr Array class.
#' @title ZarrArray Class
#' @docType class
#' @description
#' Instantiate an array from an initialized store.
#' @param selection Selections are lists containing either scalars, strings, or Slice objects. Two character
#' literals are supported: "..." selects all remaining array dimensions and ":" selects all of a specific 
#' array dimension.
#'
#' @format [R6::R6Class]
#' @rdname ZarrArray
#' @export
ZarrArray <- R6::R6Class("ZarrArray",
  private = list(
    # store Array store, already initialized.
    #' @keywords internal
    store = NULL,
    # chunk_store Separate storage for chunks. If not provided, `store` will be used for storage of both chunks and metadata.
    #' @keywords internal
    chunk_store = NULL,
    # path Storage path. String, optional.
    #' @keywords internal
    path = NULL,
    # read_only True if array should be protected against modification.
    #' @keywords internal
    read_only = NULL,
    # synchronizer Array synchronizer. Object, optional.
    #' @keywords internal
    synchronizer = NULL,
    # cache_metadata If True (default), array configuration metadata will be cached. If False, metadata will be reloaded prior to all data access and modification.
    #' @keywords internal
    cache_metadata = NULL,
    # cache_attrs If True (default), user attributes will be cached. If False, attributes will be reloaded prior to all data access and modification.
    #' @keywords internal
    cache_attrs = NULL,
    # write_empty_chunks If True, all chunks will be stored regardless of their contents. If False (default), each chunk is compared to the array's fill value prior to storing. If a chunk is uniformly equal to the fill value, then that chunk is not be stored, and the store entry for that chunk's key is deleted.
    #' @keywords internal
    write_empty_chunks = NULL,
    # key_prefix Storage path prefix prepended to chunk keys when accessing the store. Set to path + "/" or empty string for root.
    #' @keywords internal
    key_prefix = NULL,
    # is_view TRUE if this array is a view on another array, sharing the same underlying store data.
    #' @keywords internal
    is_view = NULL,
    # attrs A MutableMapping containing user-defined attributes. Attribute values must be JSON serializable.
    #' @keywords internal
    attrs = NULL,
    # meta Parsed array metadata from .zarray (V2) or zarr.json (V3).
    #' @keywords internal
    meta = NULL,
    # shape Integer vector describing the length of each dimension of the array.
    #' @keywords internal
    shape = NULL,
    # chunks Integer vector describing the length of each dimension of a chunk.
    #' @keywords internal
    chunks = NULL,
    # dtype Dtype object representing the data type of the array.
    #' @keywords internal
    dtype = NULL,
    # fill_value A value used for uninitialized portions of the array.
    #' @keywords internal
    fill_value = NULL,
    # order A string indicating the order in which bytes are arranged within chunks ("C" for row-major, "F" for column-major).
    #' @keywords internal
    order = NULL,
    # dimension_separator Separator character used in chunk keys, typically "." (flat) or "/" (nested directory structure).
    #' @keywords internal
    dimension_separator = NULL,
    # compressor Primary compression codec used to compress chunk data before storage.
    #' @keywords internal
    compressor = NULL,
    # filters One or more codecs used to transform data prior to compression.
    #' @keywords internal
    filters = NULL,
    # zarr_format: integer, 2L or 3L. Determines which metadata format is in use.
    #' @keywords internal
    zarr_format = NULL,
    # chunk_key_encoding: list with $name and $configuration$separator (V3 only).
    # V3 default: name="default", separator="/", producing keys like "c/0/1/2".
    #' @keywords internal
    chunk_key_encoding = NULL,
    # dimension_names: character vector or NULL. Named dimensions (V3 only).
    #' @keywords internal
    dimension_names = NULL,
    # vindex Shortcut for vectorized (inner) indexing, supporting coordinate-style selection using integer arrays.
    #' @keywords internal
    vindex = NULL,
    # oindex Shortcut for orthogonal (outer) indexing, allowing independent selection per dimension using integer, slice, integer array, or boolean array.
    #' @keywords internal
    oindex = NULL,
    # (Re)load metadata from store without synchronization (file locking).
    # Detects V3 (zarr.json) vs V2 (.zarray) format automatically.
    load_metadata_nosync = function() {

      # Check for V3 zarr.json first, then fall back to V2 .zarray.
      # Reference: zarr-python checks zarr.json first for V3 format detection.
      v3_key <- paste0(private$key_prefix, ZARR_JSON)
      if (private$store$contains_item(v3_key)) {
        private$load_metadata_v3_nosync()
        return()
      }

      # V2 path (existing code, unchanged)
      private$zarr_format <- 2L

      mkey <- paste0(private$key_prefix, ARRAY_META_KEY)

      meta <- try_from_zmeta(mkey, private$store)
      
      if(is.null(meta)) {
        meta_bytes <- private$store$get_item(mkey)
        if(!is.null(meta_bytes))
          meta <- private$store$metadata_class$decode_array_metadata(meta_bytes)
      }
      
      private$meta <- meta
      
      if(is.list(meta$shape)) {
        private$shape <- as.integer(meta$shape)
      } else {
        # meta$shape might be null.
        private$shape <- meta$shape
      }
      if(is.list(meta$chunks)) {
        private$chunks <- as.integer(meta$chunks)
      } else {
        # meta$chunks might be null.
        private$chunks <- meta$chunks
      }
      private$fill_value <- meta$fill_value
      private$order <- meta$order
      if("dimension_separator" %in% names(meta) && !is.na(meta$dimension_separator) && !is.null(meta$dimension_separator)) {
        private$dimension_separator <- meta$dimension_separator
      } else {
        # V2 stores don't carry dimension separators; "." is the spec default.
        private$dimension_separator <- "."
      }
      if(is_na(meta$compressor) || is.null(meta$compressor)) {
        private$compressor <- NA
      } else {
        private$compressor <- get_codec(meta$compressor)
      }
      object_codec <- NA
      if(is_na(meta$filters) || is.null(meta$filters)) {
        private$filters <- NA
        object_codec <- NA
      } else {
        private$filters <- list()
        for(config in meta$filters) {
          private$filters <- append(private$filters, get_codec(config))
        }
        if(length(private$filters) == 1) {
          object_codec <- private$filters[[1]]
        }
      }
      private$dtype <- normalize_dtype(meta$dtype, object_codec = object_codec)
    },
    # Load V3 array metadata from zarr.json.
    # Reference: https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html
    # V3 zarr.json contains: zarr_format, node_type, shape, data_type,
    # chunk_grid, chunk_key_encoding, codecs, fill_value, attributes,
    # dimension_names.
    load_metadata_v3_nosync = function() {
      mkey <- paste0(private$key_prefix, ZARR_JSON)
      meta_bytes <- private$store$get_item(mkey)

      # Decode and validate as V3 array
      meta3 <- Metadata3$new()
      meta <- meta3$decode_array_metadata(meta_bytes)

      private$meta <- meta
      private$zarr_format <- 3L

      # --- shape ---
      if (is.list(meta$shape)) {
        private$shape <- as.integer(meta$shape)
      } else {
        private$shape <- meta$shape
      }

      # --- chunks from chunk_grid ---
      # V3 chunk_grid must be "regular" with a chunk_shape configuration.
      if (is.null(meta$chunk_grid) || meta$chunk_grid$name != "regular") {
        stop("Only 'regular' chunk_grid is supported for V3 arrays")
      }
      chunk_shape <- meta$chunk_grid$configuration$chunk_shape
      if (is.list(chunk_shape)) {
        private$chunks <- as.integer(chunk_shape)
      } else {
        private$chunks <- chunk_shape
      }

      # --- chunk_key_encoding ---
      # V3 default: name="default", separator="/", chunk keys like "c/0/1/2"
      # V3 v2-compat: name="v2", separator=".", chunk keys like "0.1.2"
      private$chunk_key_encoding <- meta$chunk_key_encoding
      if (is.null(private$chunk_key_encoding)) {
        private$chunk_key_encoding <- list(
          name = "default",
          configuration = list(separator = "/")
        )
      }
      # Set dimension_separator for compatibility with code that reads it
      sep <- private$chunk_key_encoding$configuration$separator
      private$dimension_separator <- if (!is.null(sep)) sep else "/"

      # --- codecs -> compressor, filters, order, endian ---
      # V3 codec pipeline replaces V2 compressor + filters.
      # resolve_v3_codecs() maps V3 codecs to V2-compatible structures
      # so decode_chunk() works unchanged.
      codec_result <- resolve_v3_codecs(meta$codecs)
      private$compressor <- codec_result$compressor
      private$filters <- codec_result$filters
      private$order <- codec_result$order

      # --- dtype ---
      # V3 uses string names like "float64"; convert to V2 numpy-style
      # so the existing Dtype class works unchanged.
      v2_dtype_str <- v3_dtype_to_v2_dtype(meta$data_type,
                                            endian = codec_result$endian)

      # Check for object codec (vlen-utf8) in filters
      object_codec <- NA
      if (!is_na(private$filters)) {
        for (f in private$filters) {
          if (inherits(f, "VLenUtf8Codec")) {
            object_codec <- f
            break
          }
        }
      }
      private$dtype <- normalize_dtype(v2_dtype_str, object_codec = object_codec)

      # --- fill_value ---
      # V3 fill_value is required (not optional like V2).
      # V3 encodes NaN/Infinity/-Infinity as JSON strings; decode back to numeric.
      private$fill_value <- decode_fill_value_v3(meta$fill_value)

      # JSON arrays of strings come back as lists under simplifyVector=FALSE;
      # coerce to a character vector so the public getter returns the type
      # users expect. NULL stays NULL when the field is absent from zarr.json.
      if (is.null(meta$dimension_names)) {
        private$dimension_names <- NULL
      } else {
        private$dimension_names <- as.character(unlist(meta$dimension_names))
      }
    },
    # method_description
    # Load or reload metadata from store.
    load_metadata = function() {
      private$load_metadata_nosync()
      # TODO: support for synchronization
    },
    # method_description
    # Referesh metadata if not cached without synchronization (file locking).
    refresh_metadata_nosync = function() {
      if(!private$cache_metadata && !private$is_view) {
        private$load_metadata_nosync()
      }
    },
    # method_description
    # Refresh metadata from store if not cached.
    refresh_metadata = function() {
      if(!private$cache_metadata) {
        private$load_metadata()
      }
    },
    # method_description
    # Write metadata to store without synchronization (file locking).
    flush_metadata_nosync = function() {
      if(private$is_view) {
        stop("Operation not permitted for views")
      }

      if (!is.null(private$zarr_format) && private$zarr_format == 3L) {
        # V3: rebuild and rewrite zarr.json with updated metadata
        v3_meta <- create_v3_array_meta(
          shape = private$shape,
          chunks = private$chunks,
          dtype = private$dtype$dtype,
          compressor = private$compressor,
          fill_value = private$fill_value,
          order = private$order,
          filters = private$filters,
          chunk_key_encoding = private$chunk_key_encoding,
          attributes = if (!is.null(private$attrs)) private$attrs$to_list() else obj_list(),
          dimension_names = private$dimension_names
        )
        mkey <- paste0(private$key_prefix, ZARR_JSON)
        meta3 <- Metadata3$new()
        encoded_meta <- meta3$encode_array_metadata(v3_meta)
        private$store$set_item(mkey, encoded_meta)
        return(invisible(NULL))
      }

      # V2 path
      if(!is_na(private$compressor)) {
        compressor_config <- private$compressor$get_config()
      } else {
        compressor_config <- NA
      }
      if(!is_na(private$filters)) {
        filters_config <- list()
        for(filter in private$filters) {
          filters_config <- append(filters_config, list(filter$get_config()))
        }
      } else {
        filters_config <- NA
      }
      zarray_meta <- list(
        shape = private$shape,
        chunks = private$chunks,
        dtype = jsonlite::unbox(private$dtype$dtype),
        compressor = compressor_config,
        fill_value = jsonlite::unbox(private$fill_value),
        order = jsonlite::unbox(private$order),
        filters = filters_config,
        dimension_separator = jsonlite::unbox(private$dimension_separator)
      )
      mkey <- paste0(private$key_prefix, ARRAY_META_KEY)

      encoded_meta <- private$store$metadata_class$encode_array_metadata(zarray_meta)
      private$store$set_item(mkey, encoded_meta)
    },
    # Build the store key for a chunk at the given coordinates.
    # V2: "key_prefix/0.1.2" (dimension_separator between coords)
    # V3 default: "key_prefix/c/0/1/2" (prefix "c" + separator between coords)
    # V3 v2-compat: "key_prefix/0.1.2" (no prefix, configured separator)
    chunk_key = function(chunk_coords) {
      if (!is.null(private$zarr_format) && private$zarr_format == 3L) {
        # V3 chunk key encoding
        # Reference: https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#chunk-key-encoding
        encoding_name <- private$chunk_key_encoding$name
        sep <- private$chunk_key_encoding$configuration$separator
        if (is.null(sep)) sep <- "/"

        if (encoding_name == "default") {
          # Default V3: prefix "c" + separator + coords
          # e.g., coords (0,1,2) -> "c/0/1/2"
          coord_part <- do.call(paste, c(as.list(chunk_coords), sep = sep))
          return(paste0(private$key_prefix, "c", sep, coord_part))
        } else if (encoding_name == "v2") {
          # V2-compatible: no prefix, just coords with separator
          return(paste0(private$key_prefix, do.call(paste, c(as.list(chunk_coords), sep = sep))))
        } else {
          stop("Unsupported chunk_key_encoding: ", encoding_name)
        }
      }
      # V2 chunk key (existing behavior)
      # Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0/zarr/core.py#L2063
      return(paste0(private$key_prefix, do.call(paste, c(as.list(chunk_coords), sep = private$dimension_separator))))
    },
    # Compute the number of chunks along each dimension of the array.
    compute_cdata_shape = function() {
      # Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0/zarr/core.py#L428
      if(is.null(private$shape)) {
        return(1)
      }
      shape <- private$shape
      chunks <- private$chunks
      cdata_shape <- list()
      for(i in seq_len(length(shape))) {
        s <- shape[i]
        c <- chunks[i]
        cdata_shape <- append(cdata_shape, ceiling(s / c))
      }
      cdata_shape <- as.numeric(cdata_shape)
      return(cdata_shape)
    },
    # method_description
    # Resize an array without synchronization (file locking)
    resize_nosync = function(...) {
      # Note: When resizing an array, the data are not rearranged in any way.
      # If one or more dimensions are shrunk, any chunks falling outside the
      # new array shape will be deleted from the underlying store.
      # Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0/zarr/core.py#L2340
      args <- list(...)
      old_shape <- private$shape
      new_shape <- as.numeric(normalize_resize_args(old_shape, args))
      old_cdata_shape <- private$compute_cdata_shape()

      # Update metadata
      private$shape <- new_shape
      private$flush_metadata_nosync()

      # Determine the new number and arrangement of chunks
      chunks <- private$chunks
      new_cdata_shape <- list()
      for(i in seq_len(length(new_shape))) {
        s <- new_shape[i]
        c <- chunks[i]
        new_cdata_shape <- append(new_cdata_shape, ceiling(s / c))
      }
      new_cdata_shape <- as.numeric(new_cdata_shape)

      # Remove any chunks not within range
      chunk_store <- self$get_chunk_store()
      cidx_df <- do.call(expand.grid, lapply(old_cdata_shape, seq_len))
      for(row_idx in seq_len(dim(cidx_df)[1])) {
        cidx <- as.numeric(cidx_df[row_idx, ])
        if(all(as.logical(lapply(zip_numeric(cidx, new_cdata_shape), function(v) v[1] < v[2])))) {
          # pass; keep the chunk
        } else {
          key <- private$chunk_key(cidx)
          tryCatch(
            chunk_store$delete_item(key),
            error = function(e) {
              warning(paste("Failed to delete chunk", key, ":", e$message))
            }
          )
        }
      }
    },
    # Retrieve data for a zero-dimensional array.
    get_basic_selection_zd = function(selection = NA, out = NA, fields = NA) {
      # Special case basic selection for zero-dimensional array
      # Check selection is valid
      if(!is.null(selection) && selection != "...") {
        stop("err_too_many_indices(selection, ())")
      }
      selection <- ensure_list(selection)
      # Obtain encoded data for chunk
      c_key <- private$chunk_key(c(0))

      chunk_nested_array <- tryCatch({
        c_data <- self$get_chunk_store()$get_item(c_key)
        chunk_inner <- private$decode_chunk(c_data)
        # From raw.
        NestedArray$new(chunk_inner, shape = private$chunks, dtype = private$dtype, order = private$order)
      }, error = function(cond) {
        if(is_key_error(cond)) {
          # chunk not initialized
          as_dtype_func <- private$dtype$get_asrtype()
          chunk_inner <- as_dtype_func(private$fill_value)
          # From array().
          return(NestedArray$new(chunk_inner, shape = private$chunks, dtype = private$dtype, order = private$order))
        } else {
          print(cond$message)
          stop("rethrow")
        }
      })      

      # TODO: Handle fields
      # if(!is.na(fields)) {
      #   chunk <- chunk[fields]
      # }

      # Handle selection of the scalar value via empty tuple
      if(is_na(out)) {
        out <- chunk_nested_array
      } else {
        # TODO
        #out[selection] <- as_scalar(chunk)
      }
      return(out)
    },
    # Retrieve data for an N-dimensional array using basic (integer/slice) selection.
    get_basic_selection_nd = function(selection = NA, out = NA, fields = NA) {
      # Route negative-step slices to orthogonal selection
      sel_list <- if(is.list(selection)) selection else list(selection)
      has_neg_step <- any(vapply(sel_list, function(s) {
        is_slice(s) && !is.na(s$step) && s$step < 0
      }, logical(1)))
      if(has_neg_step) {
        indexer <- OrthogonalIndexer$new(selection, self)
      } else {
        indexer <- BasicIndexer$new(selection, self)
      }
      return(private$get_selection(indexer, out = out, fields = fields))
    },
    # Retrieve data by iterating over chunks that overlap the indexer selection, extracting data into the output array.
    get_selection = function(indexer, out = NA, fields = NA) {
      # Reference: https://github.com/gzuidhof/zarr.js/blob/292804/src/core/index.ts#L304
      # We iterate over all chunks which overlap the selection and thus contain data
      # that needs to be extracted. Each chunk is processed in turn, extracting the
      # necessary data and storing into the correct location in the output array.
      out_dtype <- private$dtype
      out_shape <- indexer$shape
      out_size <- compute_size(indexer$shape)

      if(!is.na(out)) {
        # TODO: handle out provided as parameter
      } else {
        out <- NestedArray$new(NULL, shape = out_shape, dtype = out_dtype, order = private$order)
      }

      if(out_size == 0) {
        return(out)
      }

      # --- zarrs fast path ---
      if (can_use_zarrs(indexer, private$store)) {
        ranges <- selection_to_ranges(indexer)
        store_id <- private$store$get_store_identifier()
        ct <- getOption("pizzarr.concurrent_target", NULL)
        result <- tryCatch(
          zarrs_get_subset(store_id, private$path, ranges, ct),
          error = function(e) NULL
        )
        if (!is.null(result)) {
          # zarrs returns flat F-order data (transposed in Rust).
          # Reshape directly into R array with correct dims.
          zarrs_shape <- vapply(ranges, function(r) r[2L] - r[1L], integer(1))
          arr <- array(result$data, dim = zarrs_shape)
          # Squeeze scalar dims (IntDimIndexer) to match out_shape.
          # Only apply when out_shape has fewer dims (length-1 dims dropped).
          os <- unlist(out_shape)
          if (length(os) > 0 && !identical(zarrs_shape, os)) {
            dim(arr) <- os
          }
          out$data <- arr
          return(out)
        }
        # zarrs failed — fall through to R-native path
      }

      # --- R-native path ---
      parts <- indexer$iter()
      for(proj in parts) {
        private$chunk_getitem(proj$chunk_coords, proj$chunk_sel, out,
                              proj$out_sel, drop_axes = indexer$drop_axes)
      }

      # Return scalar instead of zero-dimensional array.
      if(length(out$shape) == 0) {
        return(out$data[0])
      }
      return(out)

    },
    # Set data for a zero-dimensional array.
    set_basic_selection_zd = function(selection, value, fields = NA) {
      # Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0e6cdc04c6413e14f57f61d389972ea937c/zarr/core.py#L1625

      # check selection is valid
      selection <- ensure_list(selection)
      if(!(length(selection) == 0 || selection == "...")) {
        stop("err_too_many_indices(selection, self._shape)")
      }

      # TODO: check fields
      #check_fields(fields, self._dtype)
      #fields = check_no_multi_fields(fields)

      # obtain key for chunk
      c_key <- private$chunk_key(c(0))

      # setup chunk
      # chunk <- tryCatch({
      #   # obtain compressed data for chunk
      #   c_data <- self$get_chunk_store()$get_item(c_key)
      #   # decode chunk
      #   chunk_inner <- private$decode_chunk(c_data)
      #   return(chunk_inner)
      # }, error = function(cond) {
      #   if(is_key_error(cond)) {
      #     # chunk not initialized
      #     as_dtype_func = get_dtype_asrtype(private$dtype)
      #     chunk_inner <- as_dtype_func(private$fill_value)
      #     return(chunk_inner)
      #   } else {
      #     print(cond$message)
      #     stop("rethrow")
      #   }
      # })
      
      # TODO
      # set value
      # if fields:
      #     chunk[fields][selection] = value
      # else:
      #     chunk[selection] = value

      # TODO
      # remove chunk if write_empty_chunks is false and it only contains the fill value
      # if (not self.write_empty_chunks) and all_equal(self.fill_value, chunk):
      #     try:
      #         del self.chunk_store[ckey]
      #         return
      #     except Exception:  # pragma: no cover
      #         # deleting failed, fallback to overwriting
      #         pass
      # else:

      # encode and store

      chunk_nested_array <- NestedArray$new(as_scalar(value), shape = NULL, dtype = private$dtype, order = private$order)
      chunk_raw <- chunk_nested_array$flatten_to_raw(order = private$order)

      c_data <- private$encode_chunk(chunk_raw)
      self$get_chunk_store()$set_item(c_key, c_data)
    },
    # Set data for an N-dimensional array using basic (integer/slice) selection.
    set_basic_selection_nd = function(selection, value, fields = NA) {
      indexer <- BasicIndexer$new(selection, self)
      return(private$set_selection(indexer, value = value, fields = fields))
    },
    # Set data by iterating over chunks that overlap the indexer selection, replacing data from the value array.
    set_selection = function(indexer, value, fields = NA) {
      # Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0/zarr/core.py#L1682
      # Reference: https://github.com/gzuidhof/zarr.js/blob/15e3a3f00eb19f0133018fb65f002311ea53bb7c/src/core/index.ts#L566

      # // We iterate over all chunks which overlap the selection and thus contain data
      # // that needs to be replaced. Each chunk is processed in turn, extracting the
      # // necessary data from the value array and storing into the chunk array.

      # // N.B., it is an important optimisation that we only visit chunks which overlap
      # // the selection. This minimises the number of iterations in the main for loop.

      selection_shape <- indexer$shape
      selection_shape_vec <- ensure_integer_vec(indexer$shape)

      if(sum(as.numeric(selection_shape)) > 0) {
        # Check value shape
        if (length(selection_shape) == 0) {
          # Setting a single value
        } else if (is_scalar(value)) {
          # Setting a scalar value
        } else if("array" %in% class(value) || (is.atomic(value) && is.vector(value))) {
          if (!is.null(dim(value)) && !all(ensure_integer_vec(dim(value)) == selection_shape_vec)) {
            stop("Shape mismatch in source array and set selection: ${dim(value)} and ${selectionShape}")
          }
          value <- NestedArray$new(value, shape = selection_shape_vec, dtype=private$dtype, order = private$order)
        } else if (inherits(value, "integer64")) {
          value <- NestedArray$new(value, shape = selection_shape_vec, dtype=private$dtype, order = private$order)
        } else if ("NestedArray" %in% class(value)) {
          if (!all(ensure_integer_vec(value$shape) == selection_shape_vec)) {
            stop("Shape mismatch in source NestedArray and set selection: ${value.shape} and ${selectionShape}")
          }
        } else {
          stop("UnsupportedOperation(object dtype requires object_codec in filters for set_item)")
        }

        # --- zarrs write fast path ---
        if (can_use_zarrs_write(indexer, private$store)) {
          ranges <- selection_to_ranges(indexer)
          store_id <- private$store$get_store_identifier()
          ct <- getOption("pizzarr.concurrent_target", NULL)
          # Extract flat data from value
          if (inherits(value, "NestedArray")) {
            write_data <- as.vector(value$data)
          } else if (is_scalar(value)) {
            write_data <- value
          } else {
            write_data <- as.vector(value)
          }
          # Rust handles F→C transpose internally; just flatten.
          # Shape is passed via ranges so Rust knows the dimensions.
          result <- tryCatch(
            zarrs_set_subset(store_id, private$path, ranges, write_data, ct),
            error = function(e) NULL
          )
          if (isTRUE(result)) return(invisible(NULL))
          # zarrs failed — fall through to R-native path
        }

        parts <- indexer$iter()
        for(proj in parts) {
          chunk_value <- private$get_chunk_value(proj, indexer, value, selection_shape)
          private$chunk_setitem(proj$chunk_coords, proj$chunk_sel, chunk_value)
        }

        return()
      }
    },
    # Decode a chunk and extract the selected region into the output array.
    process_chunk = function(out, cdata, chunk_selection, drop_axes, out_is_ndarray, fields, out_selection, partial_read_decode = FALSE) {
      # Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0/zarr/core.py#L1755
      # TODO
    },
    # Extract the portion of a value array corresponding to a single chunk projection.
    get_chunk_value = function(proj, indexer, value, selection_shape) {
      # Reference: https://github.com/gzuidhof/zarr.js/blob/15e3a3f00eb19f0133018fb65f002311ea53bb7c/src/core/index.ts#L550
      
      # value is the full NestedArray representing the value to be set.
      # we call value.get() to get the value for the current chunk selection,
      # since the full value might span multiple chunks.
      if (length(selection_shape) == 0) {
        chunk_value <- value
      } else if (is_scalar(value)) {
        chunk_value <- value
      } else {
        chunk_value <- value$get(proj$out_sel)
        if (isTRUE(indexer$drop_axes)) {
          stop("Handling drop axes not supported yet")
        }
      }
      return(chunk_value)
    },
    # method_description
    # TODO
    chunk_buffer_to_raw_array = function(decoded_chunk) {
      # TODO
    },
    # method_description
    chunk_getitem = function(chunk_coords, chunk_selection, out, out_selection, drop_axes = NA, fields = NA) {
      # TODO
      # Reference: https://github.com/gzuidhof/zarr.js/blob/15e3a3f00eb19f0133018fb65f002311ea53bb7c/src/core/index.ts#L380

      if(length(chunk_coords) != length(private$chunks)) {
        stop("Inconsistent shapes: chunkCoordsLength: ${chunkCoords.length}, cDataShapeLength: ${this.chunkDataShape.length}")
      }
      c_key <- private$chunk_key(chunk_coords)

      tryCatch({
        c_data <- self$get_chunk_store()$get_item(c_key)
        decoded_chunk <- private$decode_chunk(c_data)

        if(is_contiguous_selection(out_selection) && is_total_slice(chunk_selection, private$chunks) && is.null(private$filters)) {
          out$set(out_selection, NestedArray$new(decoded_chunk, shape=private$chunks, dtype=private$dtype, order = private$order))
          return(TRUE)
        }

        # Decode chunk
        chunk <- NestedArray$new(decoded_chunk, shape=private$chunks, dtype=private$dtype, order = private$order)
        tmp <- chunk$get(chunk_selection)

        if(!is_na(drop_axes)) {
          stop("Drop axes is not supported yet")
        }
        out$set(out_selection, tmp)
      }, error = function(cond) {
        if(is_key_error(cond)) {
          # fill with scalar if cKey doesn't exist in store
          # NaN is a valid fill value; is.na(NaN)==TRUE so check is.nan first
          if ((is.numeric(private$fill_value) && is.nan(private$fill_value)) ||
              !is_na(private$fill_value)) {
            out$set(out_selection, as_scalar(private$fill_value))
          }
        } else {
          print(cond$message)
          stop("Different type of error - rethrow")
        }
      })
    },
    # method_description
    # TODO
    chunk_getitems = function(lchunk_coords, lchunk_selection, out, lout_selection, drop_axes = NA, fields = NA) {
      # TODO
    },
    # method_description
    # TODO
    chunk_setitem = function(chunk_coords, chunk_selection, value, fields = NA) {
      # Reference: https://github.com/gzuidhof/zarr.js/blob/15e3a3f00eb19f0133018fb65f002311ea53bb7c/src/core/index.ts#L625
      
      # Obtain key for chunk storage
      chunk_key <- private$chunk_key(chunk_coords)

      dtype_constr = private$dtype$get_typed_array_ctr()
      chunk_size <- compute_size(private$chunks)

      if (is_total_slice(chunk_selection, private$chunks)) {
        # Totally replace chunk

        # Optimization: we are completely replacing the chunk, so no need
        # to access the existing chunk data

        if (is_scalar(value)) {
          chunk <- NestedArray$new(
            value,
            shape = private$chunks,
            dtype = private$dtype,
            order = private$order
          )
        } else {
          # Ensure the value data is coerced to the target dtype's R type
          as_dtype_func <- private$dtype$get_asrtype()
          coerced_data <- as_dtype_func(value$data)
          dim(coerced_data) <- dim(value$data)
          # Use actual data shape, not private$chunks: boundary chunks may be
          # smaller than the nominal chunk size so value$data won't fill a
          # full-sized chunk and would cause a recycling warning.
          chunk_shape <- if (!is.null(dim(coerced_data)) &&
                             !identical(as.integer(dim(coerced_data)),
                                        as.integer(private$chunks))) {
            dim(coerced_data)
          } else {
            private$chunks
          }
          chunk <- NestedArray$new(
            coerced_data,
            shape = chunk_shape,
            dtype = private$dtype,
            order = private$order
          )
        }
        chunk_raw <- chunk$flatten_to_raw(order = private$order)
      } else {
        # partially replace the contents of this chunk

        # Existing chunk data
        chunk_nested_array <- tryCatch({

          # Chunk is initialized if this does not error
          chunk_store_data <- self$get_chunk_store()$get_item(chunk_key)
          dbytes <- private$decode_chunk(chunk_store_data)
          # From raw.
          NestedArray$new(
            dbytes,
            shape = private$chunks,
            dtype = private$dtype,
            order = private$order
          )
        }, error = function(cond) {
          if (is_key_error(cond)) {
            # Chunk is not initialized
            chunk_data <- dtype_constr(chunk_size)
            # NaN is a valid fill value; is.na(NaN)==TRUE so check is.nan first
            if (is.numeric(private$fill_value) && is.nan(private$fill_value)) {
              chunk_data <- chunk_fill(chunk_data, private$fill_value)
            } else if (!is_na(private$fill_value)) {
              chunk_data <- chunk_fill(chunk_data, private$fill_value)
            }
            # From base R array.
            return(NestedArray$new(
              chunk_data,
              shape = private$chunks,
              dtype = private$dtype,
              order = private$order
            ))
          } else {
            print(cond$message)
            # // Different type of error - rethrow
            stop("throw error;")
          }
        })
        
        # Now that we have the existing chunk data,
        # we set the new value by using the chunk_selection
        # to specify which subset to replace.
        chunk_nested_array$set(chunk_selection, value)
        chunk_raw <- chunk_nested_array$flatten_to_raw(order = private$order)
      }
      # We encode the new chunk and set it in the chunk store.
      chunk_data <- private$encode_chunk(chunk_raw)
      self$get_chunk_store()$set_item(chunk_key, chunk_data)
    },
    # method_description
    # TODO
    chunk_setitem_nosync = function(chunk_coords, chunk_selection, value, fields = NA) {
      # TODO
    },
    # method_description
    # TODO
    chunk_setitems = function(lchunk_coords, lchunk_selection, values, fields = NA) {
      # TODO
    },
    # method_description
    # TODO
    process_for_setitem = function(ckey, chunk_selection, value, fields = NA) {
      # TODO
    },
    chunk_delitem = function(ckey) {
      self$get_chunk_store()$delete_item(ckey)
    },
    # method_description
    chunk_delitems = function(ckeys) {
      for(ckey in ckeys) {
        private$chunk_delitem(ckey)
      }
    },
    # method_description
    # TODO
    decode_chunk = function(cdata, start = NA, nitems = NA, expected_shape = NA) {
      # Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0e6cdc04c6413e14f57f61d389972ea937c/zarr/core.py#L2066
      # decompress
      if(!is_na(private$compressor)) {
        # TODO: only decode requested items
        # if (
        #     all(x is not None for x in [start, nitems])
        #     and self._compressor.codec_id == "blosc"
        # ) and hasattr(self._compressor, "decode_partial"):
        #     chunk = self._compressor.decode_partial(cdata, start, nitems)
        # else:
        chunk <- private$compressor$decode(cdata, self)
      } else {
        chunk <- cdata
      }

      # apply filters
      if(!is_na(private$filters)) {
        for (f in rev(private$filters)) {
          chunk <- f$decode(chunk)
        }
      }

      # TODO: view as numpy array with correct dtype
      # chunk <- ensure_ndarray(chunk)
      # special case object dtype, because incorrect handling can lead to
      # segfaults and other bad things happening
      # if self._dtype != object:
      #    chunk = chunk.view(self._dtype)
      # elif chunk.dtype != object:
          # If we end up here, someone must have hacked around with the filters.
          # We cannot deal with object arrays unless there is an object
          # codec in the filter chain, i.e., a filter that converts from object
          # array to something else during encoding, and converts back to object
          # array during decoding.
          # raise RuntimeError('cannot read object array without object codec')

      # ensure correct chunk shape
      return(chunk)
    },
    # method_description
    # TODO
    encode_chunk = function(chunk_as_raw) {
      # Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0e6cdc04c6413e14f57f61d389972ea937c/zarr/core.py#L2105

      chunk <- chunk_as_raw

      # apply filters
      if(!is_na(private$filters)) {
        for(f in private$filters) {
          chunk <- f$encode(chunk)
        }
      }

      # TODO: check object encoding
      #if ensure_ndarray(chunk).dtype == object:
      #    raise RuntimeError('cannot write object array without object codec')

      # compress
      if(!is_na(private$compressor)) {
        cdata <- private$compressor$encode(chunk, self)
      } else {
        cdata <- chunk
      }

      # TODO: ensure in-memory data is immutable and easy to compare
      #if isinstance(self.chunk_store, MutableMapping):
      #    cdata = ensure_bytes(cdata)

      return(cdata)
    },
    # Append data to the array along the specified axis without synchronization.
    # Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0/zarr/core.py#L2141
    append_nosync = function(data, axis = 1) {
      if (!is.array(data)) {
        data <- as.array(data)
      }

      old_shape <- private$shape
      data_shape <- dim(data)
      if (length(data_shape) != length(old_shape)) {
        stop("incompatible number of dimensions for append")
      }
      for (i in seq_along(old_shape)) {
        if (i != axis && data_shape[i] != old_shape[i]) {
          stop("shape mismatch on non-append axis")
        }
      }

      new_shape <- old_shape
      new_shape[axis] <- old_shape[axis] + data_shape[axis]
      do.call(self$resize, as.list(new_shape))

      selection <- list()
      for (i in seq_along(new_shape)) {
        if (i == axis) {
          selection[[i]] <- slice(old_shape[axis] + 1, new_shape[axis])
        } else {
          selection[[i]] <- slice(1, new_shape[i])
        }
      }

      self$set_item(selection, data)
    }
  ),
  public = list(
    #' @description
    #' Create a new ZarrArray instance.
    #' @param store ([Store])\cr
    #'   Array store, already initialized.
    #' @param path (`character(1)`)\cr
    #'   Storage path.
    #' @param read_only (`logical(1)`)\cr
    #'   Whether the array is read-only.
    #' @param chunk_store ([Store] or `NA`)\cr
    #'   Separate storage for chunks. If not provided, `store` will be used
    #'   for both chunks and metadata.
    #' @param synchronizer (`ANY` or `NA`)\cr
    #'   Object used to synchronize write access to the array.
    #' @param cache_metadata (`logical(1)`)\cr
    #'   Whether to cache metadata.
    #' @param cache_attrs (`logical(1)`)\cr
    #'   Whether to cache attributes.
    #' @param write_empty_chunks (`logical(1)`)\cr
    #'   Whether to write empty chunks.
    #' @return A [ZarrArray] instance.
    initialize = function(store, path = NA, read_only = FALSE, 
                          chunk_store = NA, synchronizer = NA, 
                          cache_metadata = TRUE, cache_attrs = TRUE, 
                          write_empty_chunks = TRUE) {
      private$store <- store
      private$chunk_store <- chunk_store
      if(!is.na(path) && nchar(path) > 0) {
        private$path <- normalize_storage_path(path)
        private$key_prefix <- paste0(private$path, "/")
      } else {
        private$path <- NA
        private$key_prefix <- ""
      }
      private$read_only <- read_only
      private$synchronizer <- synchronizer
      private$cache_metadata <- cache_metadata
      private$cache_attrs <- cache_attrs
      private$is_view <- FALSE
      private$write_empty_chunks <- write_empty_chunks

      private$load_metadata()

      akey <- paste0(private$key_prefix, ATTRS_KEY)
      if (!is.null(private$zarr_format) && private$zarr_format == 3L) {
        # V3: attributes are embedded in zarr.json, not in a separate .zattrs.
        # Create Attributes object and pre-populate cache from parsed metadata.
        private$attrs <- Attributes$new(store, key = akey, zarr_format = 3L)
        if (!is.null(private$meta$attributes)) {
          private$attrs$set_cached_v3_attrs(private$meta$attributes)
        }
      } else {
        # V2: attributes in separate .zattrs file (existing behavior)
        private$attrs <- Attributes$new(store, key = akey, zarr_format = 2L)
      }

      private$oindex <- OIndex$new(self)
      private$vindex <- VIndex$new(self)
    },
    #' @description
    #' get store from array.
    #' @return [Store].
    get_store = function() {
      return(private$store)
    },    
    #' @description
    #' get array path
    #' @return `character(1)` or `NA`.
    get_path = function() {
      return(private$path)
    },
    #' @description
    #' get full array name
    #' @return `character(1)` or `NA`.
    get_name = function() {
      if(!is.na(private$path)) {
        name <- private$path
        name_vec <- str_to_vec(name)
        if(name_vec[1] != "/") {
          name <- paste0("/", name)
        }
        return(name)
      }
      return(NA)
    },
    #' @description
    #' get the basename of an array
    #' @return `character(1)` or `NA`.
    get_basename = function() {
      name <- self$get_name()
      if(!is.na(name)) {
        name_segments <- stringr::str_split(name, "/")[[1]]
        return(name_segments[length(name_segments)])
      }
      return(NA)
    },
    #' @description
    #' get the read only property of an array (TRUE/FALSE)
    #' @return `logical(1)`.
    get_read_only = function() {
      return(private$read_only)
    },
    #' @description
    #' set the read only property of an array
    #' @param val (`logical(1)`)\cr
    #'   Value to set.
    #' @return `NULL` (called for side effects).
    set_read_only = function(val) {
      private$read_only <- val
    },
    #' @description
    #' get the chunk store for an array
    #' @return [Store].
    get_chunk_store = function() {
      if(is_na(private$chunk_store)) {
        return(private$store)
      } else {
        return(private$chunk_store)
      }
    },
    #' @description
    #' get the shape of an array
    #' @return `integer()`.
    get_shape = function() {
      private$refresh_metadata()
      return(private$shape)
    },
    #' @description
    #' set or reset the size of an array
    #' @param value (`integer()`)\cr
    #'   Numeric size to set.
    #' @return `NULL` (called for side effects).
    set_shape = function(value) {
      self$resize(value)
    },
    #' @description
    #' Change the shape of the array by growing or shrinking one or more dimensions.
    #' @param ... Arguments for do.call.
    #' @return `NULL` (called for side effects).
    resize = function(...) {
      args <- list(...)
      do.call(private$resize_nosync, args)
    },
    #' @description
    #' get the chunk metadata of an array
    #' @return `integer()`.
    get_chunks = function() {
      return(private$chunks)
    },
    #' @description
    #' get the Dtype of an array
    #' @return [Dtype].
    get_dtype = function() {
      return(private$dtype)
    },
    #' @description
    #' get the compressor of an array
    #' @return [Codec] or `NA`.
    get_compressor = function() {
      return(private$compressor)
    },
    #' @description
    #' get the fill value of an array
    #' @return Scalar fill value.
    get_fill_value = function() {
      return(private$fill_value)
    },
    #' @description
    #' set the fill value of an array
    #' @param val Fill value to use.
    #' @return `NULL` (called for side effects).
    set_fill_value = function(val) {
      private$fill_value <- val
      private$flush_metadata_nosync()
    },
    #' @description
    #' Set dimension names of the array (V3 only).
    #' @param names Character vector of length equal to the number of array
    #'   dimensions, or `NULL` to clear.
    #' @return `NULL` (called for side effects).
    set_dimension_names = function(names) {
      if (is.null(private$zarr_format) || private$zarr_format != 3L) {
        stop("DimensionNamesV2Error(set_dimension_names is only supported for V3 arrays)")
      }
      if (!is.null(names) && length(names) != length(private$shape)) {
        stop("DimensionNamesLengthError(length must match length(shape))")
      }
      private$dimension_names <- names
      private$flush_metadata_nosync()
    },
    #' @description
    #' get the storage order metadata of an array.
    #' @return `character(1)`, `"C"` or `"F"`.
    get_order = function() {
      return(private$order)
    },
    #' @description
    #' get the filters metadata of an array
    #' @return `list()` of [Codec] objects or `NA`.
    get_filters = function() {
      return(private$filters)
    },
    #' @description
    #' Get the synchronizer used to coordinate write access to the array.
    #' @return Synchronizer object or `NA`.
    get_synchronizer = function() {
      return(private$synchronizer)
    },
    #' @description
    #' get attributes of array
    #' @return [Attributes].
    get_attrs = function() {
      return(private$attrs)
    },
    #' @description
    #' get number of dimensions of array
    #' @return `integer(1)`.
    get_ndim = function() {
      return(length(private$shape))
    },
    #' @description
    #' Get dimension names of the array (V3 only).
    #' @return Character vector or `NULL`.
    get_dimension_names = function() {
      return(private$dimension_names)
    },
    #' @description
    #' Get the total number of elements in the array.
    #' @return `numeric(1)`.
    get_size = function() {
      # Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0/zarr/core.py#L383
      # TODO
    },
    #' @description
    #' Get the size in bytes of each item in the array.
    #' @return `integer(1)`.
    get_itemsize = function() {
      # TODO
    },
    #' @description
    #' get number of bytes of an array
    #' @return `numeric(1)`.
    get_nbytes = function() {
      private$refresh_metadata()
      return(self$get_size() * self$get_itemsize())
    },
    #' @description
    #' Get the total number of stored bytes for the array, including metadata and compressed chunk data.
    #' @return `numeric(1)`.
    get_nbytes_stored = function() {
      # Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0/zarr/core.py#L413
      # TODO
    },
    #' @description
    #' Get the number of chunks along each dimension of the array.
    #' @return `numeric()`.
    get_cdata_shape = function() {
      private$refresh_metadata()
      return(private$compute_cdata_shape())
    },
    #' @description
    #' Get the total number of chunks in the array.
    #' @return `integer(1)`.
    get_nchunks = function() {
      # TODO
    },
    #' @description
    #' Get the number of chunks that have been initialized with data.
    #' @return `integer(1)`.
    get_nchunks_initialized = function() {
      # TODO
    },
    #' @description
    #' get is_view metadata of array
    #' @return `logical(1)`.
    get_is_view = function() {
      return(private$is_view)
    },
    #' @description
    #' get orthogonal index of array
    #' @return [OIndex].
    get_oindex = function() {
      return(private$oindex)
    },
    #' @description
    #' get vectorized index of array
    #' @return [VIndex].
    get_vindex = function() {
      return(private$vindex)
    },
    #' @description
    #' get write empty chunks setting of array
    #' @return `logical(1)`.
    get_write_empty_chunks = function() {
      return(private$write_empty_chunks)
    },
    #' @description
    #' check if another object refers to the same array. does not check array data
    #' @param other Other object to check.
    #' @return `logical(1)`.
    equals = function(other) {
      if (!inherits(other, "ZarrArray")) return(FALSE)
      return(all(c(
        identical(private$store, other$get_store()),
        identical(private$read_only, other$get_read_only()),
        identical(private$path, other$get_path()),
        !private$is_view
      )))
    },
    #' @description
    #' Print a human-readable summary of the array.
    #' @param ... Ignored.
    #' @return `self` (invisibly).
    print = function(...) {
      nm <- self$get_name()
      if (is.na(nm)) nm <- "/"
      store_type <- class(private$store)[1]
      zf <- if (!is.null(private$zarr_format)) private$zarr_format else 2L
      cat(paste0("<ZarrArray> ", nm, "\n"))
      cat(paste0("  Shape       : (", paste(private$shape, collapse = ", "), ")\n"))
      cat(paste0("  Chunks      : (", paste(private$chunks, collapse = ", "), ")\n"))
      cat(paste0("  Data type   : ", private$dtype$dtype, "\n"))
      cat(paste0("  Fill value  : ", private$fill_value, "\n"))
      cat(paste0("  Order       : ", private$order, "\n"))
      cat(paste0("  Read-only   : ", private$read_only, "\n"))
      cat(paste0("  Compressor  : ", format_codec(private$compressor), "\n"))
      cat(paste0("  Store type  : ", store_type, "\n"))
      cat(paste0("  Zarr format : ", zf, "\n"))
      invisible(self)
    },
    #' @description
    #' Iterate over the array or a range of it, yielding successive slices along the first dimension. Uses chunk caching for efficient sequential access.
    #' @param start Start index of the iteration range (1-based, inclusive).
    #' @param end End index of the iteration range (1-based, inclusive).
    #' @return `list()` of [NestedArray] slices.
    islice = function(start = NA, end = NA) {
      # TODO
    },
    #' @description
    #' Return the length of the first dimension. Raises an error for zero-dimensional arrays.
    #' @return `integer(1)`.
    length = function() {
      if(private$shape) {
        return(private$shape[1])
      } else {
        # 0-dimensional array, same error message as numpy
        stop("length of unized object")
      }
    },
    #' @description
    #' Subset the array.
    #' @param selection Selection specifying the subset to retrieve.
    #' @return [NestedArray].
    get_item = function(selection) {
      # Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0/zarr/core.py#L580
      # Reference: https://github.com/gzuidhof/zarr.js/blob/master/src/core/index.ts#L266
      
      if(is_pure_fancy_indexing(selection)){
        # TODO: implement vindex further for vectorized indexing
        stop("vectorized indexing is not supported yet")
        # return(self$get_vindex()$get_item(selection))
      } else {
        return(self$get_basic_selection(selection)) 
      }
    },
    #' @description
    #' Get a selection of an array based on a "basic" list of slices.
    #' @param selection Selection specifying the subset to retrieve.
    #' @param out Not yet implemented.
    #' @param fields Not yet implemented.
    #' @return [NestedArray].
    get_basic_selection = function(selection = NA, out = NA, fields = NA) {
      # Refresh metadata
      if(!private$cache_metadata) {
        private$load_metadata()
      }
      # Handle zero-dimensional arrays
      if(is.null(private$shape) || length(private$shape) == 0) {
        return(private$get_basic_selection_zd(selection, out = out, fields = fields))
      }
      return(private$get_basic_selection_nd(selection, out = out, fields = fields))
    },
    #' @description
    #' Retrieve data using orthogonal (outer) indexing.
    #' @param selection Selection specifying the subset to retrieve.
    #' @param out Not yet implemented.
    #' @param fields Not yet implemented.
    #' @return [NestedArray].
    get_orthogonal_selection = function(selection = NA, out = NA, fields = NA) {
      
      # Refresh metadata
      if(!private$cache_metadata) {
        private$load_metadata()
      }
      
      indexer <- OrthogonalIndexer$new(selection, self)
      return(private$get_selection(indexer, out = out, fields = fields))
    },
    #' @description
    #' Retrieve data using coordinate (point) indexing. Not yet implemented.
    #' @param selection Selection specifying the subset to retrieve.
    #' @param out Not yet implemented.
    #' @param fields Not yet implemented.
    #' @return [NestedArray].
    get_coordinate_selection = function(selection = NA, out = NA, fields = NA) {
      # TODO
    },
    #' @description
    #' Retrieve data using a boolean mask. Not yet implemented.
    #' @param selection Selection specifying the subset to retrieve.
    #' @param out Not yet implemented.
    #' @param fields Not yet implemented.
    #' @return [NestedArray].
    get_mask_selection = function(selection = NA, out = NA, fields = NA) {
      # TODO
    },
    #' @description
    #' Set a subset of the array.
    #' @param selection Selection specifying the subset to set.
    #' @param value The value to set, as an R array() or a Zarr NestedArray instance.
    #' @return `NULL` (called for side effects).
    set_item = function(selection, value) {
      self$set_basic_selection(selection, value)
    },
    #' @description
    #' Set a selection of the array using basic (integer/slice) indexing.
    #' @param selection Selection specifying the subset to set.
    #' @param value The value to set.
    #' @param fields Not yet implemented.
    #' @return `NULL` (called for side effects).
    set_basic_selection = function(selection, value, fields = NA) {
      # Handle zero-dimensional arrays
      if(is.null(private$shape) || length(private$shape) == 0) {
        return(private$set_basic_selection_zd(selection, value = value, fields = fields))
      }
      return(private$set_basic_selection_nd(selection, value = value, fields = fields))
    },
    #' @description
    #' Set data using orthogonal (outer) indexing. Not yet implemented.
    #' @param selection Selection specifying the subset to set.
    #' @param value Not yet implemented.
    #' @param fields Not yet implemented.
    #' @return `NULL` (called for side effects).
    set_orthogonal_selection = function(selection, value, fields = NA) {
      # TODO
    },
    #' @description
    #' Set data using coordinate (point) indexing. Not yet implemented.
    #' @param selection Selection specifying the subset to set.
    #' @param value Not yet implemented.
    #' @param fields Not yet implemented.
    #' @return `NULL` (called for side effects).
    set_coordinate_selection = function(selection, value, fields = NA) {
      # TODO
    },
    #' @description
    #' Set data using a boolean mask. Not yet implemented.
    #' @param selection Selection specifying the subset to set.
    #' @param value Not yet implemented.
    #' @param fields Not yet implemented.
    #' @return `NULL` (called for side effects).
    set_mask_selection = function(selection, value, fields = NA) {
      # TODO
    },
    #' @description
    #' Get array diagnostic info. Not yet implemented.
    #' @return `character(1)`.
    get_info = function() {
      # Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0/zarr/core.py#L2141
      # TODO
    },
    #' @description
    #' Compute a checksum digest of the array data. Not yet implemented.
    #' @param hashname Name of hash.
    #' @return `character(1)`.
    get_digest = function(hashname = "sha1") {
      # TODO
    },
    #' @description
    #' Compute a hex-string checksum digest. Not yet implemented.
    #' @param hashname Name of hash.
    #' @return `character(1)`.
    get_hexdigest = function(hashname = "sha1") {
      # TODO
    },
    #' @description
    #' Append data to the array along the specified axis.
    #' @param data Data to append. Will be coerced to array if needed.
    #' @param axis Axis to append along (1-indexed, R convention). Default 1.
    #' @return `NULL` (called for side effects).
    append = function(data, axis = 1) {
      private$append_nosync(data, axis)
    },
    #' @description
    #' Return a view on the array with modified interpretation. Not yet implemented.
    #' @param shape Not yet implemented.
    #' @param chunks Not yet implemented.
    #' @param dtype Not yet implemented.
    #' @param fill_value Not yet implemented.
    #' @param filters Not yet implemented.
    #' @param read_only Not yet implemented.
    #' @param synchronizer Not yet implemented.
    #' @return [ZarrArray].
    view = function(shape = NA, chunks = NA, dtype = NA, fill_value = NA,
                    filters = NA, read_only = NA, synchronizer = NA) {
      # TODO
    },
    #' @description
    #' Return a view with a different dtype. Not yet implemented.
    #' @param dtype Not yet implemented.
    #' @return [ZarrArray].
    astype = function(dtype) {
      # Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0/zarr/core.py#L2586
      # TODO
    },
    #' @description
    #' Get the dimension separator used in chunk keys.
    #' @return `character(1)`.
    get_dimension_separator = function() {
      return(private$dimension_separator)
    },
    #' @description
    #' Convert Zarr object to R array (for S3 method). Note that this loads all data into memory.
    #' @return An R `array`.
    as.array = function() {
      return(self$get_item("...")$data)
    }
  )
)

#' S3 method for custom bracket subsetting
#'
#' @param obj Object.
#' @param ... Dots.
#' @keywords internal
#' @export
`[.ZarrArray` <- function(obj, ...) {
  filters <- substitute(...())
  if(length(filters) != length(obj$get_shape())) {
    stop("This Zarr object has ", length(obj$get_shape()), " dimensions, ", length(filters), " were supplied")
  }
  filters <- manage_filters(filters)
  return(obj$get_orthogonal_selection(filters))
}

#' S3 method for custom bracket assignment
#'
#' @param obj Object.
#' @param ... Dots.
#' @param value Array or ZarrArray.
#' @keywords internal
#' @export
`[<-.ZarrArray` <- function(obj, ..., value) {
  stop("Assignment using bracket notation is not yet supported - use set_item() directly")
}

#' S3 method for as.array
#'
#' @param x Object.
#' @param ... Not used.
#' @keywords internal
#' @export
as.array.ZarrArray = function(x, ...) {
  x$as.array()
}
