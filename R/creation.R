#' @keywords internal
rmdir <- function(store, path=NA) {
    # Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0e6cdc04c6413e14f57f61d389972ea937c/zarr/storage.py#L130

    # Remove all items under the given path. If `store` provides a `rmdir` method,
    # this will be called, otherwise will fall back to implementation via the
    # `Store` interface.

    path <- normalize_storage_path(path)
    if("rmdir" %in% names(store) && store$is_erasable()) {
        store$rmdir(path)
    }
}

#' @keywords internal
init_array_metadata <- function(
    store,
    shape,
    chunks=NA,
    dtype=NA,
    compressor=NA,
    fill_value=NA,
    order=NA,
    overwrite=FALSE,
    path=NA,
    chunk_store=NA,
    filters=NA,
    object_codec=NA,
    dimension_separator=NA,
    zarr_format=2L,
    dimension_names=NULL
) {
    # Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0e6cdc04c6413e14f57f61d389972ea937c/zarr/storage.py#L358
    if(is_na(compressor)) {
        compressor <- "default"
    }
    if(is.na(order)) {
        order <- "F"
    }

    # guard conditions
    if (overwrite) {
        # attempt to delete any pre-existing items in store
        store$rmdir(path)
        if(!is.na(chunk_store)) {
            chunk_store$rmdir(path)
        }
    } else if (contains_array(store, path)) {
        stop("ContainsArrayError(path)")
    } else if (contains_group(store, path)) {
        stop("ContainsGroupError(path)")
    }

    # normalize metadata
    dtype <- normalize_dtype(dtype, object_codec = object_codec)

    object_codec <- normalize_object_codec(dtype, object_codec)

    # shape = normalize_shape(shape) + dtype.shape
    # dtype = dtype.base

    shape <- normalize_shape(shape)

    if (!is.null(dimension_names) && length(dimension_names) != length(shape)) {
        stop("DimensionNamesLengthError(length must match length(shape))")
    }

    dtype_itemsize <- dtype$num_bytes
    chunks <- normalize_chunks(chunks, shape, dtype_itemsize)
    order <- normalize_order(order)
    fill_value <- normalize_fill_value(fill_value, dtype)

    # # optional array metadata
    # if dimension_separator is None:
    #     dimension_separator = getattr(store, "_dimension_separator", None)
    # dimension_separator = normalize_dimension_separator(dimension_separator)

    # # compressor prep
    # if shape == ():
    #     # no point in compressing a 0-dimensional array, only a single value
    #     compressor = None
    # elif compressor == 'none':
    #     # compatibility
    #     compressor = None
    if (is.character(compressor) && compressor == "default") {
        compressor <- get_default_compressor()
    }

    # obtain compressor config
    compressor_config = NA
    if(!is_na(compressor)) {
        compressor_config <- tryCatch(
            compressor$get_config(),
            error = function(e) stop("BadCompressorError(compressor)")
        )
    }

    # obtain filters config
    filters_config <- list()
    if(!is_na(filters)) {
        for(f in filters) {
            append(filters_config, list(f$get_config()))
        }
    }

    # Append object codec config to filters (validation already done above)
    if (dtype$is_object && !is_na(object_codec)) {
        filters_config <- append(filters_config, list(object_codec$get_config()))
    }

    # use null to indicate no filters
    if (length(filters_config) == 0) {
        filters_config <- NA
    }

    # --- zarrs create fast path ---
    if (can_use_zarrs_create(store, dtype)) {
        preset <- compressor_to_preset(compressor, compressor_config)
        if (!is.na(preset)) {
            store_id <- store$get_store_identifier()
            v3_dtype <- v2_dtype_to_v3_dtype(dtype$dtype)$data_type
            result <- tryCatch(
                zarrs_create_array(
                    store_id, normalize_storage_path(path),
                    as.integer(shape), as.integer(chunks),
                    v3_dtype, preset, fill_value,
                    "{}", zarr_format
                ),
                error = function(e) NULL
            )
            if (!is.null(result)) return(invisible(NULL))
            # zarrs failed — fall through to R-native path
        }
    }

    # initialize metadata
    if (zarr_format == 3L) {
        v3_meta <- create_v3_array_meta(
            shape = shape,
            chunks = chunks,
            dtype = dtype$dtype,
            compressor = compressor,
            fill_value = fill_value,
            order = order,
            filters = filters,
            dimension_names = dimension_names
        )
        key <- paste0(path_to_prefix(path), ZARR_JSON)
        meta3 <- Metadata3$new()
        encoded_meta <- meta3$encode_array_metadata(v3_meta)
        store$set_item(key, encoded_meta)
    } else {
        if (!is.null(dimension_names)) {
            stop("DimensionNamesV2Error(dimension_names is only supported for zarr_format = 3L)")
        }
        zarray_meta <- create_zarray_meta(
            shape=shape,
            chunks=chunks,
            dtype=dtype,
            compressor=compressor_config,
            fill_value=fill_value,
            order=order,
            filters=filters_config,
            dimension_separator=dimension_separator
        )
        key <- paste0(path_to_prefix(path), ARRAY_META_KEY)
        encoded_meta <- store$metadata_class$encode_array_metadata(zarray_meta)
        store$set_item(key, encoded_meta)
    }
}

#' @keywords internal
init_group_metadata <- function(
    store,
    overwrite = FALSE,
    path = NA,
    chunk_store = NA,
    zarr_format = 2L
) {
    # Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0e6cdc04c6413e14f57f61d389972ea937c/zarr/storage.py#L493
    
    # guard conditions
    if(overwrite) {
        # attempt to delete any pre-existing items in store
        store$rmdir(path)
        if(!is.na(chunk_store)) {
            chunk_store$rmdir(path)
        }
    } else if (contains_array(store, path)) {
        stop("ContainsArrayError(path)")
    } else if (contains_group(store, path)) {
        stop("ContainsGroupError(path)")
    }

    # initialize metadata
    if (zarr_format == 3L) {
        key <- paste0(path_to_prefix(path), ZARR_JSON)
        meta3 <- Metadata3$new()
        encoded_meta <- meta3$encode_group_metadata()
        store$set_item(key, encoded_meta)
    } else {
        # N.B., currently no metadata properties are needed, however there may be in future
        zgroup_meta <- obj_list()
        key <- paste0(path_to_prefix(path), GROUP_META_KEY)
        encoded_meta <- store$metadata_class$encode_group_metadata(zgroup_meta)
        store$set_item(key, encoded_meta)
    }
}

#' @keywords internal
require_parent_group <- function(
    path,
    store,
    chunk_store,
    overwrite,
    zarr_format = 2L
) {
    # Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0e6cdc04c6413e14f57f61d389972ea937c/zarr/storage.py#L206
    # assume path is normalized
    if(!is.na(path)) {
        segments <- stringr::str_split(path, "/")[[1]]
        for(i in seq_len(length(segments) - 1)) {
            p <- paste(segments[0:i], collapse="/")
            if(contains_array(store, p)) {
                # Parent contained an array, so overwrite as group.
                init_group_metadata(store, path=p, chunk_store=chunk_store, overwrite=overwrite, zarr_format=zarr_format)
            } else if (!contains_group(store, p)) {
                init_group_metadata(store, path=p, chunk_store=chunk_store, zarr_format=zarr_format)
            }
        }
    }

}



#' Initialize an array store with the given configuration. Note that this is a low-level
#' function and there should be no need to call this directly from user code.
#' @param store : Store
#'     A mapping that supports string keys and bytes-like values.
#' @param shape : int or tuple of ints
#'     Array shape.
#' @param chunks : bool, int or tuple of ints, optional
#'     Chunk shape. If True, will be guessed from `shape` and `dtype`. If
#'     False, will be set to `shape`, i.e., single chunk for the whole array.
#' @param dtype : string or dtype, optional
#'     NumPy dtype.
#' @param compressor : Codec, optional
#'     Primary compressor.
#' @param fill_value : object
#'     Default value to use for uninitialized portions of the array.
#' @param order : \code{'C', 'F'}, optional
#'     Memory layout to be used within each chunk.
#' @param overwrite : bool, optional
#'     If True, erase all data in `store` prior to initialisation.
#' @param path : string, bytes, optional
#'     Path under which array is stored.
#' @param chunk_store : Store, optional
#'     Separate storage for chunks. If not provided, `store` will be used
#'     for storage of both chunks and metadata.
#' @param filters : sequence, optional
#'     Sequence of filters to use to encode chunk data prior to compression.
#' @param object_codec : Codec, optional
#'     A codec to encode object arrays, only needed if dtype=object.
#' @param dimension_separator : \code{'.', '/'}, optional
#'     Separator placed between the dimensions of a chunk.
#' @keywords internal
init_array <- function(
    store,
    shape,
    chunks = TRUE,
    dtype=NA,
    compressor=NA,
    fill_value=NA,
    order = NA,
    overwrite = FALSE,
    path = NA,
    chunk_store = NA,
    filters=NA,
    object_codec=NA,
    dimension_separator=NA,
    zarr_format=2L,
    dimension_names=NULL
) {
    if(is_na(compressor)) {
        compressor <- "default"
    }
    if(is.na(order)) {
        order <- "F"
    }

    # normalize path
    path <- normalize_storage_path(path)

    # ensure parent group initialized
    require_parent_group(path, store=store, chunk_store=chunk_store, overwrite=overwrite, zarr_format=zarr_format)

    init_array_metadata(
        store,
        shape=shape,
        chunks=chunks,
        dtype=dtype,
        compressor=compressor,
        fill_value=fill_value,
        order=order,
        overwrite=overwrite,
        path=path,
        chunk_store=chunk_store,
        filters=filters,
        object_codec=object_codec,
        dimension_separator=dimension_separator,
        zarr_format=zarr_format,
        dimension_names=dimension_names
    )

}

#' Initialize a group store. Note that this is a low-level function and there should be no
#' need to call this directly from user code.
#' @inheritParams init_array
#' @keywords internal
init_group <- function(
    store,
    overwrite = FALSE,
    path = NA,
    chunk_store = NA,
    zarr_format = 2L
) {
    # normalize path
    path <- normalize_storage_path(path)

    # ensure parent group initialized
    require_parent_group(path, store=store, chunk_store=chunk_store, overwrite=overwrite, zarr_format=zarr_format)

    init_group_metadata(
        store,
        overwrite=overwrite,
        path=path,
        chunk_store=chunk_store,
        zarr_format=zarr_format
    )
}


#' Create an empty array
#' @inheritParams init_array
#' @param chunks : int or tuple of ints, optional
#'     Chunk shape. If True, will be guessed from `shape` and `dtype`. If
#'     False, will be set to `shape`, i.e., single chunk for the whole array.
#'     If an int, the chunk size in each dimension will be given by the value
#'     of `chunks`. Default is True.
#' @param synchronizer : object, optional
#'     Array synchronizer.
#' @param cache_metadata : bool, optional
#'     If True, array configuration metadata will be cached for the
#'     lifetime of the object. If False, array metadata will be reloaded
#'     prior to all data access and modification operations (may incur
#'     overhead depending on storage and data access pattern).
#' @param cache_attrs : bool, optional
#'     If True (default), user attributes will be cached for attribute read
#'     operations. If False, user attributes are reloaded from the store prior
#'     to all attribute read operations.
#' @param read_only : bool, optional
#'     True if array should be protected against modification.
#' @param write_empty_chunks : bool, optional
#'     If True (default), all chunks will be stored regardless of their
#'     contents. If False, each chunk is compared to the array's fill value
#'     prior to storing. If a chunk is uniformly equal to the fill value, then
#'     that chunk is not be stored, and the store entry for that chunk's key
#'     is deleted. This setting enables sparser storage, as only chunks with
#'     non-fill-value data are stored, at the expense of overhead associated
#'     with checking the data of each chunk.
#' @param zarr_format : int, optional
#'     Zarr format version. Use \code{2L} (default) for Zarr V2 or \code{3L}
#'     for Zarr V3.
#' @param dimension_names : character vector, optional
#'     Named dimensions for V3 arrays. Length must equal \code{length(shape)}.
#'     V3 only -- passing a non-NULL value with \code{zarr_format = 2L} is an
#'     error. Default \code{NULL}.
#' @returns ZarrArray
#' @export
zarr_create <- function(
    shape,
    chunks=TRUE,
    dtype=NA,
    compressor=NA,
    fill_value=NA,
    order=NA,
    store=NA,
    synchronizer=NA,
    overwrite=FALSE,
    path=NA,
    chunk_store=NA,
    filters=NA,
    cache_metadata=TRUE,
    cache_attrs=TRUE,
    read_only=FALSE,
    object_codec=NA,
    dimension_separator=NA,
    write_empty_chunks=TRUE,
    zarr_format=2L,
    dimension_names=NULL
) {
    # Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0e6cdc04c6413e14f57f61d389972ea937c/zarr/creation.py#L18
    if(is_na(compressor)) {
        compressor <- "default"
    }
    if(is.na(fill_value) && !is.nan(fill_value)) {
        fill_value <- 0
    }
    if(is.na(order)) {
        order <- "F"
    }

    # handle polymorphic store arg
    store <- normalize_store_arg(store)

    # initialize array metadata
    init_array(store, shape=shape, chunks=chunks, dtype=dtype, compressor=compressor,
               fill_value=fill_value, order=order, overwrite=overwrite, path=path,
               chunk_store=chunk_store, filters=filters, object_codec=object_codec,
               dimension_separator=dimension_separator, zarr_format=zarr_format,
               dimension_names=dimension_names)

    
    # instantiate array
    z <- ZarrArray$new(
        store,
        path=path,
        chunk_store=chunk_store,
        synchronizer=synchronizer,
        cache_metadata=cache_metadata,
        cache_attrs=cache_attrs,
        read_only=read_only,
        write_empty_chunks=write_empty_chunks
    )
    return(z)
}

#' Create an array filled with NAs.
#' @inheritParams zarr_create
#' @param ... The params of zarr_create()
#' @returns ZarrArray
#' @export
zarr_create_empty <- function(shape, ...) {
    return(zarr_create(shape=shape, fill_value=NA, ...))
}

#' Create an array initialized with data.
#' @param data A base R array() or pizzarr NestedArray instance.
#' @param ... The params of zarr_create()
#' @returns ZarrArray
#' @export
zarr_create_array <- function(data, ...) {
    z <- zarr_create(...)
    z$set_item("...", data)
    return(z)
}

#' Create an array filled with zeros.
#' @inheritParams zarr_create
#' @param ... The params of zarr_create()
#' @returns ZarrArray
#' @export
zarr_create_zeros <- function(shape, ...) {
    return(zarr_create(shape=shape, fill_value=0, ...))
}

#' Create a group.
#' @inheritParams init_array
#' @inheritParams zarr_create
#' @returns ZarrGroup
#' @export
zarr_create_group <- function(
    store = NA,
    overwrite = FALSE,
    chunk_store = NA,
    cache_attrs = TRUE,
    synchronizer = NA,
    path = NA,
    zarr_format = 2L
) {
    # Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0e6cdc04c6413e14f57f61d389972ea937c/zarr/hierarchy.py#L1061
    # handle polymorphic store arg
    store <- normalize_store_arg(store)
    path <- normalize_storage_path(path)

    if(overwrite || !contains_group(store)) {
        init_group(
            store,
            overwrite = overwrite,
            chunk_store = chunk_store,
            path = path,
            zarr_format = zarr_format
        )
    }
    return(ZarrGroup$new(
        store,
        read_only = FALSE,
        chunk_store = chunk_store,
        cache_attrs = cache_attrs,
        synchronizer = synchronizer,
        path = path
    ))
}

#' Open a group using file-mode-like semantics.
#' @inheritParams zarr_open
#' @inheritParams zarr_create
#' @param storage_options : dict
#'     If using an fsspec URL to create the store, these will be passed to
#'     the backend implementation. Ignored otherwise.
#' @returns ZarrGroup
#' @export
zarr_open_group <- function(
    store = NA,
    mode = NA,
    cache_attrs = TRUE,
    synchronizer = NA,
    path = NA,
    chunk_store = NA,
    storage_options = NA,
    zarr_format = 2L
) {
    # Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0e6cdc04c6413e14f57f61d389972ea937c/zarr/hierarchy.py#L1119

    if(is_na(mode)) {
        mode <- "a"
    }

    # handle polymorphic store arg
    store <- normalize_store_arg(store, storage_options=storage_options, mode=mode)
    if(!is_na(chunk_store)) {
        chunk_store <- normalize_store_arg(chunk_store, storage_options=storage_options, mode=mode)
    }
    path <- normalize_storage_path(path)

    # Auto-detect V3 format from existing store
    v <- detect_zarr_version(store, path)
    if (!is.na(v) && v == 3L) {
      zarr_format <- 3L
    }

    # ensure store is initialized

    if(mode == "r" || mode == "r+") {
        if (!contains_group(store, path)) {
            if(contains_array(store, path)) {
                stop("ContainsArrayError(path)")
            }
            stop("GroupNotFoundError(path)")
        }

    } else if (mode == 'w') {
        init_group(store, overwrite=TRUE, path=path, chunk_store=chunk_store, zarr_format=zarr_format)

    } else if(mode == "a") {
        if (!contains_group(store, path)) {
            if(contains_array(store, path)) {
                stop("ContainsArrayError(path)")
            }
            init_group(store, path=path, chunk_store=chunk_store, zarr_format=zarr_format)
        }
    } else if(mode == "w-" || mode == "x") {
        if (contains_array(store, path)) {
            stop("ContainsArrayError(path)")
        } else if (contains_group(store, path)) {
            stop("ContainsGroupError(path)")
        } else {
            init_group(store, path=path, chunk_store=chunk_store, zarr_format=zarr_format)
        }
    }

    # determine read only status
    read_only <- (mode == "r")

    return(ZarrGroup$new(
        store,
        read_only=read_only,
        cache_attrs=cache_attrs,
        synchronizer=synchronizer,
        path=path,
        chunk_store=chunk_store
    ))
}

#' Open an array using file-mode-like semantics.
#' @inheritParams init_array
#' @inheritParams zarr_create
#' @inheritParams zarr_open
#' @inheritParams zarr_open_group
#' @returns ZarrArray
#' @export
zarr_open_array <- function(
    store = NA,
    storage_options = NA,
    mode = NA,
    shape = NA,
    chunks=TRUE,
    dtype=NA,
    compressor=NA,
    fill_value=NA,
    order=NA,
    synchronizer=NA,
    overwrite=FALSE,
    path=NA,
    chunk_store=NA,
    filters=NA,
    cache_metadata=TRUE,
    cache_attrs=TRUE,
    object_codec=NA,
    dimension_separator=NA,
    write_empty_chunks=TRUE,
    zarr_format=2L
) {
    # Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0e6cdc04c6413e14f57f61d389972ea937c/zarr/creation.py#L376
    if(is_na(compressor)) {
        compressor <- "default"
    }
    if(is.na(fill_value) && !is.nan(fill_value)) {
        fill_value <- 0
    }
    if(is.na(order)) {
        order <- "F"
    }

    if(is_na(mode)) {
        mode <- "a"
    }

    # handle polymorphic store arg
    store <- normalize_store_arg(store, storage_options=storage_options, mode=mode)
    if(!is_na(chunk_store)) {
        chunk_store <- normalize_store_arg(chunk_store, storage_options=storage_options, mode=mode)
    }
    path <- normalize_storage_path(path)

    # Auto-detect V3 format from existing store
    v <- detect_zarr_version(store, path)
    if (!is.na(v) && v == 3L) {
      zarr_format <- 3L
    }

    # ensure store is initialized
    if(mode == "r" || mode == "r+") {
        if (!contains_array(store, path)) {
            if(contains_group(store, path)) {
                stop("ContainsGroupError(path)")
            }
            stop("ArrayNotFoundError(path)")
        }

    } else if (mode == 'w') {
        init_array(
            store, shape=shape, chunks=chunks, dtype=dtype,
            compressor=compressor, fill_value=fill_value,
            order=order, filters=filters, overwrite=TRUE, path=path,
            object_codec=object_codec, chunk_store=chunk_store,
            zarr_format=zarr_format
        )

    } else if(mode == "a") {
        if (!contains_array(store, path)) {
            if(contains_group(store, path)) {
                stop("ContainGroupError(path)")
            }
            init_array(
                store, shape=shape, chunks=chunks, dtype=dtype,
                compressor=compressor, fill_value=fill_value,
                order=order, filters=filters, path=path,
                object_codec=object_codec, chunk_store=chunk_store,
                zarr_format=zarr_format
            )
        }
    } else if(mode == "w-" || mode == "x") {
        if (contains_group(store, path)) {
            stop("ContainsGroupError(path)")
        } else if (contains_array(store, path)) {
            stop("ContainsArrayError(path)")
        } else {
            init_array(
                store, shape=shape, chunks=chunks, dtype=dtype,
                compressor=compressor, fill_value=fill_value,
                order=order, filters=filters, path=path,
                object_codec=object_codec, chunk_store=chunk_store,
                zarr_format=zarr_format
            )
        }
    }

    # determine read only status
    read_only <- (mode == "r")

    # instantiate array
    z <- ZarrArray$new(
        store,
        path=path,
        chunk_store=chunk_store,
        synchronizer=synchronizer,
        cache_metadata=cache_metadata,
        cache_attrs=cache_attrs,
        read_only=read_only,
        write_empty_chunks=write_empty_chunks
    )
    return(z)
}

#' Convenience function to save a ZarrArray to the local file system.
#' @inheritParams init_array
#' @param arr : ZarrArray
#'     The array with data to save.
#' @param ... Additional arguments to pass to zarr_create_array().
#' @export
zarr_save_array <- function(store, arr, ...) {
    # Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0e6cdc04c6413e14f57f61d389972ea937c/zarr/convenience.py#L112
    store <- normalize_store_arg(store)
    zarr_create_array(data=arr$get_item("..."), shape=arr$get_shape(), store=store, ...)
}

#' Convenience function to open a group or array using file-mode-like semantics.
#' @inheritParams init_array
#' @param mode : \code{'r', 'r+', 'a', 'w', 'w-'}, optional
#'     Persistence mode: 'r' means read only (must exist); 'r+' means
#'     read/write (must exist); 'a' means read/write (create if doesn't
#'     exist); 'w' means create (overwrite if exists); 'w-' means create
#'     (fail if exists).
#' @param ... Additional arguments to pass to zarr_open_array or zarr_open_group.
#' @returns ZarrArray or ZarrGroup
#' @export
zarr_open <- function(store = NA, mode = NA, path = NA, ...) {
    kwargs <- list(...)

    store <- normalize_store_arg(store)
    path <- normalize_storage_path(path)
    
    if(is_na(mode)) {
        mode <- "a"
        if(inherits(store, "HttpStore"))
          mode <- "r"
    }

    if(mode %in% c("w", "w-", "x")) {
        if("shape" %in% names(kwargs)) {
            return(zarr_open_array(store=store, mode=mode, path=path, ...))
        } else {
            return(zarr_open_group(store=store, mode=mode, path=path, ...))
        }
    } else if(mode == "a") {
        if("shape" %in% names(kwargs) || contains_array(store, path)) {
            return(zarr_open_array(store=store, mode=mode, path=path, ...))
        } else {
            return(zarr_open_group(store=store, mode=mode, path=path, ...))
        }
    } else {
        if(contains_array(store, path)) {
            return(zarr_open_array(store=store, mode=mode, path=path, ...))
        } else if(contains_group(store, path)) {
            return(zarr_open_group(store=store, mode=mode, path=path, ...))
        } else {
            stop("PathNotFoundError(path)")
        }
    }
}
