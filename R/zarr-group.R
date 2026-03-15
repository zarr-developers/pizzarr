# Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0/zarr/hierarchy.py#L39

#' The Zarr Group class.
#' @title ZarrGroup Class
#' @docType class
#' @description
#' Instantiate a group from an initialized store.
#'
#' @format [R6::R6Class]
#' @rdname ZarrGroup
#' @export
ZarrGroup <- R6::R6Class("ZarrGroup",
  private = list(
    # store A MutableMapping providing the underlying storage for the group.
    #' @keywords internal
    store = NULL,
    # path Storage path for this group within the store.
    #' @keywords internal
    path = NULL,
    # read_only TRUE if modification operations are not permitted.
    #' @keywords internal
    read_only = NULL,
    # chunk_store Separate storage for array chunks. If not provided, the main store handles both chunks and metadata.
    #' @keywords internal
    chunk_store = NULL,
    # cache_attrs If TRUE (default), user attributes will be cached for faster access.
    #' @keywords internal
    cache_attrs = NULL,
    # synchronizer Object used to synchronize write access to the group and its arrays.
    #' @keywords internal
    synchronizer = NULL,
    # key_prefix Storage path prefix prepended to keys when accessing the store. Set to path + "/" or empty string for root.
    #' @keywords internal
    key_prefix = NULL,
    # meta Parsed group metadata from .zgroup (V2) or zarr.json (V3).
    #' @keywords internal
    meta = NULL,
    # attrs A MutableMapping containing user-defined attributes. Attribute values must be JSON serializable.
    #' @keywords internal
    attrs = NULL,
    # zarr_format Integer 2L or 3L. Detected from the store on initialization.
    #' @keywords internal
    zarr_format = NULL,
    item_path = function(item) {
      # Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0e6cdc04c6413e14f57f61d389972ea937c/zarr/hierarchy.py#L302
      # Check if the first character is a forward slash
      is_absolute <- !is_na(item) && is.character(item) && substr(item, 1, 1) == "/"
      path <- normalize_storage_path(item)
      if(!is_absolute && !is_na(private$path)) {
        path <- paste0(private$key_prefix, path)
      }
      return(path)
    },
    create_group_nosync = function(name, overwrite = FALSE) {
      path <- private$item_path(name)

      # create terminal group (inherit parent's zarr_format)
      init_group(
        private$store,
        path = path,
        overwrite = overwrite,
        chunk_store = self$get_chunk_store(),
        zarr_format = if (!is.null(private$zarr_format)) private$zarr_format else 2L
      )

      # create group instance
      return(ZarrGroup$new(
        private$store,
        path = path,
        read_only = private$read_only,
        chunk_store = self$get_chunk_store(),
        synchronizer = private$synchronizer,
        cache_attrs = private$attrs$cache
      ))
    },
    print_tree = function(prefix, level, depth) {
      if (!is.na(level) && depth >= level) return(invisible(NULL))

      path <- if (is_na(private$path) || private$path == "") "" else private$path
      listdir_key <- if (path == "") NA else path
      entries <- tryCatch(private$store$listdir(listdir_key), error = function(e) character(0))
      # Skip metadata keys
      entries <- entries[!grepl("^\\.", entries) & entries != "zarr.json"]

      # Filter to zarr nodes (arrays and groups)
      children <- character(0)
      for (e in entries) {
        child_path <- if (path == "") e else paste0(path, "/", e)
        if (contains_array(private$store, child_path) ||
            contains_group(private$store, child_path)) {
          children <- c(children, e)
        }
      }

      n <- length(children)
      for (i in seq_along(children)) {
        is_last <- (i == n)
        connector <- if (is_last) "\u2514\u2500\u2500 " else "\u251C\u2500\u2500 "
        child_path <- if (path == "") children[i] else paste0(path, "/", children[i])

        if (contains_array(private$store, child_path)) {
          arr <- ZarrArray$new(private$store, path = child_path, read_only = TRUE)
          shape_str <- paste0("(", paste(arr$get_shape(), collapse = ", "), ")")
          dtype_str <- arr$get_dtype()$dtype
          cat(paste0(prefix, connector, children[i], " ", shape_str, " ", dtype_str, "\n"))
        } else {
          cat(paste0(prefix, connector, children[i], "\n"))
          child_group <- ZarrGroup$new(
            private$store, path = child_path, read_only = TRUE
          )
          child_prefix <- paste0(prefix, if (is_last) "    " else "\u2502   ")
          child_group$.__enclos_env__$private$print_tree(child_prefix, level, depth + 1L)
        }
      }
    },
    create_dataset_nosync = function(name, data = NA, ...) {
      path <- private$item_path(name)

      kwargs <- list(...)

      if(is.null(kwargs[['synchronizer']])) {
        synchronizer <- private$synchronizer
      } else {
        synchronizer <- kwargs[['synchronizer']]
      }
      if(is.null(kwargs[['cache_attrs']])) {
        cache_attrs <- private$attrs$cache
      } else {
        cache_attrs <- kwargs[['cache_attrs']]
      }

      # Inherit parent group's zarr_format unless explicitly overridden
      grp_zarr_format <- if (!is.null(private$zarr_format)) private$zarr_format else 2L

      if(is_na(data)) {
        a <- zarr_create(
          store = private$store,
          path = path,
          chunk_store = self$get_chunk_store(),
          synchronizer = synchronizer,
          cache_attrs = cache_attrs,
          zarr_format = grp_zarr_format,
          ...
        )
      } else {
        a <- zarr_create_array(
          store = private$store,
          path = path,
          data = data,
          chunk_store = self$get_chunk_store(),
          synchronizer = synchronizer,
          cache_attrs = cache_attrs,
          zarr_format = grp_zarr_format,
          ...
        )
      }
      return(a)
    }
  ),
  public = list(
    #' @description
    #' Create a new ZarrGroup instance.
    #' @param store ([Store])\cr
    #'   Group store, already initialized.
    #' @param path (`character(1)`)\cr
    #'   Storage path.
    #' @param read_only (`logical(1)`)\cr
    #'   Whether the group is read-only.
    #' @param chunk_store ([Store] or `NA`)\cr
    #'   Separate storage for chunks.
    #' @param synchronizer (`ANY` or `NA`)\cr
    #'   Object used to synchronize write access.
    #' @param cache_attrs (`logical(1)`)\cr
    #'   Whether to cache attributes.
    #' @return A `ZarrGroup` instance.
    initialize = function(store, path = NA, read_only = FALSE, 
                          chunk_store = NA, cache_attrs = TRUE, 
                          synchronizer = NA) {
      private$store <- store
      private$path <- normalize_storage_path(path)
      if(!is_na(private$path) && private$path != "") {
        private$key_prefix <- paste0(private$path, "/")
      } else {
        private$key_prefix <- ""
      }
      private$read_only <- read_only
      private$chunk_store <- chunk_store

      # guard conditions
      # contains_array() checks both V2 and V3 formats
      if(contains_array(store, path)) {
        stop("ContainsArrayError(path)")
      }

      # Detect V3 vs V2 and load metadata accordingly
      v3_key <- paste0(private$key_prefix, ZARR_JSON)
      if (store$contains_item(v3_key)) {
        # V3 group: read zarr.json, validate as group
        private$zarr_format <- 3L
        meta_bytes <- store$get_item(v3_key)
        meta3 <- Metadata3$new()
        private$meta <- meta3$decode_group_metadata(meta_bytes)

        # V3 attributes are embedded in zarr.json
        a_key <- paste0(private$key_prefix, ATTRS_KEY)
        private$attrs <- Attributes$new(store, key = a_key, read_only = read_only,
                                        cache = cache_attrs, synchronizer = synchronizer,
                                        zarr_format = 3L)
        if (!is.null(private$meta$attributes)) {
          private$attrs$set_cached_v3_attrs(private$meta$attributes)
        }
      } else {
        # V2 group: existing logic unchanged
        private$zarr_format <- 2L
        m_key <- paste0(private$key_prefix, GROUP_META_KEY)

        # use consolidated metadata if exists
        meta <- try_from_zmeta(m_key, store)

        if(!is.null(meta)) {
          private$meta <- meta
        } else {
          # initialize metadata
          meta_bytes <- tryCatch({
            meta_bytes <- store$get_item(m_key)
          }, error = function(cond) {
            if(is_key_error(cond)) {
              stop("GroupNotFoundError(path) in Group$new")
            } else {
              stop(cond$message)
            }
          })

          if(!is.null(meta_bytes))
            private$meta <- private$store$metadata_class$decode_group_metadata(meta_bytes)
        }

        # V2 attributes in separate .zattrs file
        a_key <- paste0(private$key_prefix, ATTRS_KEY)
        private$attrs <- Attributes$new(store, key = a_key, read_only = read_only,
                                        cache = cache_attrs, synchronizer = synchronizer,
                                        zarr_format = 2L)
      }
    },
    #' @description
    #' Get group store
    #' @return [Store].
    get_store = function() {
      return(private$store)
    },
    #' @description
    #' Get group path
    #' @return `character(1)`.
    get_path = function() {
      return(private$path)
    },
    #' @description
    #' Get group metadata
    #' @return `list()` or `NULL`.
    get_meta = function() {
      return(private$meta)
    },
    #' @description
    #' Get group name
    #' @return `character(1)`.
    get_name = function() {
      if(!is_na(private$path)) {
        name <- private$path
        if(substr(name, 1, 1) != "/") {
          name <- paste0("/", name)
        }
        return(name)
      }
      return("/")
    },
    #' @description
    #' Is store read only?
    #' @return `logical(1)`.
    get_read_only = function() {
      return(private$read_only)
    },
    #' @description
    #' Get group chunk store
    #' @return [Store].
    get_chunk_store = function() {
      if(is_na(private$chunk_store)) {
        return(private$store)
      }
      return(private$chunk_store)
    },
    #' @description
    #' Get group synchronizer
    #' @return Synchronizer object or `NA`.
    get_synchronizer = function() {
      return(private$synchronizer)
    },
    #' @description
    #' Get group attributes
    #' @return [Attributes].
    get_attrs = function() {
      return(private$attrs)
    },
    #' @description
    #' Test for group membership.
    #' @param item Character item to test for.
    #' @return `logical(1)`.
    contains_item = function(item) {
      path <- private$item_path(item)
      return(contains_array(private$store, path) || contains_group(private$store, path))
    },
    #' @description
    #' Obtain a group member.
    #' @param item Character item to retrieve.
    #' @return [ZarrArray] or [ZarrGroup].
    get_item = function(item) {
      if(is.null(item)) {
        #for case with no internet
        stop("item can not be null")
      }
      path <- private$item_path(item)
      if(contains_array(private$store, path)) {
        return(ZarrArray$new(
          private$store,
          path = path,
          read_only = private$read_only,
          chunk_store = self$get_chunk_store(),
          synchronizer = private$synchronizer,
          cache_attrs = private$attrs$cache
        ))
      } else if(contains_group(private$store, path)) {
        return(ZarrGroup$new(
          private$store,
          path = path,
          read_only = private$read_only,
          chunk_store = self$get_chunk_store(),
          synchronizer = private$synchronizer,
          cache_attrs = private$attrs$cache
        ))
      } else {
        stop("KeyError: item")
      }
    },
    #' @description
    #' create a group
    #' @param name Character group name.
    #' @param overwrite Logical overwrite.
    #' @return [ZarrGroup].
    create_group = function(name, overwrite = FALSE) {
      return(private$create_group_nosync(name, overwrite = overwrite))
    },
    #' @description
    #' Create a dataset (array) within this group.
    #' @param name Character dataset name.
    #' @param data Data to add to group.
    #' @param ... Extra arguments to pass to `zarr_create()` or `array()`.
    #' @return [ZarrArray].
    create_dataset = function(name, data = NA, ...) {
      return(private$create_dataset_nosync(name, data = data, ...))
    },
    #' @description
    #' Print a human-readable summary of the group.
    #' @param ... Ignored.
    #' @return `self` (invisibly).
    print = function(...) {
      nm <- self$get_name()
      store_type <- class(private$store)[1]
      zf <- if (!is.null(private$zarr_format)) private$zarr_format else 2L
      # Count zarr node members
      path <- if (is_na(private$path) || private$path == "") "" else private$path
      listdir_key <- if (path == "") NA else path
      entries <- tryCatch(private$store$listdir(listdir_key), error = function(e) character(0))
      entries <- entries[!grepl("^\\.", entries) & entries != "zarr.json"]
      n_members <- 0L
      for (e in entries) {
        child_path <- if (path == "") e else paste0(path, "/", e)
        if (contains_array(private$store, child_path) ||
            contains_group(private$store, child_path)) {
          n_members <- n_members + 1L
        }
      }
      cat(paste0("<ZarrGroup> ", nm, "\n"))
      cat(paste0("  Store type  : ", store_type, "\n"))
      cat(paste0("  Zarr format : ", zf, "\n"))
      cat(paste0("  Read-only   : ", private$read_only, "\n"))
      cat(paste0("  No. members : ", n_members, "\n"))
      invisible(self)
    },
    #' @description
    #' Print an ASCII tree showing the group hierarchy.
    #' @param level Maximum depth to display. `NA` (default) shows all levels.
    #' @return `self` (invisibly).
    tree = function(level = NA) {
      nm <- self$get_name()
      cat(nm, "\n")
      private$print_tree("", level, 0L)
      invisible(self)
    }
    # TODO: convenience functions like zeros, ones, empty, full, ...
  )
)
