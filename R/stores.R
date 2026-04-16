#' Abstract store for Zarr
#' @title Store Class
#' @docType class
#' @description
#' Class representing an abstract store
#'
#' @format [R6::R6Class]
#' @family Store classes
#' @rdname Store
#' @export
Store <- R6::R6Class("Store",
   private = list(
      readable = NULL,
      writeable = NULL,
      erasable = NULL,
      listable = NULL,
      store_version = NULL,
      zmetadata = NULL,
      #' @keywords internal
      listdir_from_keys = function(path) {
        stop("NotImplementedError(listdir_from_keys)")
      },
      #' @keywords internal
      rename_from_keys = function(src_path, dst_path) {
        stop("NotImplementedError(rename_from_keys)")
      },
      #' @keywords internal
      rmdir_from_keys = function(path) {
        stop("NotImplementedError(rmdir_from_keys)")
      }
   ),
   public = list(
    #' @field metadata_class The metadata encoder/decoder for this store.
    #' @keywords internal
    metadata_class = NULL,
    #' @description 
    #' Create a `Store` object 
    initialize = function() {
      private$readable <- TRUE
      private$writeable <- TRUE
      private$erasable <- TRUE
      private$listable <- TRUE
      private$store_version <- 2
      self$metadata_class <- Metadata2$new()
    },
    #' @description
    #' Test if Store is readable.
    #' @return Logical.
    is_readable = function() {
      return(private$readable)
    },
    #' @description
    #' Test if Store is writeable.
    #' @return Logical.
    is_writeable = function() {
      return(private$writeable)
    },
    #' @description
    #' Test if Store is eraseable.
    #' @return Logical.
    is_erasable = function() {
      return(private$erasable)
    },
    #' @description
    #' Test if Store is listable.
    #' @return Logical.
    is_listable = function() {
      return(private$listable)
    },
    #' @description
    #' Close the store.
    #' @return `NULL`.
    close = function() {
      # Do nothing by default
    },
    #' @description
    #' List the store directory.
    #' @param path character path.
    #' @return Character vector of keys.
    listdir = function(path=NA) {
      if(is.na(path)) {
        path <- ""
      }
      path <- normalize_storage_path(path)
      return(private$listdir_from_keys(path))
    },
    #' @description
    #' Rename a Store path.
    #' @param src_path character source path.
    #' @param dst_path character destination path.
    #' @return `NULL` (called for side effects).
    rename = function(src_path, dst_path) {
      if(!self$is_erasable()) {
        stop("Store is not erasable, cannot call 'rename'")
      }
      private$rename_from_keys(src_path, dst_path)
    },
    #' @description
    #' Remove a path within a Store.
    #' @param path character path.
    #' @return `NULL` (called for side effects).
    rmdir = function(path) {
      if(!self$is_erasable()) {
        stop("Store is not erasable, cannot call 'rmdir'")
      }
      path <- normalize_storage_path(path)
      private$rmdir_from_keys(path)
    },
     #' @description
     #' Get an item from the store.
     #' @param key The item key.
     #' @return The item data in a vector of type raw.
     get_item = function(key) {

     },
     #' @description
     #' Set an item in the store.
     #' @param key The item key.
     #' @param value The item value as a vector of type raw.
     #' @return `NULL` (called for side effects).
     set_item = function(key, value) {

     },
     #' @description
     #' Determine whether the store contains an item.
     #' @param key The item key.
     #' @return A boolean value.
     contains_item = function(key) {

     },
    #' @description
    #' Delete an item from the store.
    #' @param key The item key.
    #' @return `NULL` (called for side effects).
    delete_item = function(key) {
      stop("NotImplementedError(delete_item)")
    },
    #' @description
    #' Get consolidated metadata if it exists.
    #' @return A list or `NULL`.
    get_consolidated_metadata = function() {
      return(private$zmetadata)
    },
    #' @description
    #' Return a store identifier for zarrs dispatch.
    #'
    #' Subclasses override this to return a filesystem path or URL.
    #' Returns `NULL` by default (signals: use R-native path).
    #' @return A character string or `NULL`.
    get_store_identifier = function() {
      NULL
    },
    #' @description
    #' Print a human-readable summary of the store.
    #' @param ... Ignored.
    #' @return `self` (invisibly).
    print = function(...) {
      cat(paste0("<", class(self)[1], ">\n"))
      invisible(self)
    }
   )
)

# Reference: https://github.com/zarr-developers/zarr_implementations/blob/c0bd932/generate_data/js/src/fsstore.js#L7

#' DirectoryStore for Zarr
#' @title DirectoryStore Class
#' @docType class
#' @description
#' Store class using directories and files on a standard file system.
#'
#' @format [R6::R6Class] inheriting from [Store].
#' @family Store classes
#' @rdname DirectoryStore
#' @export
DirectoryStore <- R6::R6Class("DirectoryStore",
  inherit = Store,
  public = list(
    #' @field root The path to the root of the store.
    #' @keywords internal
    root = NULL,
    #' @description
    #' Create a new file system store.
    #' @param root The path to the root of the store.
    #' @return A new `DirectoryStore` object.
    initialize = function(root) {
      super$initialize()
      self$root <- root
      if(!dir.exists(root)) {
        dir.create(root, recursive = TRUE, showWarnings = FALSE)
      }
    },
    #' @description
    #' Get an item from the store.
    #' @param key The item key.
    #' @return The item data in a vector of type raw.
    get_item = function(key) {
      fp <- file.path(self$root, key)
      if(!file.exists(fp)) {
        stop("KeyError:", key)
      }
      fp_size <- file.info(fp)$size
      fp_pointer <- file(fp, "rb")
      fp_data <- readBin(fp_pointer, what = "raw", n = fp_size)
      close(fp_pointer)
      return(fp_data)
    },
    #' @description
    #' Set an item in the store.
    #' @param key The item key.
    #' @param value The item value as a vector of type raw.
    #' @return `NULL` (called for side effects).
    set_item = function(key, value) {
      fp <- file.path(self$root, key)
      dir.create(dirname(fp), recursive = TRUE, showWarnings = FALSE)
      fp_pointer <- file(fp, "wb")
      writeBin(value, fp_pointer)
      close(fp_pointer)
    },
    #' @description
    #' Determine whether the store contains an item.
    #' @param key The item key.
    #' @return A boolean value.
    contains_item = function(key) {
      fp <- file.path(self$root, key)
      return(file.exists(fp))
    },
    #' @description
    #' Delete an item from the store.
    #' @param key The item key.
    #' @return `NULL` (called for side effects).
    delete_item = function(key) {
      fp <- file.path(self$root, key)
      if(file.exists(fp)) {
        file.remove(fp)
      }
    },
    #' @description
    #' Remove a path within a Store.
    #' @param path Character path.
    #' @return `NULL` (called for side effects).
    rmdir = function(path=NA) {
      path <- normalize_storage_path(path)
      dir_path <- self$root
      if(!is.na(path)) {
        dir_path <- file.path(self$root, path)
      }
      if(dir.exists(dir_path)) {
        unlink(dir_path, recursive = TRUE)
      }
    },
    #' @description
    #' List the store directory.
    #' @param key Character key.
    #' @return `character()` vector of entries.
    listdir = function(key=NA) {
      if(is_na(key)) {
        dir_path <- self$root
      } else {
        dir_path <- file.path(self$root, key)
      }
      if(!dir.exists(dir_path)) {
        stop("KeyError:", key)
      }
      dir_list <- sort(list.files(dir_path, full.names = FALSE, all.files = TRUE, include.dirs = TRUE))
      dir_list <- dir_list[!dir_list %in% c(".", "..")]
      return(dir_list)
    },
    #' @description
    #' Print a human-readable summary of the store.
    #' @description
    #' Return the absolute filesystem path for zarrs dispatch.
    #' @return A character string.
    get_store_identifier = function() {
      normalizePath(self$root, mustWork = FALSE)
    },
    #' @param ... Ignored.
    #' @return `self` (invisibly).
    print = function(...) {
      cat(paste0("<DirectoryStore> file://", self$root, "\n"))
      invisible(self)
    }
  )
)


# Reference: https://github.com/zarr-developers/zarr-python/blob/a5dfc3b4/zarr/storage.py#L512

#' MemoryStore for Zarr
#' @title MemoryStore Class
#' @docType class
#' @description
#' Store class that uses a hierarchy of list objects,
#' thus all data will be held in main memory.
#'
#' @format [R6::R6Class] inheriting from [Store].
#' @family Store classes
#' @rdname MemoryStore
#' @export
MemoryStore <- R6::R6Class("MemoryStore",
   inherit = Store,
   public = list(
     #' @field root The root list for the store.
     #' @keywords internal
     root = NULL,
     #' @description
     #' Create a new memory store.
     #' @return A new `MemoryStore` object.
     initialize = function() {
      super$initialize()
       self$root <- obj_list()
     },
     #' @description
     #' Get the parent of an item.
     #' @keywords internal
     #' @param item The item key.
     #' @return A list with the keys `parent` and `key`.
     get_parent = function(item) {
       parent <- self$root
       segments <- strsplit(item, "/")[[1]]
       for(k in segments[1:length(segments)-1]) {
         parent <- parent[[k]]
         if(!is.list(parent)) {
           stop("KeyError:", item)
         }
       }
       return(list(parent = parent, key = segments[length(segments)]))
     },
     #' @description
     #' Get an item from the store.
     #' @param item The item key.
     #' @return The item data in a vector of type raw.
     get_item = function(item=NA) {
       if(is_na(item)) {
        return(self$root)
       }
       parent_and_key <- self$get_parent(item)
       parent <- parent_and_key$parent
       key <- parent_and_key$key

       if(key %in% names(parent)) {
         value <- parent[[key]]
       } else {
         stop("KeyError:", item)
       }
       return(value)
     },
     #' @description
     #' Set an item in the store.
     #' @param item The item key.
     #' @param value The item value as a vector of type raw.
     #' @return `NULL` (called for side effects).
     set_item = function(item, value) {
       segments <- strsplit(item, "/")[[1]]
       if(length(segments) > 1) {
         for(i in 1:(length(segments)-1)) {
           k <- segments[i]
           if(i == 1 && k %in% names(self$root)) {
             if(!is.list(self$root[[k]])) {
               stop("KeyError:", item)
             }
           } else if(i > 1 && k %in% names(self$root[[segments[1:(i-1)]]])) {
             if(!is.list(self$root[[segments[1:i]]])) {
               stop("KeyError:", item)
             }
           } else {
             self$root[[segments[1:i]]] <- obj_list()
           }
         }
       }
       self$root[[segments]] <- value
     },
     #' @description
     #' Determine whether the store contains an item.
     #' @param item The item key.
     #' @return A boolean value.
     contains_item = function(item) {
       result <- tryCatch({
         parent_and_key <- self$get_parent(item)
         parent <- parent_and_key$parent
         key <- parent_and_key$key
         if(key %in% names(parent)) {
           value <- parent[[key]]
           return(!is.list(value))
         } else {
           return(FALSE)
         }
       }, error = function(e) {
         return(FALSE)
       })
       return(result)
     },
     #' @description
     #' List the store directory.
     #' @param key Character key.
     #' @return `character()` vector of entries.
     listdir = function(key=NA) {
      item <- self$get_item(key)
      if(!is.list(item)) {
        stop("KeyError:", key)
      }
      return(sort(names(item)))
     },
     #' @description
     #' Delete an item from the store.
     #' @param key The item key.
     #' @return `NULL` (called for side effects).
     delete_item = function(key) {
      self$set_item(key, NULL)
     },
     #' @description
     #' Remove a path within a Store.
     #' @param item Character item.
     #' @return `NULL` (called for side effects).
     rmdir = function(item) {
      if(is_na(item) || item == "") {
        self$root <- obj_list()
      } else {
        self$set_item(item, NULL)
      }
     },
     #' @description
     #' Print a human-readable summary of the store.
     #' @param ... Ignored.
     #' @return `self` (invisibly).
     print = function(...) {
       cat("<MemoryStore>\n")
       invisible(self)
     }
   )
)

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

# Reference: https://github.com/manzt/zarrita.js/blob/main/packages/storage/src/fetch.ts

#' @title HttpStore Class
#' @docType class
#' @description
#' Store class that uses HTTP requests.
#' Read-only. Depends on the `crul` package.
#' @format [R6::R6Class] inheriting from [Store].
#' @family Store classes
#' @rdname HttpStore
#' @importFrom memoise memoise timeout
#' @export
HttpStore <- R6::R6Class("HttpStore",
  inherit = Store,
  private = list(
    url = NULL,
    base_path = NULL,
    domain = NULL,
    options = NULL,
    headers = NULL,
    client = NULL,
    zmetadata = NULL,
    make_request_memoized = NULL,
    cache_enabled = NULL,
    cache_time_seconds = NULL,
    make_request = function(item) {
      key <- item_to_key(item)
      path <- paste(private$base_path, key, sep="/")

      ret <- try_from_zmeta(key, self)

      if(!is.null(ret)) return(ret)

      tryCatch(private$client$get(path = path),
               error = function(e) {
                 warning("Can't proceed, web request failed. Error was:", e)
                 NULL
               })
    },
    memoize_make_request = function() {
      if(private$cache_enabled) {
        private$make_request_memoized <-  memoise(
          function(key) private$make_request(key), 
          ~timeout(private$cache_time_seconds)
        )
      } else {
        private$make_request_memoized <- private$make_request
      }
    },
    get_zmetadata = function() {
      res <- private$make_request(".zmetadata")
      
      if(!is.null(res$status_code) && res$status_code == 200) {
        out <- try_fromJSON(res$parse("UTF-8"))
      } else out <- NULL
      
      return(out)
    }
  ),
  public = list(
    #' @description
    #' Create a `HttpStore` object
    #' @param url (`character(1)`)\cr
    #'   URL of the store.
    #' @param options (`list()` or `NA`)\cr
    #'   Options passed to crul.
    #' @param headers (`list()` or `NA`)\cr
    #'   Headers passed to crul.
    #' @return A new `HttpStore` object.
    initialize = function(url, options = NA, headers = NA) {
      super$initialize()
      # Remove trailing slash if necessary.
      if(substr(url, nchar(url), nchar(url)) == "/") {
        private$url <- substr(url, 1, nchar(url)-1)
      } else {
        private$url <- url
      }
      private$options <- options
      private$headers <- headers

      segments <- stringr::str_split(private$url, "/")[[1]]
      private$domain <- paste(segments[1:3], collapse="/")

      # Support both cases in which the store is located at the root
      # of the domain, or located under some base_path.

      private$base_path <- ifelse(length(segments) == 3, "", 
                                  paste(segments[4:length(segments)], collapse="/"))
      
      if(!requireNamespace("crul", quietly = TRUE)) {
        stop("HttpStore requires the crul package")
      }

      private$client <- crul::HttpClient$new(
        url = private$domain,
        opts = private$options,
        headers = private$headers
      )

      private$cache_time_seconds <- getOption("pizzarr.http_store_cache_time_seconds")
      private$cache_enabled <- private$cache_time_seconds > 0

      private$memoize_make_request()
      
      private$zmetadata <- private$get_zmetadata()
    },
    #' @description
    #' Get an item from the store.
    #' @param item The item key.
    #' @return The item data in a vector of type raw.
    get_item = function(item) {
      res <- private$make_request_memoized(item)
      return(res$content)
    },
    #' @description
    #' Determine whether the store contains an item.
    #' @param item The item key.
    #' @return A boolean value.
    contains_item = function(item) {
      # use consolidated metadata if it exists
      if(!is.null(try_from_zmeta(item_to_key(item), self))) {
        return(TRUE)
      } else if(!is.null(self$get_consolidated_metadata())) {
        return(FALSE)
      } else {
        res <- private$make_request_memoized(item)
        
        return(!is.null(res$status_code) && res$status_code == 200)        
      }

    },
    #' @description
    #' Fetches .zmetadata from the store evaluates its names
    #' @return Character vector of unique keys that do not start with a `.`.
    listdir = function() {

      if(!is.null(private$zmetadata)) {
        tryCatch({
          out <- names(private$zmetadata$metadata) |>
            stringr::str_subset("^\\.", negate = TRUE) |>
            stringr::str_split("/") |>
            vapply(\(x) head(x, 1), "") |>
            unique()
        }, error = \(e) warning("\n\nError parsing .zmetadata:\n\n", e))
      } else {
        out <- NULL
        message(".zmetadata not found for this http store. Can't listdir")
      }
      
      return(out)
      
    },
    #' @description
    #' Get cache time of http requests.
    #' @return `numeric(1)`.
    get_cache_time_seconds = function() {
      return(private$cache_time_seconds)
    },
    #' @description
    #' Set cache time of http requests.
    #' @param seconds Number of seconds until cache is invalid -- 0 for no cache.
    #' @return `NULL` (called for side effects).
    set_cache_time_seconds = function(seconds) {
      private$cache_time_seconds <- seconds
      # We need to re-memoize.
      private$memoize_make_request()
    },
    #' @description
    #' Print a human-readable summary of the store.
    #' @description
    #' Return the store URL for zarrs dispatch.
    #' @return A character string.
    get_store_identifier = function() {
      private$url
    },
    #' @param ... Ignored.
    #' @return `self` (invisibly).
    print = function(...) {
      cat(paste0("<HttpStore> ", private$url, "\n"))
      invisible(self)
    }
  )
)
