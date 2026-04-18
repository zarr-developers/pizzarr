#' @keywords internal
path_to_prefix <- function(path) {
    # Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0e6cdc04c6413e14f57f61d389972ea937c/zarr/_storage/store.py#L134
    # assume path already normalized
    if(!is.na(path) && stringr::str_length(path) > 0) {
        prefix <- paste0(path, '/')
    } else {
        prefix <- ""
    }
    return(prefix)
}

#' @keywords internal
contains_array <- function(store, path=NA) {
    # Return True if the store contains an array at the given logical path.
    # Checks V2 (.zarray) first, then V3 (zarr.json with node_type="array").
    path <- normalize_storage_path(path)
    prefix <- path_to_prefix(path)
    # V2 check
    key <- paste0(prefix, ARRAY_META_KEY)
    ret <- store$contains_item(key)
    if (!is.null(ret) && ret) return(TRUE)
    # V3 check
    return(contains_array_v3(store, path))
}

#' @keywords internal
contains_group <- function(store, path=NA) {
    # Return True if the store contains a group at the given logical path.
    # Checks V2 (.zgroup) first, then V3 (zarr.json with node_type="group").
    path <- normalize_storage_path(path)
    prefix <- path_to_prefix(path)
    # V2 check
    key <- paste0(prefix, GROUP_META_KEY)
    ret <- store$contains_item(key)
    if (!is.null(ret) && ret) return(TRUE)
    # V3 check
    return(contains_group_v3(store, path))
}

# V3 format detection functions.
# Reference: https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html
# In V3, both arrays and groups are identified by zarr.json with a node_type field.

# Fetch and parse zarr.json node_type at a given path.
# Returns the node_type string ("array" or "group") or NULL on failure.
# @keywords internal
get_v3_node_type <- function(store, path = NA) {
  path <- normalize_storage_path(path)
  prefix <- path_to_prefix(path)
  key <- paste0(prefix, ZARR_JSON)
  if (!store$contains_item(key)) return(NULL)

  tryCatch({
    raw_meta <- store$get_item(key)
    meta <- try_fromJSON(rawToChar(raw_meta), simplifyVector = FALSE)
    if (!is.null(meta$zarr_format) && meta$zarr_format == 3 &&
        !is.null(meta$node_type)) {
      meta$node_type
    } else {
      NULL
    }
  }, error = function(e) {
    NULL
  })
}

# Check if the store contains a V3 array at the given path.
# @keywords internal
contains_array_v3 <- function(store, path = NA) {
  identical(get_v3_node_type(store, path), "array")
}

# Check if the store contains a V3 group at the given path.
# @keywords internal
contains_group_v3 <- function(store, path = NA) {
  identical(get_v3_node_type(store, path), "group")
}

# Detect zarr format version at a given path.
# Reference: zarr-python checks zarr.json first (V3), then .zarray/.zgroup (V2).
# If both exist, V3 takes precedence with a warning.
#
# @param store A Store instance.
# @param path Storage path (NA for root).
# @return Integer 2L, 3L, or NA_integer_ if neither format detected.
# @keywords internal
detect_zarr_version <- function(store, path = NA) {
  path <- normalize_storage_path(path)
  prefix <- path_to_prefix(path)

  has_v3 <- store$contains_item(paste0(prefix, ZARR_JSON))
  has_v2_array <- store$contains_item(paste0(prefix, ARRAY_META_KEY))
  has_v2_group <- store$contains_item(paste0(prefix, GROUP_META_KEY))
  has_v2 <- has_v2_array || has_v2_group

  if (has_v3 && has_v2) {
    warning("Both V2 and V3 metadata found at path '", path,
            "'. Using V3.")
    return(3L)
  }
  if (has_v3) return(3L)
  if (has_v2) return(2L)
  NA_integer_
}
