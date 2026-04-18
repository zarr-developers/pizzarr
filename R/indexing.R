# Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0/zarr/indexing.py#L61

# Check whether a selection contains only integer array-likes
# This is used to determine whether zarr.array$get_item() calls vectorized (inner) indexing
is_pure_fancy_indexing <- function(selection, ndim = length(selection)) {
    
  # one dimensions case
  if(ndim == 1){
    if(is_integer_vec(selection) | is_integer_list(selection)){
      return(TRUE)
    }
  } 
  
  # check if there are any slice objects 
  no_slicing <- (length(selection) == ndim) & !(any(sapply(selection, function(s) inherits(s, "Slice"))))
  
  # check for integer vectors
  all_integer <- all(sapply(selection, function(sel){
    (is_integer(sel) | is_integer_list(sel)) | is_integer_vec(sel)
  }))
  any_integer <- any(sapply(selection, function(sel){
    # is_integer_list(sel) | is_integer_vec(sel)
    is_integer_list(sel) | is_integer_vec(sel) | is_integer(sel)
  }))
  
  # return
  return((no_slicing & all_integer) & any_integer)
}


# Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0/zarr/indexing.py#L655

#' The Zarr OIndex class.
#' @title OIndex Class
#' @docType class
#' @description
#'  Orthogonal index class
#' @rdname OIndex
#' @keywords internal
OIndex <- R6::R6Class("OIndex",
  public = list(
    #' @field array The array instance.
    #' @keywords internal
    array = NULL,
    #' @description
    #' Create a new OIndex instance.
    #' @param array The array instance.
    #' @return An `OIndex` instance.
    initialize = function(array) {
      self$array <- array
    },
    #' @description
    #' get method for the Oindex instance
    #' @param selection Selection to apply.
    #' @return An `OIndex` instance.
    get_item = function(selection) {
      # self$array <- array$get
      self$array$get_orthogonal_selection(selection)
    }
  )
)

# Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0/zarr/indexing.py#L811

#' The Zarr VIndex class.
#' @title VIndex Class
#' @docType class
#' @description
#'  Vectorized index class
#' @rdname VIndex
#' @keywords internal
VIndex <- R6::R6Class("VIndex",
  public = list(
    #' @field array The array instance.
    #' @keywords internal
    array = NULL,
    #' @description
    #' Create a new VIndex instance.
    #' @param array The array instance.
    #' @return A `VIndex` instance.
    initialize = function(array) {
      self$array <- array
    }
  )
)

# Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0/zarr/indexing.py#L138

#' The Zarr IntDimIndexer class.
#' @title IntDimIndexer Class
#' @docType class
#' @description
#' Indexer for a single integer selection along one dimension.
#' @rdname IntDimIndexer
#' @keywords internal
IntDimIndexer <- R6::R6Class("IntDimIndexer",
  inherit = DimIndexer,
  public = list(
    #' @field dim_sel Normalized integer index for this dimension.
    #' @keywords internal
    dim_sel = NULL,
    #' @field dim_len Length of the array along this dimension.
    #' @keywords internal
    dim_len = NULL,
    #' @field dim_chunk_len Length of a chunk along this dimension.
    #' @keywords internal
    dim_chunk_len = NULL,
    #' @description
    #' Create a new IntDimIndexer instance.
    #' @param dim_sel Integer dimension selection.
    #' @param dim_len Integer dimension length.
    #' @param dim_chunk_len Integer dimension chunk length.
    #' @return A `IntDimIndexer` instance.
    initialize = function(dim_sel, dim_len, dim_chunk_len) {
      # Normalize
      dim_sel <- normalize_integer_selection(dim_sel, dim_len)

      self$dim_sel <- dim_sel
      self$dim_len <- dim_len
      self$dim_chunk_len <- dim_chunk_len
      self$num_items <- 1
    },
    #' @description
    #' Compute the chunk dimension projection for the single selected index.
    #' @return a `ChunkDimProjection` instance
    iter = function() {
      # TODO: use generator/yield features from async package
      dim_chunk_index <- floor(self$dim_sel / self$dim_chunk_len)
      dim_offset <- dim_chunk_index * self$dim_chunk_len
      dim_chunk_sel <- self$dim_sel - dim_offset
      dim_out_sel <- NA
      return(list(
        ChunkDimProjection$new(
          dim_chunk_index,
          dim_chunk_sel,
          dim_out_sel
        )
      ))
    }
  )
)

# Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0/zarr/indexing.py#L163

#' The Zarr SliceDimIndexer class.
#' @title SliceDimIndexer Class
#' @docType class
#' @description
#' Indexer for a slice selection along one dimension.
#' @rdname SliceDimIndexer
#' @keywords internal
SliceDimIndexer <- R6::R6Class("SliceDimIndexer",
  inherit = DimIndexer,
  public = list(
    #' @field dim_len Dimension length.
    #' @keywords internal
    dim_len = NULL,
    #' @field dim_chunk_len Dimension chunk length.
    #' @keywords internal
    dim_chunk_len = NULL,
    #' @field num_chunks Number of chunks.
    #' @keywords internal
    num_chunks = NULL,
    #' @field start Start index.
    #' @keywords internal
    start = NULL,
    #' @field stop Stop index.
    #' @keywords internal
    stop = NULL,
    #' @field step Step size.
    #' @keywords internal
    step = NULL,
    #' @description
    #' Create a new SliceDimIndexer instance.
    #' @param dim_sel Integer dimension selection.
    #' @param dim_len Integer dimension length.
    #' @param dim_chunk_len Integer dimension chunk length.
    #' @return A `SliceDimIndexer` instance.
    initialize = function(dim_sel, dim_len, dim_chunk_len) {
      # Reference: https://github.com/gzuidhof/zarr.js/blob/292804/src/core/indexing.ts#L311
      si <- dim_sel$indices(dim_len)
      self$start <- si[1]
      self$stop <- si[2]
      self$step <- si[3]
      if(self$step < 1) {
        stop('NegativeStepError')
      }
      self$dim_len <- dim_len
      self$dim_chunk_len <- dim_chunk_len
      self$num_items <- max(0, ceiling((self$stop - self$start) / self$step))
      self$num_chunks <- ceiling(self$dim_len / self$dim_chunk_len)
    },
    #' @description
    #' Iterate over chunks that overlap the slice, yielding a ChunkDimProjection for each.
    #' @return A list of ChunkDimProjection objects.
    iter = function() {
      # TODO: use generator/yield features from async package
      dim_chunk_index_from <- floor(self$start / self$dim_chunk_len)
      dim_chunk_index_to <- ceiling(self$stop / self$dim_chunk_len)

      # START R-SPECIFIC
      if(dim_chunk_index_from == dim_chunk_index_to) {
        dim_chunk_index_to <- dim_chunk_index_to + 1
      }
      # END R-SPECIFIC

      # Iterate over chunks in range
      result <- list()
      for(dim_chunk_index in seq(from = dim_chunk_index_from, to = (dim_chunk_index_to - 1), by = 1)) {

        # Compute offsets for chunk within overall array
        dim_offset <- dim_chunk_index * self$dim_chunk_len
        dim_limit <- min(self$dim_len, (dim_chunk_index + 1) * self$dim_chunk_len)

        # Determine chunk length, accounting for trailing chunk
        dim_chunk_len <- dim_limit - dim_offset

        dim_chunk_sel_start <- 0
        dim_chunk_sel_stop <- 0
        dim_out_offset <- 0

        if(self$start < dim_offset) {
          # Selection starts before the current chunk

          dim_chunk_sel_start <- 0
          remainder <- (dim_offset - self$start) %% self$step
          if(remainder > 0) {
            dim_chunk_sel_start <- dim_chunk_sel_start + (self$step - remainder)
          }
          # Compute number of previous items, provides offset into output array
          dim_out_offset <- ceiling((dim_offset - self$start) / self$step)
        } else {
          # Selection starts within the current chunk
          dim_chunk_sel_start <- self$start - dim_offset
          dim_out_offset <- 0
        }

        if(self$stop > dim_limit) {
          # Selection ends after current chunk
          dim_chunk_sel_stop <- self$dim_chunk_len
        } else {
          # Selection ends within current chunk
          dim_chunk_sel_stop <- self$stop - dim_offset
        }

        dim_chunk_sel <- zb_slice(dim_chunk_sel_start, dim_chunk_sel_stop, self$step)
        dim_chunk_num_items <- ceiling((dim_chunk_sel_stop - dim_chunk_sel_start) / self$step)
        dim_out_sel <- zb_slice(dim_out_offset, dim_out_offset + dim_chunk_num_items)

        result <- append(result, ChunkDimProjection$new(
          dim_chunk_index,
          dim_chunk_sel,
          dim_out_sel
        ))
      }
      return(result)
    }
  )
)

# Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0/zarr/indexing.py#L326

#' The Zarr BasicIndexer class.
#' @title BasicIndexer Class
#' @docType class
#' @description
#'  An indexer class to normalize a selection of an array and provide an iterator 
#'  of indexes over the dimensions of an array.
#' @rdname BasicIndexer
#' @keywords internal
BasicIndexer <- R6::R6Class("BasicIndexer",
  inherit = Indexer,
  public = list(
    #' @field dim_indexers List of per-dimension indexers (IntDimIndexer or SliceDimIndexer).
    #' @keywords internal
    dim_indexers = NULL,
    #' @description
    #' Create a new BasicIndexer instance.
    #' @param selection Selection as with ZarrArray: scalar, string, or Slice.
    #' @param array [ZarrArray] object that will be indexed.
    #' @return A `BasicIndexer` instance.
    initialize = function(selection, array) {

      shape <- array$get_shape()
      chunks <- array$get_chunks()

      selection <- normalize_list_selection(selection, shape)
      
      # Setup per-dimension indexers
      dim_indexers <- vector("list", length(selection))
      for(i in seq_along(selection)) {
        dim_sel <- selection[[i]]
        dim_len <- shape[i]
        dim_chunk_len <- chunks[i]

        if(is.null(dim_sel)) {
          dim_sel <- zb_slice(NA)
        }

        if(is_integer(dim_sel)) {
          dim_indexers[[i]] <- IntDimIndexer$new(dim_sel, dim_len, dim_chunk_len)
        } else if(is_slice(dim_sel)) {
          dim_indexers[[i]] <- SliceDimIndexer$new(dim_sel, dim_len, dim_chunk_len)
        } else {
          stop('Unsupported selection item for basic indexing, expected integer or slice')
        }
      }
      self$shape <- list()
      for(d in dim_indexers) {
        if(inherits(d, "SliceDimIndexer")) {
          self$shape <- append(self$shape, d$num_items)
        }
      }
      self$drop_axes <- NA

      self$dim_indexers <- dim_indexers
    },
    #' @description 
    #'   An iterator over the dimensions of an array
    #' @return A list of ChunkProjection objects
    iter = function() {
      # TODO: use generator/yield features from async package
      result <- list()

      # dim_indexers is a list of DimIndexer objects.
      # dim_indexer_iterables is a list (one per dimension)
      # of lists of IntDimIndexer or SliceDimIndexer objects.
      dim_indexer_iterables <- lapply(self$dim_indexers, function(di) di$iter())
      dim_indexer_product <- get_list_product(dim_indexer_iterables)


      for(row_i in seq_len(length(dim_indexer_product))) {
        dim_proj <- dim_indexer_product[[row_i]]

        chunk_coords <- list()
        chunk_sel <- list()
        out_sel <- list()

        if(!is.list(dim_proj)) {
          dim_proj <- list(dim_proj)
        }

        for(p in dim_proj) {
          chunk_coords <- append(chunk_coords, p$dim_chunk_index)
          chunk_sel <- append(chunk_sel, p$dim_chunk_sel)
          if(!is_na(p$dim_out_sel)) {
            out_sel <- append(out_sel, p$dim_out_sel)
          }
        }

        result <- append(result, ChunkProjection$new(
          chunk_coords,
          chunk_sel,
          out_sel
        ))
      }

      return(result)
    }
  )
)

# Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0e6cdc04c6413e14f57f61d389972ea937c/zarr/indexing.py#L585

#' The Zarr OrthogonalIndexer class.
#' @title OrthogonalIndexer Class
#' @docType class
#' @description
#'  An indexer class to normalize a selection of an array and provide an iterator 
#'  of indexes over the dimensions of an array.
#' @rdname OrthogonalIndexer
#' @keywords internal
OrthogonalIndexer <- R6::R6Class("OrthogonalIndexer",
                            inherit = Indexer,
                            public = list(
                              #' @field dim_indexers List of per-dimension indexers (IntDimIndexer, SliceDimIndexer, IntArrayDimIndexer, or BoolArrayDimIndexer).
                              #' @keywords internal
                              dim_indexers = NULL,
                              #' @description
                              #' Create a new OrthogonalIndexer instance.
                              #' @param selection Selection as with ZarrArray: scalar, string, or Slice.
                              #' @param array [ZarrArray] object that will be indexed.
                              #' @return An `OrthogonalIndexer` instance.
                              initialize = function(selection, array) {
                                
                                shape <- array$get_shape()
                                chunks <- array$get_chunks()
                                
                                # Normalize
                                selection <- normalize_list_selection(selection, shape)
                                
                                # Setup per-dimension indexers
                                dim_indexers <- vector("list", length(selection))
                                for(i in seq_along(selection)) {
                                  dim_sel <- selection[[i]]
                                  dim_len <- shape[i]
                                  dim_chunk_len <- chunks[i]

                                  if(is.null(dim_sel)) {
                                    dim_sel <- zb_slice(NA)
                                  }

                                  if(is_integer(dim_sel)) {
                                    dim_indexers[[i]] <- IntDimIndexer$new(dim_sel, dim_len, dim_chunk_len)
                                  } else if(is_slice(dim_sel)) {
                                    if(!is.na(dim_sel$step) && dim_sel$step < 0) {
                                      # Convert negative-step slice to integer vector
                                      si <- dim_sel$indices(dim_len)
                                      n <- si[4]
                                      if(n > 0) {
                                        dim_sel <- seq(from = si[1], by = si[3], length.out = n)
                                        dim_indexers[[i]] <- IntArrayDimIndexer$new(dim_sel, dim_len, dim_chunk_len)
                                      } else {
                                        dim_indexers[[i]] <- IntArrayDimIndexer$new(integer(0), dim_len, dim_chunk_len)
                                      }
                                    } else {
                                      dim_indexers[[i]] <- SliceDimIndexer$new(dim_sel, dim_len, dim_chunk_len)
                                    }
                                  } else if(is_bool_vec(dim_sel)) {
                                    dim_indexers[[i]] <- BoolArrayDimIndexer$new(dim_sel, dim_len, dim_chunk_len)
                                  } else if(is_integer_vec(dim_sel)) {
                                    dim_indexers[[i]] <- IntArrayDimIndexer$new(dim_sel, dim_len, dim_chunk_len)
                                  } else {
                                    stop('Unsupported selection item for basic indexing, expected integer, slice, vector of integer or boolean')
                                  }
                                }
                                self$shape <- list()
                                for(d in dim_indexers) {
                                  if(!inherits(d, "IntDimIndexer")) {
                                    self$shape <- append(self$shape, d$num_items)
                                  }
                                }
                                self$drop_axes <- NA
                                
                                self$dim_indexers <- dim_indexers
                                
                              },
                              #' @description 
                              #'   An iterator over the dimensions of an array
                              #' @return A list of ChunkProjection objects
                              iter = function() {
                                
                                # TODO: use generator/yield features from async package
                                result <- list()
                                
                                # dim_indexers is a list of DimIndexer objects.
                                # dim_indexer_iterables is a list (one per dimension)
                                # of lists of IntDimIndexer or SliceDimIndexer objects.
                                dim_indexer_iterables <- lapply(self$dim_indexers, function(di) di$iter())
                                dim_indexer_product <- get_list_product(dim_indexer_iterables)
                                
                                
                                for(row_i in seq_len(length(dim_indexer_product))) {
                                  dim_proj <- dim_indexer_product[[row_i]]
                                  
                                  chunk_coords <- list()
                                  chunk_sel <- list()
                                  out_sel <- list()
                                  
                                  if(!is.list(dim_proj)) {
                                    dim_proj <- list(dim_proj)
                                  }
                                  
                                  for(p in dim_proj) {
                                    chunk_coords <- append(chunk_coords, p$dim_chunk_index)
                                    # chunk_sel <- append(chunk_sel, p$dim_chunk_sel)
                                    chunk_sel <- append(chunk_sel, list(p$dim_chunk_sel))
                                    if(!is_na(p$dim_out_sel)) {
                                      # out_sel <- append(out_sel, p$dim_out_sel)
                                      out_sel <- append(out_sel, list(p$dim_out_sel))
                                    }
                                  }
                                  
                                  result <- append(result, ChunkProjection$new(
                                    chunk_coords,
                                    chunk_sel,
                                    out_sel
                                  ))
                                }
                                
                                return(result)
                              }
                            )
)

# Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0e6cdc04c6413e14f57f61d389972ea937c/zarr/indexing.py#L424

#' The Order class.
#' @title Order Class
#' @docType class
#' @description
#'  Determines the sort order of an integer array selection (increasing, decreasing, or unordered).
#' @rdname Order
#' @keywords internal
Order <- R6::R6Class("Order",
                     public = list(
                       #' @field UNKNOWN UNKNOWN
                       #' @keywords internal
                       UNKNOWN = 0,
                       #' @field INCREASING INCREASING
                       #' @keywords internal
                       INCREASING = 1,
                       #' @field DECREASING DECREASING
                       #' @keywords internal
                       DECREASING = 2,
                       #' @field UNORDERED UNORDERED
                       #' @keywords internal
                       UNORDERED = 3,
                       #' @description
                       #' checking order of numbers
                       #' @param a Vector of numbers.
                       check = function(a){
                         diff_a <- diff(a)
                         diff_positive <- diff_a >= 0
                         n_diff_positive <- sum(diff_positive)
                         all_increasing <- n_diff_positive == length(diff_positive)
                         any_increasing <- n_diff_positive > 0
                         if(all_increasing){
                           return(Order$public_fields$INCREASING)
                         } else if(any_increasing) {
                           return(Order$public_fields$UNORDERED)
                         } else{
                           return(Order$public_fields$DECREASING)
                         }
                       })
                     )

# Reference: https://github.com/zarr-developers/zarr-python/blob/5dd4a0e6cdc04c6413e14f57f61d389972ea937c/zarr/indexing.py#L457

#' The Zarr IntArrayDimIndexer class.
#' @title IntArrayDimIndexer Class
#' @docType class
#' @description
#'  Indexer for an integer array selection along one dimension, supporting fancy/advanced indexing.
#' @rdname IntArrayDimIndexer
#' @keywords internal
IntArrayDimIndexer <- R6::R6Class("IntArrayDimIndexer",
                               inherit = DimIndexer,
                               public = list(
                                 #' @field dim_len Dimension length.
                                 #' @keywords internal
                                 dim_len = NULL,
                                 #' @field dim_chunk_len Dimension chunk length.
                                 #' @keywords internal
                                 dim_chunk_len = NULL,
                                 #' @field num_chunks Number of chunks.
                                 #' @keywords internal
                                 num_chunks = NULL,
                                 #' @field dim_sel Selection on dimension.
                                 #' @keywords internal
                                 dim_sel = NULL,
                                 #' @field dim_out_sel Output array indices corresponding to each selected element, reordered when selection is not increasing.
                                 #' @keywords internal
                                 dim_out_sel = NULL,
                                 #' @field order Memory order.
                                 #' @keywords internal
                                 order = NULL,
                                 #' @field chunk_nitems Number of items per chunk.
                                 #' @keywords internal
                                 chunk_nitems = NULL, 
                                 #' @field dim_chunk_ixs Chunks that should be visited.
                                 #' @keywords internal
                                 dim_chunk_ixs = NULL,
                                 #' @field chunk_nitems_cumsum Offsets into the output array.
                                 #' @keywords internal
                                 chunk_nitems_cumsum = NULL, 
                                 #' @description
                                 #' Create a new IntArrayDimIndexer instance.
                                 #' @param dim_sel Integer dimension selection.
                                 #' @param dim_len Integer dimension length.
                                 #' @param dim_chunk_len Integer dimension chunk length.
                                 #' @param sel_order order
                                 #' @return A `IntArrayDimIndexer` instance.
                                 initialize = function(dim_sel, dim_len, dim_chunk_len, sel_order = Order$public_fields$UNKNOWN) {

                                   # Normalize
                                   dim_sel <- sapply(dim_sel, normalize_integer_selection, dim_len = dim_len)
                                   self$dim_sel <- dim_sel
                                   
                                   # store attributes 
                                   self$dim_len <- dim_len
                                   self$dim_chunk_len <- dim_chunk_len
                                   self$num_items <- length(dim_sel)
                                   self$num_chunks <- ceiling(self$dim_len / self$dim_chunk_len)
                                   
                                   # dim_sel_chunk <- ceiling(dim_sel / dim_chunk_len) # pre zb_int() implementation
                                   dim_sel_chunk <- floor(dim_sel / dim_chunk_len)
                                   
                                   # determine order of indices
                                   if(sel_order == Order$public_fields$UNKNOWN)
                                     sel_order <- Order$public_methods$check(dim_sel)
                                   self$order <- sel_order
                                   
                                   if(self$order == Order$public_fields$INCREASING){
                                     self$dim_sel <-  dim_sel
                                   } else if(self$order == Order$public_fields$DECREASING) {
                                     self$dim_sel = rev(dim_sel)
                                     self$dim_out_sel = rev(seq(1,self$num_items))
                                     # self$dim_out_sel = rev(seq(0,self$num_items-1)) # Python based indexing
                                   } else {
                                     # sort indices to group by chunk
                                     self$dim_out_sel = order(dim_sel_chunk)
                                     self$dim_sel <- dim_sel[self$dim_out_sel]
                                     # self$dim_out_sel <- self$dim_out_sel - 1 # Python based indexing
                                   }
                                   
                                   # precompute number of selected items for each chunk
                                   # self$chunk_nitems <- tabulate(dim_sel_chunk, nbins = self$num_chunks) # pre zb_int() implementation
                                   self$chunk_nitems <- tabulate(dim_sel_chunk + 1, nbins = self$num_chunks)
                                   
                                   # find chunks that we need to visit
                                   self$dim_chunk_ixs = which(self$chunk_nitems != 0)
                                   
                                   # compute offsets into the output array
                                   self$chunk_nitems_cumsum = cumsum(self$chunk_nitems)
                                   
                                 },
                                 #' @description 
                                 #'   An iterator over the dimensions of an array
                                 #' @return A list of ChunkProjection objects
                                 iter = function() {
                                   
                                   # Iterate over chunks in range
                                   result <- list()
                                   for(dim_chunk_ix in self$dim_chunk_ixs) {
                                     
                                     # find region in output
                                     # if (dim_chunk_ix == 0) {
                                     if (dim_chunk_ix == 1) { 
                                       start <- 0
                                     } else {
                                       start <- self$chunk_nitems_cumsum[dim_chunk_ix - 1]
                                     }
                                     stop <- self$chunk_nitems_cumsum[dim_chunk_ix]
                                     
                                     # START R-SPECIFIC
                                     if(start == stop) {
                                       stop <- stop + 1
                                     }
                                     # END R-SPECIFIC
                                     
                                     if (self$order == Order$public_fields$INCREASING) {
                                       dim_out_sel <- seq(start, stop - 1)
                                     } else {
                                       dim_out_sel <- self$dim_out_sel[(start + 1):stop]
                                       # START R-SPECIFIC
                                       dim_out_sel <- dim_out_sel - 1
                                       # END R-SPECIFIC
                                     }

                                     # START R-SPECIFIC
                                     dim_chunk_ix <- dim_chunk_ix - 1
                                     # END R-SPECIFIC
                                     
                                     # find region in chunk
                                     dim_offset <- dim_chunk_ix * self$dim_chunk_len
                                     # dim_chunk_sel <- self$dim_sel[(start + 1):stop] - dim_offset - 1 # pre zb_int implementation()
                                     dim_chunk_sel <- self$dim_sel[(start + 1):stop] - dim_offset

                                     # # START R-SPECIFIC
                                     # dim_chunk_ix <- dim_chunk_ix - 1
                                     # # END R-SPECIFIC

                                     result <- append(result, ChunkDimProjection$new(
                                       dim_chunk_ix,
                                       dim_chunk_sel,
                                       dim_out_sel
                                     ))
                                   }
                                   return(result)
                                 }
                               )
)

# Reference: https://github.com/zarr-developers/zarr-python/blob/4a3bbf1cbb89e90ea9ca4d6d75dae23ed4b957c9/src/zarr/core/indexing.py#L581
#' The Zarr BoolArrayDimIndexer class.
#' @title BoolArrayDimIndexer Class
#' @docType class
#' @description
#'  Indexer for a boolean array selection along one dimension, selecting elements where the mask is TRUE.
#' @rdname BoolArrayDimIndexer
#' @keywords internal
BoolArrayDimIndexer <- R6::R6Class("BoolArrayDimIndexer",
                                   inherit = DimIndexer,
                                   public = list(
                                     #' @field dim_sel Selection on dimension.
                                     #' @keywords internal
                                     dim_sel = NULL,
                                     #' @field dim_len Dimension length.
                                     #' @keywords internal
                                     dim_len = NULL,
                                     #' @field dim_chunk_len Dimension chunk length.
                                     #' @keywords internal
                                     dim_chunk_len = NULL,
                                     #' @field num_chunks Number of chunks.
                                     #' @keywords internal
                                     num_chunks = NULL,
                                     #' @field chunk_nitems Number of items per chunk.
                                     #' @keywords internal
                                     chunk_nitems = NULL, 
                                     #' @field chunk_nitems_cumsum Offsets into the output array.
                                     #' @keywords internal
                                     chunk_nitems_cumsum = NULL, 
                                     #' @field dim_chunk_ixs Chunks that should be visited.
                                     #' @keywords internal
                                     dim_chunk_ixs = NULL,
                                     #' @field dim_out_sel Output array indices for the selected elements.
                                     #' @keywords internal
                                     dim_out_sel = NULL,
                                     #' @description
                                     #' Create a new BoolArrayDimIndexer instance.
                                     #' @param dim_sel Integer dimension selection.
                                     #' @param dim_len Integer dimension length.
                                     #' @param dim_chunk_len Integer dimension chunk length.
                                     #' @return A `BoolArrayDimIndexer` instance.
                                     initialize = function(dim_sel, dim_len, dim_chunk_len) {
                                       
                                       # check selection length
                                       if(length(dim_sel) != dim_len)
                                         stop(paste0("IndexError: Boolean vector has the wrong length for dimension; expected ", dim_len, ", got ", length(dim_sel)))
                                       
                                       # precompute number of selected items for each chunk
                                       num_chunks <- ceiling(dim_len / dim_chunk_len)
                                       chunk_nitems <- rep(0, num_chunks)
                                       for(dim_chunk_ix in 1:num_chunks){
                                         dim_offset <- ((dim_chunk_ix - 1) * dim_chunk_len) + 1
                                         # START R-SPECIFIC
                                         dim_offset_limits <- dim_offset+dim_chunk_len-1
                                         dim_offset_limits <- ifelse(dim_offset_limits > length(dim_sel), length(dim_sel), dim_offset_limits)
                                         # STOP R-SPECIFIC
                                         
                                         chunk_nitems[dim_chunk_ix] <- sum(dim_sel[dim_offset:dim_offset_limits] != 0)
                                       }
                                       
                                       # compute offsets into the output array
                                       chunk_nitems_cumsum <- cumsum(chunk_nitems)
                                       num_items <- rev(chunk_nitems_cumsum)[1]
                                       
                                       # find chunks that we need to visit
                                       dim_chunk_ixs <- which(chunk_nitems != 0)
                                       
                                       # store attributes
                                       self$dim_sel <- dim_sel
                                       self$dim_len <- dim_len
                                       self$dim_chunk_len <- dim_chunk_len
                                       self$num_chunks <- num_chunks
                                       self$chunk_nitems <- chunk_nitems
                                       self$chunk_nitems_cumsum <- chunk_nitems_cumsum
                                       self$num_items <- num_items
                                       self$dim_chunk_ixs <- dim_chunk_ixs
                                     },
                                     #' @description 
                                     #'   An iterator over the dimensions of an array
                                     #' @return A list of ChunkProjection objects
                                     iter = function() {
                                       
                                       # Iterate over chunks in range
                                       result <- list()
                                       for(dim_chunk_ix in self$dim_chunk_ixs) {
                                         
                                         # find region in chunk
                                         dim_offset <- ((dim_chunk_ix - 1) * self$dim_chunk_len) + 1
                                         dim_chunk_sel <- self$dim_sel[dim_offset:(dim_offset+self$dim_chunk_len-1)]
                                         
                                         # pad out if final chunk
                                         if(length(dim_chunk_sel) < length(self$dim_chunk_len)){
                                           tmp <- rep(FALSE, self$dim_chunk_len)
                                           tmp[1:length(dim_chunk_sel)] <- dim_chunk_sel
                                           dim_chunk_sel <- tmp
                                         }
                                         
                                         # find region in output
                                         if (dim_chunk_ix == 1) { 
                                           start <- 0
                                         } else {
                                           start <- self$chunk_nitems_cumsum[dim_chunk_ix - 1]
                                         }
                                         stop <- self$chunk_nitems_cumsum[dim_chunk_ix]
                                         
                                         # START R-SPECIFIC
                                         if(start == stop) {
                                           stop <- stop + 1
                                         }
                                         # END R-SPECIFIC
                                         
                                         # get out selection
                                         dim_out_sel <- seq(start, stop - 1)
                                         
                                         # make boolean as integer, specific to pizzarr
                                         dim_chunk_sel <- which(dim_chunk_sel) - 1
                                         
                                         # START R-SPECIFIC
                                         dim_chunk_ix <- dim_chunk_ix - 1
                                         # END R-SPECIFIC
                                         
                                         result <- append(result, ChunkDimProjection$new(
                                           dim_chunk_ix,
                                           dim_chunk_sel,
                                           dim_out_sel
                                         ))
                                         
                                       }
                                       
                                       return(result)
                                     }
                                   )
)
