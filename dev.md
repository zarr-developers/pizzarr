# Phase 2, Iteration 2: `zarrs_retrieve_subset` + R-side dispatch

## Previous iterations (complete)

- **Phase 1:** extendr scaffolding, store cache, `zarrs_node_exists`,
  `zarrs_close_store`. 3 Rust functions, ~600 LOC Rust.
- **Phase 2, Iteration 1:** `zarrs_open_array_metadata`, `zarrs_runtime_info`,
  `zarrs_set_codec_concurrent_target`. `array_open.rs`, `dtype_dispatch.rs`,
  `metadata.rs`, `info.rs`. 6 total `#[extendr]` functions.

---

## Goal

Implement the Rust-side `zarrs_retrieve_subset()` function and the R-side
dispatch wiring so that `ZarrArray$get_item()` can read array data through
zarrs when the backend is available. This is the critical performance path —
zarrs handles chunk identification, parallel decode, and codec execution
internally, bypassing pizzarr's R-native chunk loop entirely.

Scope is deliberately limited: contiguous slice selections only (no fancy
indexing, no negative steps, no coordinate selection). Those fall through to
the R-native path.

---

## Changes

### New Rust module

**`src/rust/src/retrieve.rs`** — `retrieve_subset()` implementation.

```rust
pub(crate) fn retrieve_subset(
    store_url: &str,
    array_path: &str,
    ranges: List,          // R list of length-2 integer vectors: c(start, stop)
    concurrent_target: Nullable<i32>,
) -> extendr_api::Result<List>
```

Steps:
1. Open array via `array_open::open_array_at_path()` (reuses cached store).
2. Convert `ranges` (R list of `c(start, stop)` integer pairs) into
   `Vec<Range<u64>>`. Ranges are 0-based, exclusive stop — matching zarrs
   convention. The R side handles the conversion from pizzarr's selection
   objects.
3. Build `ArraySubset` via `ArraySubset::new_with_ranges(&ranges_vec)`.
4. Classify dtype via `dtype_dispatch::dtype_family()`.
5. Dispatch on `RTypeFamily` — each arm calls the appropriate
   `retrieve_array_subset_elements::<T>(&subset)` (or `_opt` variant if
   `concurrent_target` is provided), then widens to R-compatible type:

   ```
   Double          → retrieve::<f64>()   → return as REALSXP
   Float32AsDouble → retrieve::<f32>()   → widen to Vec<f64>
   Integer         → retrieve::<i32>()   → return as INTSXP
   Int16AsInteger  → retrieve::<i16>()   → widen to Vec<i32>
   Int8AsInteger   → retrieve::<i8>()    → widen to Vec<i32>
   Uint8AsInteger  → retrieve::<u8>()    → widen to Vec<i32>
   Uint16AsInteger → retrieve::<u16>()   → widen to Vec<i32>
   Uint32AsDouble  → retrieve::<u32>()   → widen to Vec<f64>
   Int64AsDouble   → retrieve::<i64>()   → widen to Vec<f64>
   Uint64AsDouble  → retrieve::<u64>()   → widen to Vec<f64>
   Logical         → retrieve::<bool>()  → convert to R logical
   None            → error: unsupported dtype, fall back to R-native
   ```

6. Compute output shape from ranges (each dim: `stop - start`).
7. Return `list!(data = ..., shape = shape_vec)`.

**`CodecOptions` handling:** If `concurrent_target` is `Some(n)`, construct
`CodecOptions::default().with_concurrent_target(n as usize)` and use
`retrieve_array_subset_elements_opt`. Otherwise use the non-`_opt` variant
(inherits global config).

**Macro for dispatch:** The 11-arm match is repetitive. Use a macro to reduce
boilerplate:

```rust
macro_rules! retrieve_as_double {
    ($array:expr, $subset:expr, $opts:expr, $t:ty) => {{
        let raw: Vec<$t> = retrieve_with_opts($array, $subset, $opts)?;
        raw.into_iter().map(|v| v as f64).collect::<Vec<f64>>()
    }};
}
```

### Modified Rust files

**`src/rust/src/lib.rs`** — Add `mod retrieve;`, one new `#[extendr]` wrapper:

```rust
#[extendr]
fn zarrs_retrieve_subset(
    store_url: &str,
    array_path: &str,
    ranges: List,
    concurrent_target: Nullable<i32>,
) -> extendr_api::Result<List> {
    retrieve::retrieve_subset(store_url, array_path, ranges, concurrent_target)
}
```

Register in `extendr_module!` (7 total functions).

**`src/rust/src/dtype_dispatch.rs`** — No changes to the enum. May add a
`retrieve_elements` helper method on `RTypeFamily` if it reduces boilerplate,
but defer that decision to implementation time.

**`src/rust/src/error.rs`** — Add `PizzarrError::Retrieve` variant:

```rust
#[error("Retrieve({path} in {url}): {reason}")]
Retrieve {
    url: String,
    path: String,
    reason: String,
},
```

### New R file

**`R/zarrs-dispatch.R`** — R-side dispatch functions:

```r
# Convert pizzarr selection (from indexer) to zarrs ranges.
# Returns a list of length-2 integer vectors: list(c(start, stop), ...)
# 0-based, exclusive stop.
selection_to_ranges <- function(indexer) {
  # BasicIndexer/OrthogonalIndexer store per-dimension SliceDimIndexer
  # objects. Each has $start, $stop fields (0-based).
  # For simple contiguous slices, these map directly to zarrs ranges.
  dim_indexers <- indexer$dim_indexers
  lapply(dim_indexers, function(di) {
    as.integer(c(di$start, di$stop))
  })
}

# Check whether the zarrs path can handle this selection.
# Returns TRUE only for contiguous slice selections (no fancy indexing,
# no integer array indexers, no negative steps).
can_use_zarrs <- function(indexer, store) {
  if (!.pizzarr_env$zarrs_available) return(FALSE)
  store_id <- store$get_store_identifier()
  if (is.null(store_id)) return(FALSE)  # MemoryStore

  # Only BasicIndexer with SliceDimIndexer on every dimension
  if (!inherits(indexer, "BasicIndexer")) return(FALSE)
  all(vapply(indexer$dim_indexers, function(di) {
    inherits(di, "SliceDimIndexer")
  }, logical(1)))
}
```

### Modified R file

**`R/zarr-array.R`** — Insert zarrs dispatch in `get_selection()` (line ~504,
after `parts <- indexer$iter()`, before `ps$apply_func()`):

```r
get_selection = function(indexer, out = NA, fields = NA) {
  out_dtype <- private$dtype
  out_shape <- indexer$shape
  out_size <- compute_size(indexer$shape)

  if (!is.na(out)) {
    # TODO: handle out provided as parameter
  } else {
    out <- NestedArray$new(NULL, shape = out_shape, dtype = out_dtype,
                           order = private$order)
  }

  if (out_size == 0) return(out)

  # --- zarrs fast path ---
  if (can_use_zarrs(indexer, private$store)) {
    ranges <- selection_to_ranges(indexer)
    store_id <- private$store$get_store_identifier()
    ct <- getOption("pizzarr.concurrent_target", NULL)
    result <- zarrs_retrieve_subset(store_id, private$path, ranges, ct)
    out$data <- result$data
    return(out)
  }

  # --- R-native path (unchanged) ---
  ps <- get_parallel_settings(...)
  ...
}
```

**Key design decisions:**

1. **Dispatch before chunk iteration.** zarrs handles chunk identification
   internally — the R side never computes `ChunkProjection` objects on the
   zarrs path. `selection_to_ranges()` reads directly from the indexer's
   per-dimension start/stop, not from `ChunkProjection`.

2. **Output injection.** zarrs returns flat data + shape. The R side sets
   `out$data` directly on the pre-allocated `NestedArray`. No intermediate
   array construction.

3. **Conservative eligibility.** `can_use_zarrs()` returns `TRUE` only for
   `BasicIndexer` with all `SliceDimIndexer` dims. `IntDimIndexer` (scalar
   selection), `IntArrayDimIndexer` (fancy indexing), `OrthogonalIndexer`,
   and negative steps all fall through to R-native. Broadening eligibility
   is a follow-up.

4. **No `Store$get_store_identifier()` changes needed.** Already exists on
   `DirectoryStore` (returns root path), `HttpStore` (returns URL), and
   `MemoryStore` (returns NULL).

### New test file

**`tests/testthat/test-zarrs-retrieve.R`** — Tests (all
`skip_if(!.pizzarr_env$zarrs_available)`):

1. **1D contiguous read:** Create V2 array, write data, read full array via
   zarrs, compare with R-native read.
2. **2D slice read:** Create V2 array, read a sub-slice, compare.
3. **V3 read:** Create V3 array, read via zarrs, compare.
4. **Multiple dtypes:** float64, int32, int16 (widened to integer), uint8.
5. **Fill value handling:** Create array, do not write data, read — should
   get fill values.
6. **zarrs matches R-native:** Side-by-side comparison of zarrs and R-native
   reads for a representative array.
7. **Error on unsupported dtype:** String array — zarrs returns error, R
   falls back.
8. **concurrent_target option:** Read with
   `options(pizzarr.concurrent_target = 1L)`, verify result matches.

### Modified documentation

**`vignettes/zarrs-backend.Rmd`** — Fill in the "Reading data via zarrs"
section with a worked example (create array, write data, read subset via
zarrs, show timing comparison).

**`R/extendr-wrappers.R`** — Auto-regenerated (7 `.Call` wrappers).

---

## API details to verify

- `Array::retrieve_array_subset_elements::<T>(&subset)` — returns
  `Result<Vec<T>, ArrayError>` where `T: ElementOwned`.
- `Array::retrieve_array_subset_elements_opt::<T>(&subset, &opts)` — same
  with `CodecOptions`.
- `ArraySubset::new_with_ranges(&[Range<u64>])` — constructor from ranges.
- `&[Range<u64>]` implements `ArraySubsetTraits` — can pass directly to
  retrieve methods without constructing `ArraySubset`.
- `CodecOptions::default().with_concurrent_target(n)` — builder pattern.

All confirmed from zarrs 0.23 docs and mdsumner/zr prior art.

---

## Done criteria

1. `rextendr::document()` succeeds (Rust compiles, wrappers regenerated)
2. `zarrs_retrieve_subset()` reads V2 and V3 arrays with gzip/zstd/raw codecs
3. `ZarrArray$get_item()` dispatches to zarrs for eligible selections
4. zarrs and R-native paths produce identical results for all test cases
5. Unsupported selections (fancy indexing, MemoryStore, string dtype) fall
   through to R-native without error
6. `options(pizzarr.concurrent_target = N)` propagates to zarrs
7. All zarrs tests pass (old + new)
8. All existing tests pass (no regressions)
9. Outcomes of this work are recorded as complete in TODO.md and lessons learned recorded in RUST-STYLE.md.

---

## Next iteration: Phase 2, Iteration 3 (or Phase 3)

Broaden zarrs eligibility: `IntDimIndexer` (scalar selection, squeeze dims),
`OrthogonalIndexer` with simple slices. Then move to Phase 3: write path
(`zarrs_store_subset`, `zarrs_create_array`).
