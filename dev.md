# Phase 2, Iteration 3: write path + broader read eligibility

## Previous iterations (complete)

- **Phase 1:** extendr scaffolding, store cache, `zarrs_node_exists`,
  `zarrs_close_store`. 3 Rust functions, ~600 LOC Rust.
- **Phase 2, Iteration 1:** `zarrs_open_array_metadata`, `zarrs_runtime_info`,
  `zarrs_set_codec_concurrent_target`. `array_open.rs`, `dtype_dispatch.rs`,
  `metadata.rs`, `info.rs`. 6 total `#[extendr]` functions.
- **Phase 2, Iteration 2:** `zarrs_retrieve_subset` + R-side dispatch.
  `retrieve.rs`, `R/zarrs-dispatch.R`, zarrs fast path in `get_selection()`.
  7 total `#[extendr]` functions. 9 Rust source files, 22 new tests.

---

## Phase 2 Iteration 2 — completion notes

### What shipped

1. **`src/rust/src/retrieve.rs`** — hot read path. Parses R list of `c(start, stop)`
   ranges into `Vec<Range<u64>>`, builds `ArraySubset`, dispatches on `RTypeFamily`
   (11 arms), returns `list(data, shape)`. Uses local `macro_rules!` for
   `retrieve_as_double!` and `retrieve_as_integer!` to reduce boilerplate.

2. **`src/rust/src/lib.rs`** — `mod retrieve;` + `#[extendr] fn zarrs_retrieve_subset()`
   wrapper. 7 functions in `extendr_module!`.

3. **`src/rust/Cargo.toml`** — added `zarrs_codec = "0.2"` (needed for `CodecOptions`).

4. **`R/zarrs-dispatch.R`** — `can_use_zarrs(indexer, store)` and
   `selection_to_ranges(indexer)`.

5. **`R/zarr-array.R`** — zarrs fast path inserted in `get_selection()` (line ~501),
   before the R-native chunk loop. Uses `tryCatch` for silent fallback.

6. **`tests/testthat/test-zarrs-retrieve.R`** — 22 tests: raw Rust function (7),
   R dispatch (4), end-to-end via `get_item()` (2), plus dtype/fill/concurrency tests.

7. **`vignettes/zarrs-backend.Rmd`** — "Reading data via zarrs" section with
   basic read, direct `zarrs_retrieve_subset` call, and concurrency control examples.

8. **`tools/config.R`** — fixed `.clean_targets` to only delete cargo target dir
   for CRAN builds (preserves incremental compilation cache during development).

### Deviations from plan

- **`CodecOptions` import path.** Plan assumed `zarrs::array::codec::CodecOptions`.
  Actual location: `zarrs_codec::CodecOptions`. Required adding `zarrs_codec = "0.2"`
  to Cargo.toml.

- **Retrieve API change.** Plan assumed `retrieve_array_subset_elements::<T>()`.
  This is deprecated in zarrs 0.23. Actual API:
  `retrieve_array_subset_opt::<Vec<T>>(subset, opts)` where the generic param is the
  output container and `Vec<T>: FromArrayBytes` when `T: ElementOwned`.

- **Trait bound.** Plan assumed `T: Element + Send + Sync`. Correct bound is
  `T: ElementOwned` (`ElementOwned: Element` is a supertrait).

- **C-order to F-order conversion.** Plan's R-side dispatch set `out$data` directly
  from zarrs flat data. This broke nD arrays because zarrs returns C-order (row-major)
  but R uses F-order (column-major). Fix: reshape with reversed dims then `aperm()`.

- **`tryCatch` fallback.** Plan did not include error handling around the zarrs call.
  Added `tryCatch(..., error = function(e) NULL)` so unsupported codecs or other zarrs
  errors fall through silently to R-native.

- **Test expectations for `selection_to_ranges`.** Plan expected `slice(2L, 8L)` to
  produce `c(2, 8)`. Actual: `c(1, 8)` because `SliceDimIndexer` stores 0-based start
  (pizzarr's `slice()` takes 1-based args, indexer subtracts 1).

### Lessons learned (recorded in RUST-STYLE.md)

1. `zarrs_codec` is a separate crate from `zarrs` — `CodecOptions` lives there.
2. `ElementOwned` is the correct trait bound for retrieve generics.
3. `retrieve_array_subset_opt::<Vec<T>>()` replaces the deprecated `_elements` API.
4. C-order to F-order conversion: `array(data, dim=rev(shape))` then `aperm(arr, rev(seq))`.
5. `tryCatch` fallback is essential for the dispatch pattern.
6. Development builds should not clean `$(TARGET_DIR)` — only CRAN builds need it.

---

## Goal (Phase 2 Iteration 3 / Phase 3)

Two objectives for the next iteration:

### A. Broaden zarrs read eligibility

`can_use_zarrs()` currently requires all dimensions to be `SliceDimIndexer`.
Scalar indexing (`m[1, ]`) produces an `IntDimIndexer`, which falls through
to R-native. This is the most common mixed selection pattern.

**Scope:** Extend `can_use_zarrs()` and `selection_to_ranges()` to handle
`IntDimIndexer` dims (scalar selection becomes a length-1 range). The zarrs side
already handles this (a range of `start..start+1` retrieves a single slice),
so changes are R-only.

### B. Write path (`zarrs_store_subset`)

Implement the Rust-side `zarrs_store_subset()` function and wire it into
`ZarrArray$set_item()`. Symmetric with the read path: R-side dispatch converts
selection to ranges, Rust side opens the array, dispatches on dtype (narrow
from R type to stored type), calls `store_array_subset`.

---

## Changes (A): broader read eligibility

### Modified R file

**`R/zarrs-dispatch.R`** — Extend both functions:

```r
can_use_zarrs <- function(indexer, store) {
  if (!.pizzarr_env$zarrs_available) return(FALSE)
  store_id <- store$get_store_identifier()
  if (is.null(store_id)) return(FALSE)
  if (!inherits(indexer, "BasicIndexer")) return(FALSE)
  # Allow SliceDimIndexer and IntDimIndexer (scalar selection)
  all(vapply(indexer$dim_indexers, function(di) {
    inherits(di, "SliceDimIndexer") || inherits(di, "IntDimIndexer")
  }, logical(1)))
}

selection_to_ranges <- function(indexer) {
  lapply(indexer$dim_indexers, function(di) {
    if (inherits(di, "IntDimIndexer")) {
      # Scalar selection: single-element range
      as.integer(c(di$dim_sel, di$dim_sel + 1L))
    } else {
      as.integer(c(di$start, di$stop))
    }
  })
}
```

**`R/zarr-array.R`** — The fast path already handles shape correctly via
`indexer$shape` (which drops squeezed dims). Verify that `IntDimIndexer`
dims are correctly excluded from `out_shape` by the existing indexer logic.

### New tests

Add to `test-zarrs-retrieve.R`:
- Scalar + slice mixed selection: `z$get_item(list(1L, slice(1L, 5L)))` via zarrs
- All-scalar selection: `z$get_item(list(1L, 1L))` via zarrs
- Compare zarrs vs R-native for scalar selections

---

## Changes (B): write path

### New Rust module

**`src/rust/src/store_subset.rs`** — `store_subset()` implementation.

```rust
pub(crate) fn store_subset(
    store_url: &str,
    array_path: &str,
    ranges: List,
    data: Robj,
    concurrent_target: Nullable<i32>,
) -> extendr_api::Result<()>
```

Steps:
1. Open array via `array_open::open_array_at_path()`.
2. Convert ranges (same as retrieve).
3. Build `ArraySubset`.
4. Classify dtype via `dtype_dispatch::dtype_family()`.
5. Dispatch on `RTypeFamily` — each arm narrows from R type to stored type:
   - R double to f64 (zero-cost), f32 (narrow with range check), i64/u64/u32 (narrow)
   - R integer to i32 (zero-cost), i16/i8/u8/u16 (narrow with range check)
   - R logical to bool
6. Call `store_array_subset_opt::<Vec<T>>(subset, data_vec, opts)`.

**Range checking:** Narrowing conversions (f64 to f32, i32 to i16, etc.) should
check for overflow and return `PizzarrError::Store` with a descriptive message
rather than silently truncating.

### Modified Rust files

**`src/rust/src/lib.rs`** — Add `mod store_subset;` and `#[extendr]` wrapper:

```rust
#[extendr]
fn zarrs_store_subset(
    store_url: &str,
    array_path: &str,
    ranges: List,
    data: Robj,
    concurrent_target: Nullable<i32>,
) -> extendr_api::Result<()> {
    store_subset::store_subset(store_url, array_path, ranges, data, concurrent_target)
}
```

Register in `extendr_module!` (8 total functions).

### Modified R files

**`R/zarrs-dispatch.R`** — Add `can_use_zarrs_write()`:

```r
can_use_zarrs_write <- function(indexer, store) {
  # Same eligibility as read, plus store must support write
  can_use_zarrs(indexer, store)
}
```

**`R/zarr-array.R`** — Insert zarrs fast path in `set_selection()`, before
the R-native chunk loop:

```r
# --- zarrs write fast path ---
if (can_use_zarrs_write(indexer, private$store)) {
  ranges <- selection_to_ranges(indexer)
  store_id <- private$store$get_store_identifier()
  ct <- getOption("pizzarr.concurrent_target", NULL)
  # Flatten value to C-order for zarrs
  write_data <- if (length(dim(value)) > 1) {
    as.vector(aperm(value, rev(seq_along(dim(value)))))
  } else {
    as.vector(value)
  }
  result <- tryCatch(
    zarrs_store_subset(store_id, private$path, ranges, write_data, ct),
    error = function(e) NULL
  )
  if (!is.null(result)) return(invisible(NULL))
  # zarrs failed — fall through to R-native path
}
```

### New test file

**`tests/testthat/test-zarrs-write.R`**:

1. Round-trip: write via zarrs, read via zarrs, compare
2. Round-trip: write via zarrs, read via R-native, compare
3. Multiple dtypes: float64, int32, int16
4. 2D array write + read
5. Partial subset write (overwrite a slice of existing data)
6. F-order to C-order conversion for nD writes
7. MemoryStore falls through to R-native write
8. concurrent_target option propagated

### Modified documentation

**`vignettes/zarrs-backend.Rmd`** — Fill in "Writing data via zarrs" section.

**`R/extendr-wrappers.R`** — Auto-regenerated (8 `.Call` wrappers).

---

## API details to verify

- `Array::store_array_subset_opt()` — confirm exact signature and generic
  parameter pattern in zarrs 0.23 docs. Likely
  `store_array_subset_opt::<&[T]>(subset, &data, opts)` or similar.
- Trait bound for store: `T: ElementOwned` same as retrieve? Or `Element`?
- F-order to C-order conversion: verify zarrs expects C-order input
  (symmetric with retrieve returning C-order).
- `IntDimIndexer` field names: confirm `$dim_sel` is the 0-based scalar index
  (check `R/indexing.R`).
- `store_array_subset` vs `store_array_subset_elements` — check if the
  `_elements` variant is also deprecated in zarrs 0.23 (similar to retrieve).

---

## Done criteria

1. `rextendr::document()` succeeds
2. `can_use_zarrs()` accepts `IntDimIndexer` dims (scalar selection)
3. `zarrs_store_subset()` writes V2 and V3 arrays
4. Round-trip read-write via zarrs produces correct results
5. zarrs and R-native write paths produce identical stored data
6. Unsupported selections fall through to R-native without error
7. All zarrs tests pass (old + new)
8. All existing tests pass (no regressions)
9. TODO.md and RUST-STYLE.md updated

---

## Next iteration

Phase 4: r-universe CI, remote stores. Configure r-universe build with
`--features full,s3,gcs`. Add HTTP reads via `zarrs_http` and `object_store`.
Test against public HTTPS Zarr stores and a public S3 bucket.
