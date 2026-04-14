# Phase 2, Iteration 3: write path + broader read eligibility

## Previous iterations (complete)

- **Phase 1:** extendr scaffolding, store cache, `zarrs_node_exists`,
  `zarrs_close_store`. 3 Rust functions, ~600 LOC Rust.
- **Phase 2, Iteration 1:** `zarrs_open_array_metadata`, `zarrs_runtime_info`,
  `zarrs_set_codec_concurrent_target`. `array_open.rs`, `dtype_dispatch.rs`,
  `metadata.rs`, `info.rs`. 6 total `#[extendr]` functions.
- **Phase 2, Iteration 2:** `zarrs_get_subset` + R-side dispatch.
  `retrieve.rs`, `R/zarrs-dispatch.R`, zarrs fast path in `get_selection()`.
  7 total `#[extendr]` functions. 9 Rust source files, 22 new tests.
- **Phase 2, Iteration 2.5:** Broader read eligibility — `IntDimIndexer`
  support in `can_use_zarrs()` and `selection_to_ranges()`, step>1 guard,
  zarrs_shape-based C→F reshape. R-only changes, 6 new tests.

---

## Phase 2 Iteration 3 — completion notes

### What shipped

1. **`src/rust/src/store.rs`** — write path, symmetric with `retrieve.rs`.
   Parses R list of `c(start, stop)` ranges, builds `ArraySubset`, dispatches
   on `RTypeFamily` (11 arms), narrows R types to stored types with range
   checks, calls `store_array_subset_opt(subset, data, opts)`. Uses
   `macro_rules!` for `narrow_double_to!` and `narrow_integer_to!`.

2. **`src/rust/src/lib.rs`** — `mod store;` + `#[extendr] fn zarrs_set_subset()`
   wrapper. 8 functions in `extendr_module!`.

3. **`R/zarrs-dispatch.R`** — `can_use_zarrs_write(indexer, store)`, delegates
   to `can_use_zarrs()`. Separate function for future read-only store support.

4. **`R/zarr-array.R`** — zarrs write fast path inserted in `set_selection()`
   (line ~658), after value preparation and before the R-native chunk loop.
   Handles NestedArray, scalar, and raw vector inputs. F→C order conversion
   via `aperm()` for nD arrays. Uses `tryCatch` for silent fallback.

5. **`R/extendr-wrappers.R`** — Auto-regenerated (8 `.Call` wrappers).

6. **`tests/testthat/test-zarrs-write.R`** — 11 tests: round-trip 1D float64,
   round-trip 2D int32, cross-path write+read, partial overwrite, int16
   narrowing, F-order conversion, MemoryStore fallback, concurrent_target.

### Deviations from plan

- **`store_array_subset_elements_opt` deprecated.** Plan (dev.md) assumed
  this API. zarrs 0.23 deprecates it in favor of `store_array_subset_opt()`
  which takes `impl IntoArrayBytes<'a>`. `Vec<T>` implements `IntoArrayBytes`
  when `T: Element` (which `ElementOwned` is a supertrait of). No turbofish
  needed — the trait dispatch is implicit.

- **Return type `bool` instead of `()`.** Rust `()` maps to R `NULL`, which
  is indistinguishable from the `tryCatch` error fallback `NULL`. Changed to
  return `true` on success so R can check `isTRUE(result)`.

- **F→C conversion uses `aperm()`.** Plan showed `aperm(value, rev(...))` on
  raw data. Actual implementation reshapes into an R array with the original
  dims, then `aperm()` to C-order, then flattens. This handles NestedArray
  values correctly.

### Lessons learned

1. `store_array_subset_opt` uses `impl IntoArrayBytes<'a>` — no turbofish.
   `Vec<T>` where `T: Element` satisfies this trait automatically.
2. `store_array_subset_elements_opt` is deprecated (same pattern as retrieve).
3. Rust `()` → R `NULL` makes success/failure indistinguishable in `tryCatch`.
   Return a sentinel `bool` instead.

---

## Done criteria (all met)

1. `rextendr::document()` succeeds — no warnings, no errors
2. `can_use_zarrs()` accepts `IntDimIndexer` dims (scalar selection) — ✓ (Iter 2.5)
3. `zarrs_set_subset()` writes V2 arrays (float64, int32, int16) — ✓
4. Round-trip read-write via zarrs produces correct results — ✓
5. zarrs and R-native write paths produce identical stored data — ✓
6. Unsupported selections fall through to R-native without error — ✓
7. All zarrs tests pass (28 retrieve + 11 write = 39) — ✓
8. All existing tests pass, 0 failures, 3 expected skips — ✓ (1375 total)

---

## Next iteration

Phase 4: r-universe CI, remote stores. Configure r-universe build with
`--features full,s3,gcs`. Add HTTP reads via `zarrs_http` and `object_store`.
Test against public HTTPS Zarr stores and a public S3 bucket.
