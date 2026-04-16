# Phase 4, Iteration 1: HTTP reads + Rust-side C/F transpose

## Previous phases (complete)

- **Phase 1:** extendr scaffolding, store cache, `zarrs_node_exists`,
  `zarrs_close_store`. 3 Rust functions, ~600 LOC Rust.
- **Phase 2, Iterations 1–3:** metadata, read path (`zarrs_get_subset`),
  write path (`zarrs_set_subset`), R-side dispatch. 8 `#[extendr]`
  functions, 10 Rust source files, 39 zarrs tests, 1375 total passing.
- **r-universe CI:** already building from `main` at
  `https://zarr-developers.r-universe.dev`. Binaries available.

---

## Goals

Two goals in one iteration — they touch the same Rust files and are
independently testable.

1. **HTTP reads via zarrs.** When a `ZarrArray` is backed by an
   `HttpStore` pointing at a public URL, the zarrs fast path handles
   `get_selection()` instead of falling through to the R-native
   `crul`-based chunk loop. Parallel chunk decode via rayon on remote
   data.

2. **Rust-side C↔F order transpose (P1 optimization).** Move the
   C-order ↔ F-order permutation from R (`aperm`) into Rust. Eliminates
   two full-array copies per read and up to five per write. Estimated to
   halve the gap with xarray on bulk operations (see TODO.md benchmarks).

Write dispatch (`can_use_zarrs_write`) already rejects non-filesystem
stores by design — HTTP write support is not in scope.

---

## Part A: HTTP reads — store cache enum

The store cache currently stores `ReadableWritableListableStorage`
(i.e., `Arc<dyn ReadableWritableListableStorageTraits>`). `zarrs_http::HTTPStore`
only implements `ReadableStorageTraits` — no write, no list.

The fix is a tagged enum that holds either the full read-write-list store
(filesystem) or a read-only store (HTTP). `array_open` and `retrieve`
call through the enum; `store` (write path) rejects the read-only variant
with a clear error.

```rust
// store_cache.rs
pub(crate) enum StorageEntry {
    ReadWriteList(ReadableWritableListableStorage),
    ReadOnly(ReadableStorage),
}
```

`Array::open()` accepts `impl Into<ReadableStorage>`. Both variants can
produce a `ReadableStorage` — filesystem via `Arc::clone` (it impls
`ReadableStorageTraits`), HTTP directly. This means `open_array_at_path`
returns `Array<dyn ReadableStorageTraits>` in both cases.

The write path (`zarrs_set_subset`) needs `WritableStorageTraits`, which
only the `ReadWriteList` variant satisfies. The R-side `can_use_zarrs_write`
already gates on store type, but Rust should also reject at the enum level
with a `PizzarrError::StoreReadOnly` error.

---

## Cargo.toml changes

Add `zarrs_http` as an optional dependency behind a new `http_sync` feature:

```toml
[features]
default = ["filesystem", "http_sync", "gzip", "zstd", "transpose"]
http_sync = ["dep:zarrs_http"]
# ... existing features unchanged

[dependencies]
zarrs_http = { version = "0.3", optional = true }
```

`http_sync` is in `default` — the default feature set only matters when
Rust compiles (r-universe tier). CRAN has no `src/` directory, so default
features are irrelevant there. Anyone compiling the Rust code wants HTTP.

**Actual version:** `zarrs_http 0.3.1` is compatible with `zarrs_storage 0.4.x`.

---

## Rust changes — HTTP

### 1. `store_cache.rs` — tagged enum

Replace `type StorageEntry = ReadableWritableListableStorage` with:

```rust
use zarrs_storage::{ReadableWritableListableStorage, ReadableStorage};

pub(crate) enum StorageEntry {
    ReadWriteList(ReadableWritableListableStorage),
    ReadOnly(ReadableStorage),
}
```

Add methods:
- `as_readable(&self) -> ReadableStorage` — works for both variants
- `as_readable_writable_listable(&self) -> Result<ReadableWritableListableStorage, PizzarrError>`
  — returns the inner value for `ReadWriteList`, errors for `ReadOnly`

The `get_or_insert` factory signature stays the same but now returns
`StorageEntry` (which is the enum, not a type alias).

### 2. `store_open.rs` — HTTP arm

```rust
#[cfg(feature = "http_sync")]
{
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        let http_store = zarrs_http::HTTPStore::new(trimmed)
            .map_err(|e| PizzarrError::StoreOpen {
                url: url.to_string(),
                reason: e.to_string(),
            })?;
        return Ok(StorageEntry::ReadOnly(Arc::new(http_store)));
    }
}
```

When `http_sync` is not compiled, the existing `FeatureNotCompiled` error
remains.

### 3. `array_open.rs` — accept both variants

Change `open_array_at_path` return type from
`Array<dyn ReadableWritableListableStorageTraits>` to
`Array<dyn ReadableStorageTraits>`.

Use `entry.as_readable()` to get the store handle. Since `Array::open`
accepts `impl Into<ReadableStorage>`, this works for both filesystem
and HTTP stores.

### 4. `retrieve.rs` — no changes expected for HTTP

`retrieve_array_subset_opt` only requires `ReadableStorageTraits`,
which both store variants satisfy via the `Array<dyn ReadableStorageTraits>`
returned by `array_open`. Verify this compiles — if zarrs internally
requires broader traits for retrieve, we'll need to adjust.

### 5. `store.rs` (write path) — guard on store type

The write path needs `WritableStorageTraits`. Open the array with the full
`ReadableWritableListableStorage` from the enum. Call
`entry.as_readable_writable_listable()?` and open the array on that.
This keeps the write path working for filesystem stores and errors cleanly
for HTTP. Rust enforces it, R also enforces it (defense in depth).

### 6. `error.rs` — new variant

```rust
#[error("StoreReadOnly({url}): cannot write to read-only store")]
StoreReadOnly { url: String },
```

### 7. `lib.rs` — feature reporting, no new exports

No new `#[extendr]` functions. The HTTP path is transparent — same
`zarrs_get_subset` call, different store backend. Add `"http_sync"` to
the `zarrs_compiled_features()` output via `cfg!(feature = "http_sync")`.

---

## Part B: Rust-side C↔F transpose (P1 optimization)

### Problem

zarrs returns flat C-order data. R stores arrays in F-order. The current
R-side conversion at `zarr-array.R:516-517` does two full copies:

```r
arr <- array(result$data, dim = rev(zarrs_shape))  # copy 1: reshape
arr <- aperm(arr, rev(seq_along(zarrs_shape)))      # copy 2: full transpose
```

For 500x500x100 float64 (200 MB), that's 400 MB of allocation +
permutation per read. The write path (`zarr-array.R:674-676`) does
the same in reverse — up to 3-5 copies total.

### Fix

Add transpose logic in Rust that permutes the flat vector before
crossing the FFI boundary.

#### `retrieve.rs` — C-to-F on read

After `retrieve_array_subset_opt` returns a `Vec<T>`, permute it from
C-order to F-order using a strided copy. The shape is known
(`subset.shape()`). For 1D, no permutation needed. For nD, a standard
index-remapping loop:

```rust
fn c_to_f_order<T: Copy>(data: &[T], shape: &[u64]) -> Vec<T> {
    // For each F-order index, compute the corresponding C-order index
    // and copy the element. Standard strided transpose.
}
```

Then the R side becomes:

```r
arr <- array(result$data, dim = zarrs_shape)  # single reshape, no copy
```

No `rev()`, no `aperm()`.

#### `store.rs` — F-to-C on write

Symmetric. Before calling `store_array_subset_opt`, permute the incoming
F-order data to C-order using the same strided copy logic (reversed).

Then the R write path simplifies to:

```r
write_data <- as.vector(value$data)  # flatten only, no aperm
```

#### Shared transpose module

Create `src/rust/src/transpose.rs` with:
- `c_to_f_order<T: Copy>(data: &[T], shape: &[u64]) -> Vec<T>`
- `f_to_c_order<T: Copy>(data: &[T], shape: &[u64]) -> Vec<T>`

Both skip permutation for 0D/1D arrays. Both are `O(n)` with a single
allocation. Two strategies for cache performance:
- **2D:** cache-blocked tiled transpose (64×64 blocks fitting L1 cache)
- **nD:** output-order iteration with incremental coordinate tracking
  (sequential writes, O(1) amortized index update via carry propagation)

The naive approach (iterate source order, scatter-write to output)
was 42% slower than R's `aperm()`. The cache-friendly version matches
or exceeds it (263–290 MB/s on 200 MB arrays).

#### R-side cleanup

- `zarr-array.R` read path (~lines 510-530): remove `rev(zarrs_shape)`
  reshape + `aperm()`. Replace with `array(result$data, dim = zarrs_shape)`.
- `zarr-array.R` write path (~lines 658-684): remove
  `array()` + `aperm()` + `as.vector()` chain. Replace with
  `as.vector(value$data)` (flatten only).

---

## R changes

### 1. `R/zarrs-dispatch.R` — read/write split

`can_use_zarrs()` currently accepts any non-null `store_id`. No change
needed — HTTP stores already return a URL from `get_store_identifier()`.

`can_use_zarrs_write()` needs a store-type guard:

```r
can_use_zarrs_write <- function(indexer, store) {
  # HTTP stores are read-only in zarrs
  if (inherits(store, "HttpStore")) return(FALSE)
  can_use_zarrs(indexer, store)
}
```

### 2. `R/zarrs-bridge.R` — feature reporting

`pizzarr_compiled_features()` already reports compiled features.
`zarrs_compiled_features()` in Rust includes `"http_sync"` when the
feature is compiled. No R changes needed.

### 3. `R/zarr-array.R` — simplified read/write paths

Remove the `aperm()` calls on the zarrs fast path (both read and write).
Rust now handles the transpose. See Part B above for details.

---

## R-universe build configuration

`http_sync` is in `default` features. r-universe compiles with default
features automatically. No special build configuration needed.

---

## Testing

### New test file: `tests/testthat/test-zarrs-http.R`

Tests gated on `skip_if_not("http_sync" %in% pizzarr_compiled_features())`.

1. **Public Zarr V2 read via zarrs HTTP** — open a known public HTTPS
   Zarr store, read a small subset via `zarrs_get_subset`, verify values.
   Use the same Embassy EBI fixture as `test-http-store.R`.

2. **Zarrs vs R-native cross-check** — read the same subset via both
   paths, compare results. Force R-native by temporarily setting
   `.pizzarr_env$zarrs_available <- FALSE`.

3. **Write dispatch rejects HttpStore** — confirm `can_use_zarrs_write`
   returns FALSE for an HttpStore.

4. **Feature detection** — `"http_sync" %in% pizzarr_compiled_features()`
   when compiled with the feature.

### Transpose tests (in existing test files)

The existing zarrs read/write tests (`test-zarrs-retrieve.R`,
`test-zarrs-write.R`) already verify correctness of nD round-trips.
After the transpose moves to Rust, these tests validate the new path
without modification. If any fail, the Rust transpose has a bug.

Add a targeted test for the transpose edge cases:
- 1D array (no permutation needed)
- 2D array (simple transpose)
- 3D+ array (full nD permutation)
- Scalar selection mixed with slice (shape has dropped dims)

### VCR cassettes

The existing HTTP tests use VCR cassettes. The zarrs HTTP path goes
through Rust's `reqwest`, not R's `crul`, so VCR won't intercept zarrs
requests.

**Recommendation:** Live requests for a small fixture, gated behind
`skip_on_cran()` and `skip_if_offline()`. The zarrs HTTP read is the
same retrieve codepath as filesystem — we're really just testing that
the store opens and serves bytes. A single small array read is sufficient.

---

## Implementation steps

### HTTP reads
1. **Cargo.toml** — add `zarrs_http` dep, `http_sync` feature, add to `default`.
2. **`store_cache.rs`** — replace type alias with `StorageEntry` enum,
   add `as_readable()` and `as_readable_writable_listable()` methods.
3. **`store_open.rs`** — add HTTP arm behind `#[cfg(feature = "http_sync")]`.
4. **`error.rs`** — add `StoreReadOnly` variant.
5. **`array_open.rs`** — change return type to `Array<dyn ReadableStorageTraits>`.
6. **`retrieve.rs`** — verify compiles with new array type. Adjust if needed.
7. **`store.rs`** (write) — use `as_readable_writable_listable()` to get
   the full store handle; error on read-only.
8. **`lib.rs`** — add `http_sync` to `zarrs_compiled_features()` output.
9. **`rextendr::document()`** — regenerate wrappers.
10. **`R/zarrs-dispatch.R`** — guard `can_use_zarrs_write` against HttpStore.
11. **Tests** — add `test-zarrs-http.R`.

### Rust-side transpose
12. **`src/rust/src/transpose.rs`** — `c_to_f_order`, `f_to_c_order`.
13. **`retrieve.rs`** — call `c_to_f_order` after retrieve, before returning.
14. **`store.rs`** — call `f_to_c_order` on incoming data before storing.
15. **`lib.rs`** — `mod transpose;`.
16. **`R/zarr-array.R`** — remove `aperm()` calls on zarrs fast path
    (read ~lines 510-530, write ~lines 658-684).

### Documentation and wrap-up
17. **Full test suite** — all existing tests pass, 0 new failures.
18. **`NEWS.md`** — add entries for HTTP read support and Rust-side
    transpose optimization under `pizzarr 0.2.0-dev`.
19. **`TODO.md`** — update Phase 4 status to note Iteration 1 complete.
    Mark P1 (C-to-F transpose) as done in the performance optimization
    roadmap. Update benchmark table if new numbers are available.
20. **`CLAUDE.md`** — update the Phase description under
    "v0.2.0: zarrs/extendr migration" to reflect HTTP read support and
    the Rust transpose. Add any new gotchas discovered during
    implementation (e.g., `zarrs_http` version pinning, `StorageEntry`
    enum pattern, trait bound surprises).
21. **`RUST-STYLE.md`** — add a "Lessons learned (Phase 4 Iteration 1)"
    section covering: `zarrs_http` API surface (constructor, trait impls),
    `StorageEntry` enum pattern for mixed read-only/read-write stores,
    strided transpose implementation details, any `Array::open` trait
    bound surprises.
22. **`dev.md`** — update completion notes (what shipped, deviations
    from plan, lessons learned) following the same format as previous
    iterations.

---

## Done criteria

1. `rextendr::document()` succeeds
2. `zarrs_get_subset` reads from a public HTTPS Zarr store
3. `can_use_zarrs_write` returns FALSE for HttpStore
4. `zarrs_set_subset` on an HTTP store errors cleanly (not silently)
5. `"http_sync" %in% pizzarr_compiled_features()` returns TRUE
6. Rust-side transpose produces identical results to R-side `aperm()`
7. R-side zarrs fast path has no `aperm()` calls (read or write)
8. All existing tests pass (1375+), 0 new failures, 3 expected skips
9. New HTTP tests pass when run with network access
10. NEWS.md, TODO.md, CLAUDE.md, RUST-STYLE.md, dev.md all updated

---

## Risks and mitigations

- **`zarrs_http` version compatibility** — zarrs_http may not have a
  release matching zarrs 0.23.x. Check crates.io. If the latest
  zarrs_http targets a different zarrs version, pin appropriately or
  use git dep.

- **`Array::open` trait bounds** — zarrs may require broader traits
  than `ReadableStorageTraits` for `Array::open`. If so, the enum
  approach needs adjustment (e.g., blanket impl or wrapper type).
  Verify at compile time.

- **reqwest TLS on Windows** — zarrs_http uses `reqwest::blocking`.
  On Windows, reqwest's default TLS (native-tls / schannel) should
  work. If not, may need `reqwest` feature flags for rustls.

- **Store cache memory** — HTTP stores hold a reqwest client internally.
  Not a concern for typical usage (few stores), but `zarrs_close_store`
  remains the escape valve.

- **Transpose correctness** — the strided copy must match R's `aperm()`
  exactly. Validate with the existing nD round-trip tests. Off-by-one
  in stride calculation is the most likely bug.

- **Transpose performance** — a single-threaded strided copy should
  still be faster than two R-side copies + `aperm()`. If benchmarks
  show otherwise, add rayon parallelism to the transpose. Measure
  before optimizing.

---

## Phase 4 Iteration 1 — completion notes

### What shipped

1. **`src/rust/Cargo.toml`** — `zarrs_http = "0.3"` optional dep, `http_sync`
   feature added to `default`.

2. **`src/rust/src/store_cache.rs`** — `StorageEntry` enum replacing the type
   alias. `ReadWriteList` (filesystem) and `ReadOnly` (HTTP) variants.
   `as_readable()` and `as_readable_writable_listable(url)` methods.

3. **`src/rust/src/store_open.rs`** — restructured with `open_http_store` and
   `open_filesystem_store` helpers. HTTP arm behind `#[cfg(feature = "http_sync")]`.

4. **`src/rust/src/array_open.rs`** — split into `open_array_at_path` (read,
   returns `Array<dyn ReadableStorageTraits>`) and `open_array_at_path_rw`
   (write, returns `Array<dyn ReadableWritableListableStorageTraits>`).

5. **`src/rust/src/transpose.rs`** (new) — `c_to_f_order` and `f_to_c_order`.
   Cache-blocked 2D (64×64 tiles) and output-order nD (incremental coordinate
   tracking). Unit tested for 1D, 2D, 2D-large, 3D, 3D-large, 4D, scalar, empty.

6. **`src/rust/src/retrieve.rs`** — array type changed to
   `Array<dyn ReadableStorageTraits>`, transpose integrated after retrieve.

7. **`src/rust/src/store.rs`** — uses `open_array_at_path_rw`, transpose
   integrated before store.

8. **`src/rust/src/error.rs`** — `StoreReadOnly` variant added.

9. **`src/rust/src/lib.rs`** — `mod transpose`, `http_sync` feature reporting.

10. **`R/zarrs-dispatch.R`** — `can_use_zarrs_write` rejects `HttpStore`.

11. **`R/zarr-array.R`** — removed `aperm()` calls on zarrs fast path.

12. **`tests/testthat/test-zarrs-http.R`** — 7 tests: feature detection, HTTPS
    read, zarrs-vs-R-native cross-check, write dispatch rejection.

### Deviations from plan

- **No `ReadableStorageTraits` impl on `StorageEntry`.** The plan assumed
  delegating the trait through the enum. The trait has lifetime-parameterized
  methods (`get_partial_many<'a>`) that are awkward to implement on an enum
  wrapping `Arc<dyn Trait>`. Instead, `store_get` in `lib.rs` calls
  `entry.as_readable()` to get the underlying `Arc` and calls `.get()` on that.

- **`zarrs_http 0.3.x`, not `0.6`.** The plan placeholder version was wrong.
  `zarrs_http 0.3.1` is the version compatible with `zarrs_storage 0.4.x`.

- **Test fixture uses GitHub raw content URL**, not Embassy EBI. The EBI
  fixture requires blosc which adds complexity. The `bcsd.zarr` fixture from
  DOI-USGS/rnz uses gzip which zarrs supports out of the box.

### Lessons learned

1. `ReadableStorageTraits` required methods are `get_partial_many`, `size_key`,
   `supports_get_partial`. `get` is a provided method — don't try to implement
   it as required.
2. Don't implement complex traits on enum wrappers over `Arc<dyn Trait>` with
   lifetime parameters. Extract the `Arc` and call trait methods directly.
3. `zarrs_http` pulls in reqwest + tokio (~80 crates), even for "synchronous"
   HTTP. Compiles fine but adds to build time.
4. Cache performance matters for transpose. The naive strided approach
   (iterate source order, scatter-write) was 42% slower than R's C-level
   `aperm()` on 200 MB arrays. Switching to cache-blocked 2D tiles and
   output-order nD iteration (sequential writes, incremental index tracking)
   recovered performance to match or beat `aperm()`.
5. Tests that directly call `zarrs_get_subset` and manually do `rev`/`aperm`
   need updating when the transpose moves to Rust.

---

## Done criteria (all met)

1. `rextendr::document()` succeeds — ✓
2. `zarrs_get_subset` reads from a public HTTPS Zarr store — ✓
3. `can_use_zarrs_write` returns FALSE for HttpStore — ✓
4. `zarrs_set_subset` on HTTP store errors with `StoreReadOnly` — ✓
5. `"http_sync" %in% pizzarr_compiled_features()` returns TRUE — ✓
6. Rust-side transpose produces identical results to R-side `aperm()` — ✓
7. R-side zarrs fast path has no `aperm()` calls (read or write) — ✓
8. All existing tests pass, 0 failures, 3 expected skips — ✓ (1380 total)
9. New HTTP tests pass with network access — ✓ (7 tests)
10. NEWS.md, TODO.md, CLAUDE.md, RUST-STYLE.md, dev.md all updated — ✓

---

## Future iterations (not in scope)

- **Phase 4, Iteration 2:** `object_store` HTTP backend (auth, pooling,
  retries) + S3/GCS via `zarrs_object_store`. Requires tokio runtime.
- **P2 (Array cache):** Cache opened `zarrs::Array` objects in Rust to
  skip metadata re-parse on repeated reads.
- **P3 (Skip NestedArray):** Return raw R array from zarrs fast path,
  skip NestedArray R6 wrapping.
- **Phase 5:** Strip R-native parallelism, ship to CRAN as pure-R update.
- **`zarrs_create_array`** (§4.7 in TODO.md) — deferred.
