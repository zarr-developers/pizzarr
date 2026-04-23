# pizzarr 0.2.0-pre

## Two-tier distribution

* CRAN ships pure R (no Rust compilation). r-universe ships pre-built binaries
  with the zarrs Rust backend compiled in. No Rust toolchain needed for end
  users on either tier.
* New functions: `is_zarrs_available()`, `pizzarr_compiled_features()`,
  `pizzarr_upgrade()`.
* `tools/cran-build.sh` produces a CRAN tarball with `src/` stripped out.

## zarrs Rust backend (r-universe tier)

* Added zarrs Rust backend via extendr for parallel decompression, codec
  execution, and store abstraction. 9 `#[extendr]` functions, 13 Rust source
  files.
* Transparent dispatch: `ZarrArray$get_item()` and `$set_item()` automatically
  use the zarrs fast path when eligible (contiguous slices, supported dtype,
  filesystem or HTTP store). Unsupported selections fall through to the R-native
  path silently.
* Read path: two-step dtype dispatch retrieves data at stored precision, then
  widens to R-compatible types (f32→f64, i16→i32, etc.) in Rust before crossing
  the FFI boundary.
* Write path: symmetric narrowing with range checks (R double→f32, R integer→i16,
  etc.). Out-of-range values produce errors, not silent truncation.
* Array creation via zarrs: `zarr_create()` dispatches to the zarrs backend when
  available. Supports V2 and V3 formats with four codec presets (none, gzip,
  blosc, zstd).
* C↔F order transpose moved from R (`aperm`) to Rust, eliminating two full-array
  copies per read and up to five per write. Cache-blocked tiling for 2D,
  output-order iteration for nD.
* HTTP/HTTPS reads via `zarrs_http` crate. `HttpStore`-backed arrays use zarrs
  for parallel chunk decode on remote data. R-native `crul`-based path remains
  as fallback.
* S3 reads via `object_store` crate. New `S3Store` R6 class for `s3://` URLs.
  Public buckets work without credentials (unsigned requests). Authenticated
  access uses standard AWS environment variables.
* GCS reads via `object_store` crate. New `GcsStore` R6 class for `gs://` URLs
  (requires credentials). Public GCS data accessible via HTTPS endpoint without
  credentials.
* Process-global store handle cache with explicit lifecycle management via
  `zarrs_close_store()`.

## Configuration

* New `pizzarr_config()` function for viewing and setting concurrency options.
  Three settings: `nthreads` (rayon thread pool size), `concurrent_target`
  (codec concurrency level), and `http_batch_range_requests` (multipart range
  toggle for HTTP stores). All three backed by environment variables
  (`PIZZARR_NTHREADS`, `PIZZARR_CONCURRENT_TARGET`,
  `PIZZARR_HTTP_BATCH_RANGE_REQUESTS`).
* `zarrs_runtime_info()` now includes `nthreads` (rayon thread pool size).

## Breaking changes

* Removed R-native parallel chunk I/O infrastructure. The
  `pizzarr.parallel_backend`, `pizzarr.parallel_write_enabled`, and
  `pizzarr.progress_bar` options are no longer used. On r-universe builds, zarrs
  handles parallelism internally via its Rust thread pool. On CRAN (pure R),
  chunk I/O is sequential.
* Dropped `pbapply`, `parallel`, `future`, and `future.apply` from Suggests.
* Removed `vignettes/parallel.Rmd`. Replaced by `vignettes/zarrs-backend.Rmd`.
* Minimum R version raised from 4.1.0 to 4.2.

## Other changes

* New `vignettes/zarrs-backend.Rmd` documenting the zarrs backend, dispatch
  behavior, store types, concurrency tuning, and cloud access.
* R-native chunk loop simplified from parallel dispatch infrastructure to
  sequential `for` loop.

# pizzarr 0.1.3 (never released)

* Added `set_dimension_names()` / `get_dimension_names()` methods to `ZarrArray` (#170).
* pkgdown site and documentation improvements (#171).

# pizzarr 0.1.2

* Added `get_dimension_names()` method to `ZarrArray` for V3 dimension names support.
* OME-NGFF vignettes now use base R `rasterImage()` instead of the `raster` package (#161).

# pizzarr 0.1.1

Fixes a flaky test failing on CRAN -- no functional changes.

# pizzarr 0.1.0

This is the first release of pizzarr to CRAN. The package has significant testing and 
validation but has not been vetted by a large and diverse user community. As a result,
bugs and undesirable behavior may be found. If you find issues please report them at:

https://github.com/zarr-developers/pizzarr/issues

* Initial release of pizzarr.
* Read and write Zarr V2 and Zarr V3 stores.
* Storage backends: DirectoryStore, MemoryStore, HttpStore.
* Compression codecs: Zstd, LZ4, Blosc, zlib, gzip, bzip2, LZMA.
* VLenUTF8 object codec for variable-length strings.
* Parallel read/write support via pbapply, parallel, and future.
* R-like one-based and Python-like zero-based slicing.
