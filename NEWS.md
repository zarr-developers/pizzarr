# pizzarr 0.2.1

## Cloud storage fixes

* S3 requests were always sent unsigned, so configured AWS credentials were
  ignored and private buckets returned 403. Signing is now decided from the
  environment: requests are unsigned when no credential source is present, so
  public buckets still work without configuration, and signed as soon as
  `AWS_ACCESS_KEY_ID`, `AWS_SESSION_TOKEN`, `AWS_PROFILE`, or another
  credential variable is set. `AWS_SKIP_SIGNATURE` forces either behaviour.
* `zarr_open()` on an `s3://` or `gs://` string fell through to
  `DirectoryStore`, which tried to create a directory named after the URL.
  These URLs now resolve to `S3Store` and `GcsStore`, and cloud stores default
  to read mode.
* `S3Store` and `GcsStore` inherited no-op `get_item()`, `contains_item()`, and
  `listdir()` methods that silently returned `NULL`. They now raise an error
  naming the supported read paths.
* `HttpStore$listdir()` took no arguments, so the `path` argument every other
  store accepts raised an error that callers swallowed --- `ZarrGroup`'s print
  method reported `No. members : 0` for every HTTP-backed group. It now accepts
  `path` and lists within it.

## Documentation

* New `vignettes/remote-stores.Rmd` covering remote and cloud access: the
  mapping from `fsspec`/`xarray` `storage_options` to pizzarr's environment
  variables, the HTTPS and `s3://` routes, alternate endpoints such as MinIO,
  Ceph, and Open Storage Network pods, and the current limits.
* Alternate S3 endpoints are configured with `AWS_ENDPOINT`; `AWS_ALLOW_HTTP`
  and `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` cover plain-HTTP and path-style
  addressing. This worked before but was undocumented.
* Anonymous access to public GCS buckets over `gs://` is documented, and now
  tested. It needs `GOOGLE_SKIP_SIGNATURE=true`, since GCS does not infer
  anonymous access from absent credentials the way S3 does.
* `S3Store`, `GcsStore`, and `HttpStore` gained detailed documentation of
  credentials, endpoints, and which distribution tier each requires.
* `zarr_open()`'s `store` argument now documents every accepted form: a path,
  an `http(s)://` URL, an `s3://` or `gs://` URL, or a `Store` object.
* `zarr_open_group()`'s `storage_options` argument is documented as ignored
  rather than as an `fsspec` passthrough.
* Vignettes reorganized around data conventions rather than transport, now that
  connection details have their own article. `remote-ome-ngff.Rmd` merged into
  `ome-ngff.Rmd`, which covers the OME-NGFF multiscales convention against both
  a local and a remote store; `remote-anndata.Rmd` renamed `anndata.Rmd`.
* The OME-NGFF and AnnData vignettes now evaluate their code at build time
  instead of showing static listings, so the examples are checked. Both require
  the `blosc` package and network access, and degrade to unevaluated when either
  is missing.
* README gained a "Remote and cloud data" section.

# pizzarr 0.2.0

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
  Public buckets work without credentials (unsigned requests).
* GCS reads via `object_store` crate. New `GcsStore` R6 class for `gs://` URLs.
  Public GCS data accessible via HTTPS endpoint without credentials.
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
