# pizzarr 0.2.0-dev

* Two-tier distribution: CRAN ships pure R; r-universe ships pre-built binaries
  with the zarrs Rust backend compiled in.
* Added zarrs Rust backend via extendr for parallel decompression and additional
  codec support (gzip, zstd, transpose).
* r-universe builds require no Rust toolchain for end users.
* New functions: `is_zarrs_available()`, `pizzarr_compiled_features()`,
  `pizzarr_upgrade()`.
* HTTP/HTTPS reads via zarrs (`zarrs_http` crate). When the zarrs backend is
  available, `HttpStore`-backed arrays use zarrs for parallel chunk decode
  on remote data. R-native `crul`-based path remains as fallback.
* C-order ↔ F-order transpose moved from R (`aperm`) to Rust, eliminating
  two full-array copies per read and up to five per write.
* Array creation via zarrs: `zarr_create()` transparently dispatches to the
  zarrs backend when available. Supports V2 and V3 formats with four codec
  presets (none, gzip, blosc, zstd). Completes the zarrs lifecycle:
  create, read, write, close.
* Removed R-native parallel chunk I/O infrastructure. The `pizzarr.parallel_backend`,
  `pizzarr.parallel_write_enabled`, and `pizzarr.progress_bar` options are no longer
  used. Setting them has no effect. On r-universe builds, zarrs handles parallelism
  internally via its Rust thread pool. On CRAN (pure R), chunk I/O is sequential.
  Dropped `pbapply`, `parallel`, `future`, and `future.apply` from Suggests.

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
