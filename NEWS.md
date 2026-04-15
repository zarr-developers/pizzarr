# pizzarr 0.2.0-dev

* Two-tier distribution: CRAN ships pure R; r-universe ships pre-built binaries
  with the zarrs Rust backend compiled in.
* Added zarrs Rust backend via extendr for parallel decompression and additional
  codec support (gzip, zstd, transpose).
* r-universe builds require no Rust toolchain for end users.
* New functions: `is_zarrs_available()`, `pizzarr_compiled_features()`,
  `pizzarr_upgrade()`.

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
