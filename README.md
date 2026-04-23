
<!-- README.md is generated from README.Rmd. Please edit that file -->

# pizzarr <a href="https://zarr.dev/pizzarr/"><img src="man/figures/logo.png" align="right" height="139" alt="pizzarr website" /></a>

[![codecov](https://codecov.io/gh/zarr-developers/pizzarr/graph/badge.svg?token=vhidertN9l)](https://app.codecov.io/gh/zarr-developers/pizzarr)
[![R-CMD-check](https://github.com/zarr-developers/pizzarr/actions/workflows/R-CMD-Check.yml/badge.svg)](https://github.com/zarr-developers/pizzarr/actions/workflows/R-CMD-Check.yml)
[![logs](https://cranlogs.r-pkg.org/badges/pizzarr)](https://cran.r-project.org/package=pizzarr)
[![CRAN
status](https://www.r-pkg.org/badges/version/pizzarr)](https://CRAN.R-project.org/package=pizzarr)
[![r-universe](https://zarr-developers.r-universe.dev/badges/pizzarr)](https://zarr-developers.r-universe.dev/pizzarr)

A Zarr implementation for R.

## Installation

pizzarr ships in two tiers — same version, different builds. The CRAN
build is pure R and needs nothing beyond base R dependencies. The
[r-universe](https://zarr-developers.r-universe.dev/pizzarr) build links
in the [zarrs](https://github.com/zarrs/zarrs) Rust crate via
[extendr](https://cran.r-project.org/package=rextendr), adding parallel
decompression and additional codec support.

### CRAN (pure R)

``` r
install.packages("pizzarr")
```

### r-universe (zarrs backend)

``` r
install.packages("pizzarr",
                 repos = c("https://zarr-developers.r-universe.dev",
                            "https://cloud.r-project.org"))
```

Both sources serve the same release version. The r-universe build
distributes pre-compiled binaries for Windows and macOS — no Rust
toolchain needed.

The r-universe binaries compile the zarrs backend with local
filesystem I/O, synchronous HTTP reads, gzip/zstd/blosc codecs,
sharding, and S3/GCS cloud store support via `object_store`. The
build system enables these features automatically when `NOT_CRAN` is
set (which r-universe does). To compile with a different feature set,
set `PIZZARR_FEATURES` explicitly (see below).

`pizzarr_compiled_features()` lists what the zarrs backend provides,
and `pizzarr_upgrade()` prints the install command when zarrs is not
compiled in. `pizzarr_config()` controls concurrency settings (thread
pool size, codec concurrency, HTTP range request batching). See
`vignette("zarrs-backend")` for details.

### Building from source (zarrs backend)

If you need to compile the zarrs backend yourself — either because no
binary is available for your platform, or because you are developing
the Rust side — you need rustc \>= 1.91. On Windows, add the GNU
target:

``` bash
rustup target add x86_64-pc-windows-gnu
```

The build system uses two environment variables to control compilation:

- **`DEBUG`** — set to any non-empty value for a debug build. This
  enables Cargo's incremental compilation, so subsequent rebuilds are
  fast (seconds instead of minutes). The resulting library is slower at
  runtime because it skips link-time optimization and does not strip
  symbols. Use this during active development on the Rust code.
- **`PIZZARR_FEATURES`** — a comma-separated list of extra Cargo
  features. When `NOT_CRAN` is set and `PIZZARR_FEATURES` is empty,
  the build defaults to `s3,gcs` (S3 and GCS cloud store support via
  `object_store` and `tokio`). Set this explicitly to override — e.g.,
  `PIZZARR_FEATURES=none` for default features only, or
  `PIZZARR_FEATURES=s3` for S3 without GCS.

A release build (no `DEBUG`) uses LTO, single codegen unit, and symbol
stripping — fast at runtime but takes several minutes to compile from
scratch.

``` bash
# Development: fast rebuilds, slower runtime, S3/GCS included
DEBUG=1 R CMD INSTALL .

# Production: slow first build, fast runtime, S3/GCS included
NOT_CRAN=true R CMD INSTALL .

# Production with default features only (no cloud stores)
R CMD INSTALL .
```

`NOT_CRAN` is set automatically when `DEBUG` is present. For builds
without vendored crates (the normal case for local development),
`NOT_CRAN` prevents the build system from attempting offline
compilation.

Development happens on the `develop` branch. See
[CONTRIBUTING.md](https://github.com/zarr-developers/pizzarr/blob/main/CONTRIBUTING.md)
for the branching and release model.

## Usage

``` r
library(pizzarr)

# Open a sample BCSD climate dataset (Zarr V3)
v3_root <- pizzarr_sample("bcsd_v3")
v3 <- zarr_open(v3_root)

# Print the group summary
v3
#> <ZarrGroup> /
#>   Store type  : DirectoryStore
#>   Zarr format : 3
#>   Read-only   : FALSE
#>   No. members : 5

# View the hierarchy
v3$tree()
#> / 
#> ├── latitude (33) <f4
#> ├── longitude (81) <f4
#> ├── pr (12, 33, 81) <f4
#> ├── tas (12, 33, 81) <f4
#> └── time (12) <f8

# Inspect an array
v3$get_item("pr")
#> <ZarrArray> /pr
#>   Shape       : (12, 33, 81)
#>   Chunks      : (12, 33, 81)
#>   Data type   : <f4
#>   Fill value  : 1.00000002004088e+20
#>   Order       : C
#>   Read-only   : FALSE
#>   Compressor  : ZstdCodec
#>   Store type  : DirectoryStore
#>   Zarr format : 3

# Read a slice: first 3 time steps, first 3 latitudes, first longitude
v3$get_item("pr")$get_item(list(slice(1, 3), slice(1, 3), 1))$data
#> , , 1
#> 
#>        [,1]   [,2]   [,3]
#> [1,] 133.97 144.51 149.92
#> [2,]  75.40  72.38  68.62
#> [3,]  93.14  91.24  89.37
```

Create an array from scratch:

``` r
a <- array(data = 1:20, dim = c(2, 10))
z <- zarr_create(shape = dim(a), dtype = "<f4", fill_value = NA)
z$set_item("...", a)
```

``` r
z
#> <ZarrArray> /
#>   Shape       : (2, 10)
#>   Chunks      : (2, 10)
#>   Data type   : <f4
#>   Fill value  : 0
#>   Order       : F
#>   Read-only   : FALSE
#>   Compressor  : ZstdCodec
#>   Store type  : MemoryStore
#>   Zarr format : 2
z$get_item(list(slice(1, 2), slice(1, 5)))$data
#>      [,1] [,2] [,3] [,4] [,5]
#> [1,]    1    3    5    7    9
#> [2,]    2    4    6    8   10
```

## Features

- **Zarr V2 and V3** read and write (format auto-detected on open)
- **Stores:** MemoryStore, DirectoryStore (read/write); HttpStore
  (read-only)
- **Data types:** boolean, int8–int64, uint8–uint64, float16/32/64,
  string, Unicode, VLenUTF8
- **Compression:** zlib/gzip, bzip2, blosc, LZMA, LZ4, Zstd
- **Blosc** requires the optional
  [`blosc`](https://cran.r-project.org/package=blosc) package
  (`install.packages("blosc")`)

## How It Works

pizzarr uses [R6](https://r6.r-lib.org/) classes mirroring the
[zarr-python](https://github.com/zarr-developers/zarr-python) object
model:

- **Store** — backend storage (`DirectoryStore` for local files,
  `MemoryStore` for in-memory, `HttpStore` for remote read-only)
- **ZarrGroup** — hierarchical container holding arrays and sub-groups
  (like a directory)
- **ZarrArray** — chunked, compressed N-dimensional array (like a file)
- **Codec** — compression/decompression (zlib, zstd, blosc, lz4, etc.)
- **Dtype** — data-type mapping between R and Zarr

Data flows through the stack: a **Store** holds raw chunk bytes, a
**Codec** pipeline compresses and decompresses them, and **ZarrArray**
presents typed N-dimensional data to R. Groups and arrays are addressed
by path within a store, just like files in a directory tree.

See `vignette("pizzarr")` for a full walkthrough.

## Ecosystem

pizzarr implements the [Zarr
specification](https://zarr-specs.readthedocs.io/) (V2 and V3) for R.
Related projects:

- [zarr-python](https://github.com/zarr-developers/zarr-python) — the
  reference Python implementation
- [zarr.js](https://github.com/gzuidhof/zarr.js) — JavaScript
  implementation
- [zarr](https://cran.r-project.org/package=zarr) — native R V3
  implementation (CRAN)
- [Rarr](https://bioconductor.org/packages/Rarr/) — Bioconductor package
  for reading and writing individual Zarr arrays (V2, limited write
  support)
- [zarr-conformance-tests](https://github.com/Bisaloo/zarr-conformance-tests)
  — cross-implementation validation

## Validation with zarr-python

A standalone integration test cross-validates that pizzarr and
zarr-python produce equivalent Zarr stores. Both implementations write
the same arrays (V2 and V3 formats, multiple dtypes, codecs, chunk
layouts, and groups with attributes), then each reads the other’s output
and verifies the data matches.

**Prerequisites:** Python 3.10+ with `zarr>=3` and `numpy` installed.

``` bash
Rscript inst/extdata/cross-validate.R
```

The script skips gracefully (exit 0) if Python is not available. On
success all checks pass and exit code is 0; any mismatch is reported and
exits 1.

## Zarr Conformance Tests

pizzarr participates in the
[zarr-conformance-tests](https://github.com/Bisaloo/zarr-conformance-tests)
framework, which validates that Zarr implementations can correctly read
standard test arrays (V2 and V3 formats, multiple dtypes). These tests
run automatically in CI on every push and pull request to `main`.

## Performance

The zarrs backend (r-universe install only) handles chunk I/O and codec execution in Rust, bypassing
R's single-threaded chunk loop entirely. For compressed data — the common
case with real Zarr stores — this translates to 6–8x throughput over pure-R.

The table below compares pizzarr's two tiers against zarr-python/xarray on
a 500×500×100 float64 array (~200 MB uncompressed) with 100×100×50 chunks.
All numbers are best-of-three throughput in MB/s, measured on Windows
(pizzarr 0.2.0-dev, zarr-python 3.1.5, April 2026).

| Scenario | pizzarr zarrs | pizzarr R-native | xarray |
|---|---|---|---|
| read_all (gzip) | 253 MB/s | 39 MB/s | 545 MB/s |
| read_all (none) | 247 MB/s | 39 MB/s | 1811 MB/s |
| read_subset (gzip) | **200 MB/s** | 38 MB/s | 138 MB/s |
| read_subset (none) | 229 MB/s | 38 MB/s | 704 MB/s |
| write_all (gzip) | 97 MB/s | 12 MB/s | 143 MB/s |
| write_all (none) | 286 MB/s | 144 MB/s | 477 MB/s |

xarray leads on bulk operations because numpy can transpose array memory
layout via stride views (zero copy) — R's `array()` always copies. The
gap narrows with compression since decompression dominates, and zarrs wins
on cross-chunk subset reads (200 vs 138 MB/s gzip) because it decodes
only the chunks that overlap the selection.

Reproduce with `Rscript inst/extdata/benchmark-zarrs-vs-xarray.R` (requires
Python with `zarr>=3` and `xarray` for the xarray column).

## Contributing

See
[CONTRIBUTING.md](https://github.com/zarr-developers/pizzarr/blob/main/CONTRIBUTING.md)
for development setup, testing, and documentation build instructions.
