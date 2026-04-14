# Contributing to pizzarr

## Development Setup

``` r
setwd("path/to/pizzarr")
install.packages("devtools")
devtools::install()
devtools::load_all()
```

## Build and Test Cycles

pizzarr has two build tiers that affect how you develop and test locally.

### CRAN tier (pure R)

If you are working on R-side logic (indexers, stores, codecs, R6 classes)
and do not need the zarrs backend, this is the faster cycle:

``` r
devtools::load_all()
devtools::test()
devtools::check()
```

Tests run single-threaded (`Config/testthat/parallel: false` in
DESCRIPTION). All zarrs-specific tests skip automatically when the Rust
library is absent — they guard on `.pizzarr_env$zarrs_available`.

To produce a CRAN-ready source tarball that strips `src/`, `configure`,
and `SystemRequirements`:

``` bash
bash tools/cran-build.sh
```

The resulting `.tar.gz` passes `R CMD check` without a Rust toolchain.

### r-universe tier (zarrs backend)

The r-universe build compiles the [zarrs](https://github.com/zarrs/zarrs)
Rust crate via [extendr](https://extendr.github.io/rextendr/). You need
rustc >= 1.91 with the GNU target on Windows:

``` bash
rustup target add x86_64-pc-windows-gnu
```

After modifying any `#[extendr]` function in `src/rust/src/lib.rs`,
regenerate the R wrappers:

``` r
rextendr::document()
```

This rebuilds the Rust library and regenerates `R/extendr-wrappers.R`.
Never edit that file by hand. Then run the standard cycle:

``` r
devtools::load_all()
devtools::test()
devtools::check()
```

`devtools::check()` will emit a NOTE about downloading Rust crates — that
is expected and is not a problem for r-universe builds. See
[RUST-STYLE.md](RUST-STYLE.md) for Rust conventions, module layout, and
build pipeline details.

## Building Documentation

``` r
devtools::document()
pkgdown::build_site()
```

## Coding Conventions

- **Style**: snake_case everywhere (functions, methods, variables).
  2-space indent.
- **Errors**: `stop("ErrorName(details)")` — mimics Python Zarr error
  names. No rlang.
- **Warnings/info**: [`warning()`](https://rdrr.io/r/base/warning.html)
  and [`message()`](https://rdrr.io/r/base/message.html) from base R.
- **Assertions**: direct `if (...) stop()` — no assertion library.
- **R6 docs**: roxygen2 with `@title`, `@docType class`,
  `@format [R6::R6Class]`. See `r6-roxygen-convention.md` for the full
  style guide.
- **Tests**: testthat 3e with `test_that("description", { ... })`.

## Resources

- [Discussion of Zarr in
  R](https://github.com/zarr-developers/community/issues/18)
- [blosc](https://cran.r-project.org/web/packages/blosc/index.html)
  - Note: `pizzarr` has an optional dependency on `blosc` for Blosc
    (de)compression.
- R package development
  - [R packages](https://r-pkgs.org/)
  - [roxygen2
    syntax](https://roxygen2.r-lib.org/articles/rd-formatting.html)
  - [R6](https://r6.r-lib.org/index.html)
  - [R6 roxygen2
    syntax](https://www.tidyverse.org/blog/2019/11/roxygen2-7-0-0/#r6-documentation)
  - [pkgdown](https://pkgdown.r-lib.org/)
- Zarr implementation
  - [zarr_implementations](https://github.com/zarr-developers/zarr_implementations)
  - [zarr-python](https://github.com/zarr-developers/zarr-python)
  - [ZSTD compression in R](https://github.com/qsbase/qs2)
  - [LZ4 and ZSTD compression in R -
    archived](https://github.com/qsbase/qs)
  - [zarr.js](https://github.com/gzuidhof/zarr.js)
  - [zarrita.js](https://github.com/manzt/zarrita.js)
  - [v2 spec](https://zarr.readthedocs.io/en/stable/spec/v2.html)
