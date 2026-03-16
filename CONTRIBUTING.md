# Contributing to pizzarr

## Development Setup

``` r
setwd("path/to/pizzarr")
install.packages("devtools")
devtools::install()
devtools::load_all()
```

## Testing

Tests run single-threaded (`Config/testthat/parallel: false` in
DESCRIPTION).

``` r
devtools::check()
devtools::test()
```

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
