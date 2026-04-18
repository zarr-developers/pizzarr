#' @keywords internal
.onLoad <- function(libname = NULL, pkgname = NULL) {
  init_options()
  .pizzarr_env$zarrs_available <- is_zarrs_available()
  if (.pizzarr_env$zarrs_available) {
    apply_zarrs_config()
  }
}

#' @keywords internal
.onAttach <- function(libname, pkgname) {
  if (!.pizzarr_env$zarrs_available &&
      getOption("pizzarr.suggest_runiverse", TRUE)) {
    packageStartupMessage(
      "pizzarr: running without zarrs backend (pure R).\n",
      "For parallel I/O and cloud storage, install from r-universe:\n",
      '  install.packages("pizzarr", ',
      'repos = "https://zarr-developers.r-universe.dev")'
    )
  }
}
