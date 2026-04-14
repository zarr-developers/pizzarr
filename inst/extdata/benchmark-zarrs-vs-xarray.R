# Benchmark pizzarr (R-native and zarrs Rust backend) vs xarray (Python).
#
# Modeled after zarrs/zarr_benchmarks: 3D arrays, multiple codecs,
# read-all / chunk / subset / write scenarios, throughput in MB/s.
#
# Prerequisites: Python 3.10+ with zarr>=3, xarray, numpy, numcodecs.
# Skips xarray benchmarks gracefully if Python is unavailable.
#
# Environment variables:
#   PIZZARR_BENCH_ITERS  — iterations per scenario (default 3)
#   PIZZARR_BENCH_CSV    — path to save full results CSV (optional)
#
# Usage:
#   Rscript inst/extdata/benchmark-zarrs-vs-xarray.R

# ---------------------------------------------------------------------------
# 0. Package loading
# ---------------------------------------------------------------------------
if (file.exists("DESCRIPTION") &&
    any(grepl("^Package: pizzarr", readLines("DESCRIPTION", n = 5)))) {
  devtools::load_all(".")
} else {
  library(pizzarr)
}

has_zarrs <- .pizzarr_env$zarrs_available

# ---------------------------------------------------------------------------
# 1. Python detection
# ---------------------------------------------------------------------------
python_cmd <- Sys.which("python")
if (nchar(python_cmd) == 0) python_cmd <- Sys.which("python3")
has_python <- nchar(python_cmd) > 0

if (has_python) {
  zarr_ver <- tryCatch(
    system2(python_cmd, c("-c", shQuote("import zarr; print(zarr.__version__)")),
            stdout = TRUE, stderr = TRUE),
    error = function(e) NULL
  )
  xr_ver <- tryCatch(
    system2(python_cmd, c("-c", shQuote("import xarray; print(xarray.__version__)")),
            stdout = TRUE, stderr = TRUE),
    error = function(e) NULL
  )
  if (is.null(zarr_ver) || !is.null(attr(zarr_ver, "status")) ||
      is.null(xr_ver) || !is.null(attr(xr_ver, "status"))) {
    message("Python found but zarr/xarray/numpy not installed. ",
            "Skipping xarray benchmarks.")
    message("Install via: pip install 'zarr>=3' xarray numpy numcodecs")
    has_python <- FALSE
  } else {
    cat(sprintf("Python: zarr %s, xarray %s (%s)\n",
                zarr_ver, xr_ver, python_cmd))
  }
} else {
  message("Python not found on PATH. Skipping xarray benchmarks.")
}

# Locate companion Python script
script_dir <- if (file.exists("inst/extdata/benchmark-xarray.py")) {
  normalizePath("inst/extdata", winslash = "/")
} else {
  normalizePath(system.file("extdata", package = "pizzarr"), winslash = "/")
}
py_bench_script <- file.path(script_dir, "benchmark-xarray.py")

if (has_python && !file.exists(py_bench_script)) {
  message("Cannot find benchmark-xarray.py in ", script_dir)
  has_python <- FALSE
}

cat(sprintf("R: pizzarr %s | zarrs backend: %s\n",
            packageVersion("pizzarr"),
            if (has_zarrs) "available" else "not available"))

# ---------------------------------------------------------------------------
# 2. Configuration
# ---------------------------------------------------------------------------
n_iter <- as.integer(Sys.getenv("PIZZARR_BENCH_ITERS", "3"))
cat(sprintf("Iterations per scenario: %d\n", n_iter))

configs <- list(
  list(
    name    = "small_none",
    shape   = c(100L, 100L, 100L),
    chunks  = c(100L, 100L, 100L),
    dtype   = "float64",
    dtype_r = "<f8",
    codec   = "none",
    size_mb = 100 * 100 * 100 * 8 / 1e6
  ),
  list(
    name    = "small_gzip",
    shape   = c(100L, 100L, 100L),
    chunks  = c(100L, 100L, 100L),
    dtype   = "float64",
    dtype_r = "<f8",
    codec   = "gzip",
    size_mb = 100 * 100 * 100 * 8 / 1e6
  ),
  list(
    name    = "medium_none",
    shape   = c(500L, 500L, 100L),
    chunks  = c(100L, 100L, 50L),
    dtype   = "float64",
    dtype_r = "<f8",
    codec   = "none",
    size_mb = 500 * 500 * 100 * 8 / 1e6
  ),
  list(
    name    = "medium_gzip",
    shape   = c(500L, 500L, 100L),
    chunks  = c(100L, 100L, 50L),
    dtype   = "float64",
    dtype_r = "<f8",
    codec   = "gzip",
    size_mb = 500 * 500 * 100 * 8 / 1e6
  )
)

# Per-config selection coordinates (1-based inclusive for R's slice())
chunk_sels <- list(
  small_none   = list(slice(1L, 100L), slice(1L, 100L), slice(1L, 100L)),
  small_gzip   = list(slice(1L, 100L), slice(1L, 100L), slice(1L, 100L)),
  medium_none  = list(slice(1L, 100L), slice(1L, 100L), slice(1L, 50L)),
  medium_gzip  = list(slice(1L, 100L), slice(1L, 100L), slice(1L, 50L))
)

subset_sels <- list(
  small_none   = list(slice(26L, 75L), slice(26L, 75L), slice(26L, 75L)),
  small_gzip   = list(slice(26L, 75L), slice(26L, 75L), slice(26L, 75L)),
  medium_none  = list(slice(51L, 250L), slice(51L, 250L), slice(11L, 60L)),
  medium_gzip  = list(slice(51L, 250L), slice(51L, 250L), slice(11L, 60L))
)

# Uncompressed sizes for subset/chunk throughput calculations
chunk_sizes_mb <- list(
  small_none  = 100 * 100 * 100 * 8 / 1e6,
  small_gzip  = 100 * 100 * 100 * 8 / 1e6,
  medium_none = 100 * 100 * 50 * 8 / 1e6,
  medium_gzip = 100 * 100 * 50 * 8 / 1e6
)

subset_sizes_mb <- list(
  small_none  = 50 * 50 * 50 * 8 / 1e6,
  small_gzip  = 50 * 50 * 50 * 8 / 1e6,
  medium_none = 200 * 200 * 50 * 8 / 1e6,
  medium_gzip = 200 * 200 * 50 * 8 / 1e6
)

make_compressor <- function(codec) {
  switch(codec,
    gzip = GzipCodec$new(),  # default level 6 (R-native only supports level 6)
    none = NA,
    stop("Unknown codec: ", codec)
  )
}

# ---------------------------------------------------------------------------
# 3. Working directory and data generation
# ---------------------------------------------------------------------------
work_dir <- tempfile("pizzarr_bench_")
dir.create(work_dir)
on.exit(unlink(work_dir, recursive = TRUE), add = TRUE)
cat("Working directory:", work_dir, "\n")

configs_json <- jsonlite::toJSON(configs, auto_unbox = TRUE)

if (has_python) {
  cat("\n--- Generating test stores via Python ---\n")
  py_work <- normalizePath(work_dir, winslash = "/")
  gen_out <- system2(python_cmd,
                     c(py_bench_script, "generate",
                       shQuote(py_work), shQuote(configs_json)),
                     stdout = TRUE, stderr = TRUE)
  cat(paste(gen_out, collapse = "\n"), "\n")
  if (!is.null(attr(gen_out, "status")) && attr(gen_out, "status") != 0) {
    stop("Python data generation failed:\n", paste(gen_out, collapse = "\n"))
  }
} else {
  # Generate stores with pizzarr so R benchmarks can still run
  cat("\n--- Generating test stores via pizzarr ---\n")
  for (cfg in configs) {
    store_path <- file.path(work_dir, paste0("data_", cfg$name, ".zarr"))
    dir.create(store_path)
    store <- DirectoryStore$new(store_path)
    root <- zarr_create_group(store = store, zarr_format = 2L)
    compressor <- make_compressor(cfg$codec)
    arr <- root$create_dataset(
      name       = "data",
      shape      = cfg$shape,
      chunks     = cfg$chunks,
      dtype      = cfg$dtype_r,
      fill_value = 0,
      compressor = compressor
    )
    set.seed(42)
    data_vec <- runif(prod(cfg$shape))
    arr$set_item("...", array(data_vec, dim = cfg$shape))
    cat(sprintf("  Generated %s: %s (%.1f MB)\n",
                cfg$name, store_path, cfg$size_mb))
  }
}

# ---------------------------------------------------------------------------
# 4. Results collector
# ---------------------------------------------------------------------------
results <- data.frame(
  scenario       = character(),
  array_config   = character(),
  codec          = character(),
  implementation = character(),
  iteration      = integer(),
  time_seconds   = double(),
  throughput_mb_s = double(),
  stringsAsFactors = FALSE
)

add_result <- function(scenario, config_name, codec, impl, iter, elapsed,
                       size_mb) {
  results <<- rbind(results, data.frame(
    scenario        = scenario,
    array_config    = config_name,
    codec           = codec,
    implementation  = impl,
    iteration       = iter,
    time_seconds    = round(elapsed, 6),
    throughput_mb_s = round(size_mb / elapsed, 2),
    stringsAsFactors = FALSE
  ))
}

# ---------------------------------------------------------------------------
# 5. R benchmark helpers
# ---------------------------------------------------------------------------

clear_store_cache <- function(store_path) {
  if (has_zarrs) {
    try(zarrs_close_store(normalizePath(store_path, winslash = "/")),
        silent = TRUE)
  }
}

bench_read_all <- function(store_path, cfg, impl_name) {
  cat(sprintf("    %s / read_all / %s\n", cfg$name, impl_name))
  for (i in seq_len(n_iter)) {
    clear_store_cache(store_path)
    t0 <- proc.time()
    z <- zarr_open(store_path, mode = "r")
    arr <- z$get_item("data")
    val <- arr$get_item("...")$data
    elapsed <- (proc.time() - t0)[["elapsed"]]
    add_result("read_all", cfg$name, cfg$codec, impl_name, i,
               elapsed, cfg$size_mb)
  }
}

bench_read_chunk <- function(store_path, cfg, impl_name) {
  cat(sprintf("    %s / read_chunk / %s\n", cfg$name, impl_name))
  sel <- chunk_sels[[cfg$name]]
  mb <- chunk_sizes_mb[[cfg$name]]
  for (i in seq_len(n_iter)) {
    clear_store_cache(store_path)
    t0 <- proc.time()
    z <- zarr_open(store_path, mode = "r")
    arr <- z$get_item("data")
    val <- arr$get_item(sel)$data
    elapsed <- (proc.time() - t0)[["elapsed"]]
    add_result("read_chunk", cfg$name, cfg$codec, impl_name, i, elapsed, mb)
  }
}

bench_read_subset <- function(store_path, cfg, impl_name) {
  cat(sprintf("    %s / read_subset / %s\n", cfg$name, impl_name))
  sel <- subset_sels[[cfg$name]]
  mb <- subset_sizes_mb[[cfg$name]]
  for (i in seq_len(n_iter)) {
    clear_store_cache(store_path)
    t0 <- proc.time()
    z <- zarr_open(store_path, mode = "r")
    arr <- z$get_item("data")
    val <- arr$get_item(sel)$data
    elapsed <- (proc.time() - t0)[["elapsed"]]
    add_result("read_subset", cfg$name, cfg$codec, impl_name, i, elapsed, mb)
  }
}

bench_write_all <- function(cfg, impl_name) {
  cat(sprintf("    %s / write_all / %s\n", cfg$name, impl_name))
  set.seed(42)
  data_array <- array(runif(prod(cfg$shape)), dim = cfg$shape)
  compressor <- make_compressor(cfg$codec)
  for (i in seq_len(n_iter)) {
    out_path <- file.path(work_dir,
                          paste0("write_r_", impl_name, "_",
                                 cfg$name, "_", i, ".zarr"))
    dir.create(out_path)
    store <- DirectoryStore$new(out_path)
    t0 <- proc.time()
    root <- zarr_create_group(store = store, zarr_format = 2L)
    arr <- root$create_dataset(
      name       = "data",
      shape      = cfg$shape,
      chunks     = cfg$chunks,
      dtype      = cfg$dtype_r,
      fill_value = 0,
      compressor = compressor
    )
    arr$set_item("...", data_array)
    elapsed <- (proc.time() - t0)[["elapsed"]]
    clear_store_cache(out_path)
    add_result("write_all", cfg$name, cfg$codec, impl_name, i,
               elapsed, cfg$size_mb)
  }
}

# ---------------------------------------------------------------------------
# 6. Run R benchmarks
# ---------------------------------------------------------------------------
cat("\n--- R benchmarks ---\n")

for (cfg in configs) {
  store_path <- file.path(work_dir, paste0("data_", cfg$name, ".zarr"))

  if (!dir.exists(store_path)) {
    cat(sprintf("  SKIP %s: store not found at %s\n", cfg$name, store_path))
    next
  }

  # -- pizzarr with zarrs backend --
  if (has_zarrs) {
    cat(sprintf("  pizzarr_zarrs: %s\n", cfg$name))
    bench_read_all(store_path, cfg, "pizzarr_zarrs")
    bench_read_chunk(store_path, cfg, "pizzarr_zarrs")
    bench_read_subset(store_path, cfg, "pizzarr_zarrs")
    bench_write_all(cfg, "pizzarr_zarrs")
    gc()
  }

  # -- pizzarr R-native (zarrs disabled) --
  cat(sprintf("  pizzarr_rnative: %s\n", cfg$name))
  orig_zarrs <- .pizzarr_env$zarrs_available
  .pizzarr_env$zarrs_available <- FALSE
  tryCatch({
    bench_read_all(store_path, cfg, "pizzarr_rnative")
    bench_read_chunk(store_path, cfg, "pizzarr_rnative")
    bench_read_subset(store_path, cfg, "pizzarr_rnative")
    bench_write_all(cfg, "pizzarr_rnative")
  }, finally = {
    .pizzarr_env$zarrs_available <- orig_zarrs
  })
  gc()
}

# ---------------------------------------------------------------------------
# 7. Run xarray benchmarks
# ---------------------------------------------------------------------------
if (has_python) {
  cat("\n--- xarray benchmarks ---\n")
  py_work <- normalizePath(work_dir, winslash = "/")
  py_out <- system2(python_cmd,
                    c(py_bench_script, "bench",
                      shQuote(py_work), shQuote(configs_json),
                      as.character(n_iter)),
                    stdout = TRUE, stderr = TRUE)

  # Last line of stdout is the JSON results; everything else is stderr progress
  # system2 with stdout=TRUE captures both (stderr merged when stderr=TRUE too)
  # Parse each line: JSON is the one that starts with "["
  json_line <- grep("^\\[", py_out, value = TRUE)
  stderr_lines <- grep("^\\[", py_out, value = TRUE, invert = TRUE)
  if (length(stderr_lines) > 0) cat(paste(stderr_lines, collapse = "\n"), "\n")

  if (length(json_line) == 1) {
    py_results <- jsonlite::fromJSON(json_line)
    results <- rbind(results, py_results)
    cat(sprintf("  Parsed %d xarray results\n", nrow(py_results)))
  } else {
    message("WARNING: Could not parse xarray benchmark results.")
    if (length(py_out) > 0) cat(paste(py_out, collapse = "\n"), "\n")
  }
}

# ---------------------------------------------------------------------------
# 8. Summary
# ---------------------------------------------------------------------------
cat("\n")
cat("=============================================================\n")
cat("  pizzarr vs xarray Benchmark Results\n")
cat("=============================================================\n")
cat(sprintf("R: pizzarr %s | zarrs: %s\n",
            packageVersion("pizzarr"),
            if (has_zarrs) "compiled" else "not available"))
if (has_python) {
  cat(sprintf("Python: zarr %s, xarray %s\n", zarr_ver, xr_ver))
}
cat(sprintf("Iterations: %d | Metric: min time, throughput at min time\n\n",
            n_iter))

if (nrow(results) == 0) {
  cat("No results collected.\n")
  quit(status = 1)
}

# Compute summary: min and median per group
agg <- do.call(rbind, lapply(
  split(results, list(results$scenario, results$array_config,
                      results$codec, results$implementation)),
  function(g) {
    if (nrow(g) == 0) return(NULL)
    data.frame(
      scenario       = g$scenario[1],
      array_config   = g$array_config[1],
      codec          = g$codec[1],
      implementation = g$implementation[1],
      min_s          = round(min(g$time_seconds), 4),
      median_s       = round(median(g$time_seconds), 4),
      max_throughput  = round(max(g$throughput_mb_s), 1),
      stringsAsFactors = FALSE
    )
  }
))
rownames(agg) <- NULL

# Sort for readable output
agg <- agg[order(agg$scenario, agg$array_config, agg$codec,
                 agg$implementation), ]

cat(sprintf("%-14s %-14s %-6s %-18s %8s %8s %12s\n",
            "Scenario", "Config", "Codec", "Implementation",
            "Min(s)", "Med(s)", "MB/s"))
cat(paste(rep("-", 86), collapse = ""), "\n")
for (i in seq_len(nrow(agg))) {
  r <- agg[i, ]
  cat(sprintf("%-14s %-14s %-6s %-18s %8.4f %8.4f %12.1f\n",
              r$scenario, r$array_config, r$codec, r$implementation,
              r$min_s, r$median_s, r$max_throughput))
}

# Optional CSV export
csv_path <- Sys.getenv("PIZZARR_BENCH_CSV", "")
if (nchar(csv_path) > 0) {
  write.csv(results, csv_path, row.names = FALSE)
  cat(sprintf("\nFull results saved to: %s\n", csv_path))
}

cat("\nDone.\n")
