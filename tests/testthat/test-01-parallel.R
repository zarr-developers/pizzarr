library(pizzarr)

sample_dir <- tools::R_user_dir("pizzarr")
clean <- !dir.exists(sample_dir)

cache <- tryCatch(pizzarr_sample("dog.ome.zarr"), error = function(e) NULL)
has_dog_sample <- !is.null(cache)

SlowGettingDirectoryStore <- R6::R6Class("SlowGettingDirectoryStore",
  inherit = DirectoryStore,
  public = list(
    get_item = function(key) {
      if(.Platform$OS.type == "windows") {
        # windows has a lot of per process overhead
        Sys.sleep(1/5)
      }
      # Simulate a slow read such as an HTTP request.
      Sys.sleep(1/5)
      return(super$get_item(key))
    }
  )
)

SlowSettingDirectoryStore <- R6::R6Class("SlowSettingDirectoryStore",
  inherit = DirectoryStore,
  public = list(
    set_item = function(key, value) {
      if(.Platform$OS.type == "windows") {
        # windows has a lot of per process overhead
        Sys.sleep(1/5)
      }
      # Simulate a slow write such as an HTTP request.
      Sys.sleep(1/5)
      return(super$set_item(key, value))
    }
  )
)

get_dog_arr <- function(slow_setting = FALSE) {
  # The path to the root of the OME-NGFF Zarr store.

  root <- file.path(tempdir(), "dog.ome.zarr")
  
  file.copy(pizzarr_sample("dog.ome.zarr"), dirname(root), 
            recursive = TRUE)
  
  # Open the OME-NGFF as a DirectoryStore.
  if(slow_setting) {
    store <- SlowSettingDirectoryStore$new(root)
  } else {
    store <- SlowGettingDirectoryStore$new(root)
  }

  zarr_arr <- zarr_open(store = store, path = "/0")
  return(zarr_arr)
}

run_parallel_get <- function(num_workers) {
  options(pizzarr.parallel_backend = num_workers)
  options(pizzarr.progress_bar = FALSE)

  zarr_arr <- get_dog_arr()
  arr <- zarr_arr$get_item("...")$data

  options(pizzarr.parallel_backend = NA)

  return(sum(arr))
}


run_parallel_set <- function(num_workers) {
  options(pizzarr.parallel_write_enabled = TRUE)
  options(pizzarr.parallel_backend = num_workers)
  options(pizzarr.progress_bar = FALSE)

  zarr_arr <- get_dog_arr(slow_setting = TRUE)
  arr <- zarr_arr$get_item("...")$data

  # Set the contents of the array to be twice the original value.
  zarr_arr$set_item("...", arr * 2.0)

  doubled_arr <- zarr_arr$get_item("...")$data

  options(pizzarr.parallel_write_enabled = FALSE)
  options(pizzarr.parallel_backend = NA)

  return(sum(doubled_arr))
}

test_that("can run get_item() and set_item in parallel", {
  skip_if(!has_dog_sample, "dog.ome.zarr sample not available")
  skip_if_not_installed("bench")

  bench_df <- bench::mark(
    run_parallel_get(1),
    run_parallel_get(2),
    iterations = 1,
    memory = FALSE,
    filter_gc = FALSE
  )

  expect_equal(unlist(bench_df$result), rep(134538481, 2))

  testthat::skip_on_cran()
  testthat::skip_on_covr()
  testthat::skip_on_os("windows")
  # timing comparison is flaky on shared CRAN build machines

  expect_equal(bench_df$total_time[[1]] > bench_df$total_time[[2]], TRUE)

})

test_that("can run set_item() in parallel", {
  skip_if(!has_dog_sample, "dog.ome.zarr sample not available")
  skip_if_not_installed("bench")

  bench_df <- bench::mark(
    run_parallel_set(1),
    run_parallel_set(2),
    iterations = 1,
    memory = FALSE,
    filter_gc = FALSE
  )

  expect_equal(unlist(bench_df$result), rep(134538481*2.0, 2))

  testthat::skip_on_cran()
  testthat::skip_on_covr()
  testthat::skip_on_os("windows")
  # timing comparison is flaky on shared CRAN build machines

  expect_equal(bench_df$total_time[[1]] > bench_df$total_time[[2]], TRUE)
  
})

cl1 <- parallel::makeCluster(1)

test_that("parse_parallel_option works as expected", {
  expect_equal(parse_parallel_option(cl1), cl1)
  expect_equal(parse_parallel_option("future"), "future")
  expect_equal(parse_parallel_option("0"), FALSE)
  expect_equal(parse_parallel_option(0), FALSE)
  expect_equal(parse_parallel_option("FALSE"), FALSE)
  expect_equal(parse_parallel_option(FALSE), FALSE)
  expect_equal(parse_parallel_option("1"), TRUE)
  expect_equal(parse_parallel_option(1), TRUE)
  expect_equal(parse_parallel_option("TRUE"), TRUE)
  expect_equal(parse_parallel_option(TRUE), TRUE)
  expect_equal(parse_parallel_option("2"), 2)
  expect_equal(parse_parallel_option(2), 2)
})

test_that("is_truthy_parallel_option works as expected", {
  expect_equal(is_truthy_parallel_option(cl1), TRUE)
  expect_equal(is_truthy_parallel_option("future"), TRUE)
  expect_equal(is_truthy_parallel_option(FALSE), FALSE)
  expect_equal(is_truthy_parallel_option(0), FALSE)
  expect_equal(is_truthy_parallel_option(TRUE), TRUE)
  expect_equal(is_truthy_parallel_option(1), TRUE)
  expect_equal(is_truthy_parallel_option(2), TRUE)
})

test_that("get_parallel_settings", {
  # covr instruments production code by injecting counter calls into function
  # bodies, making body() comparison unreliable. Skip under covr; the parallel
  # get/set tests above already exercise the actual dispatch paths.
  testthat::skip_on_covr()
  skip_if_not_installed("pbapply")
  skip_if_not_installed("future.apply")

  expect_func_equal <- function(f1, f2) {
    expect_equal(format(f1), format(f2))
  }

  # Case 1: not parallel
  ps <- get_parallel_settings(parallel_option = NA)

  expect_func_equal(ps$apply_func,
                    function(X, FUN, ..., cl = NULL) {
                      lapply(X, FUN, ...)
                    })

  expect_equal(ps$cl, NA)

  # Case 2a1: Future, progress
  ps <- get_parallel_settings(parallel_option = "future",
                              progress = TRUE)

  expect_func_equal(ps$apply_func,
                    function(X, FUN, ..., cl = NULL) {
                      pbapply::pblapply(X, FUN, ...,
                                        future.globals = FALSE,
                                        future.packages = "blosc",
                                        future.seed = TRUE, cl = cl)
                    })

  expect_equal(ps$cl, "future")

  # Case 2a2: Future, no progress

  ps <- get_parallel_settings(parallel_option = "future",
                              progress = FALSE)

  expect_func_equal(ps$apply_func,
                    function(X, FUN, ..., cl = NULL) {
                      future.apply::future_lapply(X, FUN, ...,
                                                  future.globals = FALSE,
                                                  future.packages = "blosc",
                                                  future.seed=TRUE)
                    })

  expect_equal(ps$cl, "future")

  # Case 2b1: cl = integer > 1, windows, progress = TRUE

  ps <- get_parallel_settings(on_windows = TRUE,
                              parallel_option = 2,
                              progress = TRUE)

  expect_func_equal(ps$apply_func,
                    function(X, FUN, ..., cl = NULL) {
                      pbapply::pblapply(X, FUN, ..., cl = cl)
                    })

  expect_true(inherits(ps$cl, "cluster"))

  # Case 2b2: cl = 1, progress = TRUE, windows doesn't matter

  ps <- get_parallel_settings(parallel_option = 1,
                              progress = TRUE)

  expect_func_equal(ps$apply_func,
                    function(X, FUN, ..., cl = NULL) {
                      pbapply::pblapply(X, FUN, ..., cl = cl)
                    })

  expect_equal(ps$cl, NULL)

  # Case 2b3: cl = 2, progress = TRUE, not windows

  ps <- get_parallel_settings(on_windows = FALSE,
                              parallel_option = 2,
                              progress = TRUE)

  expect_func_equal(ps$apply_func,
                    function(X, FUN, ..., cl = NULL) {
                      pbapply::pblapply(X, FUN, ..., cl = cl)
                    })

  expect_equal(ps$cl, 2)

  # Case 2b4: cl = 2, not windows, progress

  ps <- get_parallel_settings(on_windows = FALSE,
                              parallel_option = 2,
                              progress = TRUE)

  expect_func_equal(ps$apply_func,
                    function(X, FUN, ..., cl = NULL) {
                      pbapply::pblapply(X, FUN, ..., cl = cl)
                    })

  expect_equal(ps$cl, 2)

  # case 2b5 cl = 1, no progress

  ps <- get_parallel_settings(parallel_option = 1,
                              progress = FALSE)

  expect_func_equal(ps$apply_func,
                    function(X, FUN, ..., cl = NULL) {
                      lapply(X, FUN, ...)
                    })

  expect_equal(ps$cl, NULL)

  # Case 2b6: cl = 2, no progress, on windows

  ps <- get_parallel_settings(on_windows = TRUE,
                              parallel_option = 2,
                              progress = FALSE)

  expect_func_equal(ps$apply_func,
                    function(X, FUN, ..., cl = NULL) {
                      parallel::parLapply(cl, X, FUN, ...)
                    })

  expect_true(inherits(ps$cl, "cluster"))

  # Case 2b7: cl = 2, no progress, not windows

  ps <- get_parallel_settings(on_windows = FALSE,
                              parallel_option = 2,
                              progress = FALSE)

  expect_func_equal(ps$apply_func,
                    function(X, FUN, ..., cl = NULL) {
                      parallel::mclapply(X, FUN, ..., mc.cores = cl)
                    })

  expect_equal(ps$cl, 2)

})

parallel::stopCluster(cl1)

if(clean) unlink(sample_dir, recursive = TRUE)
