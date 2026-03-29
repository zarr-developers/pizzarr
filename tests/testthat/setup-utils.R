if (requireNamespace("pbapply", quietly = TRUE)) {
  pbapply::pboptions(type = "none")
}
do.call(options, pizzarr_option_defaults)

withr::defer(do.call(options, pizzarr_option_defaults), teardown_env())
