setup({
  if (requireNamespace("pbapply", quietly = TRUE)) {
    pbapply::pboptions(type = "none")
  }
  do.call(options, pizzarr_option_defaults)
})
teardown({
  do.call(options, pizzarr_option_defaults)
})
