#' @keywords internal
char_vec_to_raw <- function(char_vec, basic_type, num_chars, byte_order) {
  # Reference: https://stat.ethz.ch/R-manual/R-devel/library/base/html/iconv.html
  if(basic_type == "S") {
    iconv_to <- "UTF-8"
    num_bytes_per_char <- 1
  } else if(basic_type == "U") {
    if(byte_order == "little") {
      iconv_to <- "UTF-32LE"
    } else if(byte_order == "big") {
      iconv_to <- "UTF-32BE"
    } else {
      stop("Got unexpected byte_order in char_vec_to_raw()")
    }
    num_bytes_per_char <- 4
  } else {
    stop("Unexpected basic_type in char_vec_to_raw()")
  }
  num_bytes <- num_chars * num_bytes_per_char

  list_of_raw <- iconv(char_vec, to = iconv_to, toRaw = TRUE)

  buf <- raw(length = length(list_of_raw) * num_bytes)

  for(i in seq_len(length(list_of_raw))) {
    raw_vec_i <- list_of_raw[[i]]

    if(length(raw_vec_i) > num_bytes) {
      stop("Unexpected length of raw_vec_i in char_vec_to_raw(): string probably too long for specified dtype")
    }

    # "" and NA encode to zero bytes; leave the slot null-filled, which
    # raw_to_char_vec() reads back as "".
    if(length(raw_vec_i) == 0) next

    offset_i_start <- (i-1) * num_bytes + 1
    offset_i_stop <- offset_i_start + length(raw_vec_i) - 1

    # Byte order is already handled by iconv via UTF-32LE/UTF-32BE selection above.
    buf[offset_i_start:offset_i_stop] <- raw_vec_i
  }

  return(buf)
}

#' @keywords internal
raw_to_char_vec <- function(raw_vec, basic_type, num_chars, byte_order) {
  # Reference: https://stat.ethz.ch/R-manual/R-devel/library/base/html/iconv.html
  if(basic_type == "S") {
    iconv_from <- "UTF-8"
    num_bytes_per_char <- 1
  } else if(basic_type == "U") {
    if(byte_order == "little") {
      iconv_from <- "UTF-32LE"
    } else if(byte_order == "big") {
      iconv_from <- "UTF-32BE"
    } else {
      stop("Got unexpected byte_order in raw_to_char_vec()")
    }
    num_bytes_per_char <- 4
  } else {
    stop("Unexpected basic_type in raw_to_char_vec()")
  }
  num_bytes <- num_chars * num_bytes_per_char

  null_byte <- as.raw(rep(0x00, times = num_bytes_per_char))

  list_of_raw <- list()
  for(i in seq_len(length(raw_vec) / num_bytes)) {

    offset_i_start <- (i-1) * num_bytes + 1
    offset_i_stop <- offset_i_start + num_bytes - 1

    # We get the raw vector for each string in the array.
    raw_vec_i <- raw_vec[offset_i_start:offset_i_stop]

    if(num_bytes_per_char == 1) {
      # Since every character only uses one byte, we can simply remove all null bytes.
      raw_vec_i <- raw_vec_i[raw_vec_i != null_byte]
    } else {
      # Since each character uses multiple bytes, we need to remove only those characters
      # for which _all_ bytes are null bytes.

      # Convert the raw vector to an int32 vector and remove zeros, then convert back to raw,
      # so that we only remove characters that contain all null bytes.
      int_vec_i <- readBin(
        con = raw_vec_i,
        what = "integer",
        size = num_bytes_per_char,
        n = num_chars,
        signed = TRUE,
        endian = byte_order
      )
      int_vec_i <- int_vec_i[int_vec_i != 0]
      raw_vec_i <- writeBin(
        object = int_vec_i,
        con = raw(),
        size = num_bytes_per_char,
        endian = byte_order
      )
    }

    # We append the raw vector for this string to the list of raw vectors.
    # This is the input format required by `iconv`.
    list_of_raw <- append(list_of_raw, list(raw_vec_i))
  }
  char_vec <- iconv(list_of_raw, from = iconv_from, to = "UTF-8", toRaw = FALSE)
  return(char_vec)
}
