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
