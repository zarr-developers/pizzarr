<br/>
Pizzarr is an R implementation for [Zarr](https://zarr.readthedocs.io/).

## Getting Started

The ``pizzarr`` package includes:

* Support for reading and writing Zarr Arrays and Groups
* Support for numeric, logical, and string Zarr data types
* Support for both R-like one-based slicing and Python-like zero-based slicing
* Familiar API based on [zarr-python](https://github.com/zarr-developers/zarr-python) and [zarr.js](https://github.com/gzuidhof/zarr.js)
* R-based and [R6](https://github.com/r-lib/R6/)-based implementation for ease of maintenance


## Installation

Installation requires R 4.0.0 or greater.

```r
install.packages("devtools")
devtools::install_github("zarr-developers/pizzarr")
```

## Examples


```r
library(pizzarr)

a <- array(data=1:20, dim=c(2, 10))
#      [,1] [,2] [,3] [,4] [,5] [,6] [,7] [,8] [,9] [,10]
# [1,]    1    3    5    7    9   11   13   15   17    19
# [2,]    2    4    6    8   10   12   14   16   18    20
z <- zarr_create(shape=dim(a), dtype="<f4", fill_value=NA)

z$set_item("...", a)

selection <- z$get_item(list(slice(1, 2), slice(1, 5)))

print(selection$data)
#      [,1] [,2] [,3] [,4] [,5]
# [1,]    1    3    5    7    9
# [2,]    2    4    6    8   10
```
