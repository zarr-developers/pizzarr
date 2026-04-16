//! C-order ↔ F-order transpose for nD arrays.
//!
//! zarrs stores and returns data in C-order (row-major). R uses
//! F-order (column-major). These functions permute a flat data vector
//! between the two orderings so the conversion happens in Rust —
//! eliminating the two-copy `array() + aperm()` pattern on the R side.
//!
//! For 0D or 1D arrays, the data is returned unchanged (both orderings
//! are identical for a single dimension).
//!
//! The implementation uses nested-loop iteration in output order to
//! ensure sequential writes. For 2D arrays, a cache-blocked algorithm
//! is used to keep both read and write access patterns cache-friendly.

/// Block size for tiled 2D transpose. Chosen to fit two BxB blocks
/// of f64 (2 * 64 * 64 * 8 = 64 KiB) comfortably in L1 cache.
const BLOCK_SIZE: usize = 64;

/// Transpose a flat vector from C-order to F-order.
///
/// Given `data` laid out in C-order (last index varies fastest) for an
/// array with the given `shape`, returns a new vector laid out in
/// F-order (first index varies fastest).
///
/// For 0D or 1D shapes, returns a copy of the input unchanged.
pub(crate) fn c_to_f_order<T: Copy>(data: &[T], shape: &[u64]) -> Vec<T> {
    let ndim = shape.len();

    if ndim <= 1 {
        return data.to_vec();
    }

    if ndim == 2 {
        return transpose_2d(data, shape[0] as usize, shape[1] as usize);
    }

    // nD: collapse to 2D transpose using the first dimension vs the rest.
    // C-order [D0, D1, ..., Dn-1]: view as D0 rows × (D1*...*Dn-1) cols.
    // F-order wants first dim to vary fastest, which is equivalent to
    // transposing this 2D view... but only for the outermost dimension.
    //
    // For general nD, use nested-loop approach iterating in F-order
    // (output order) to get sequential writes.
    transpose_nd_c_to_f(data, shape)
}

/// Transpose a flat vector from F-order to C-order.
///
/// Given `data` laid out in F-order (first index varies fastest) for an
/// array with the given `shape`, returns a new vector laid out in
/// C-order (last index varies fastest).
///
/// For 0D or 1D shapes, returns a copy of the input unchanged.
pub(crate) fn f_to_c_order<T: Copy>(data: &[T], shape: &[u64]) -> Vec<T> {
    let ndim = shape.len();

    if ndim <= 1 {
        return data.to_vec();
    }

    if ndim == 2 {
        // F→C of shape [R, C] is the same as C→F of shape [C, R].
        // In F-order, shape [R,C] has R varying fastest → it's a CxR matrix
        // stored in row-major. Transposing that CxR matrix gives RxC row-major
        // which is C-order [R, C].
        return transpose_2d(data, shape[1] as usize, shape[0] as usize);
    }

    transpose_nd_f_to_c(data, shape)
}

/// Cache-blocked 2D matrix transpose.
///
/// Input: `rows × cols` matrix in row-major order.
/// Output: `cols × rows` matrix in row-major order (i.e., transposed).
///
/// Processes BLOCK_SIZE × BLOCK_SIZE tiles to keep both source and
/// destination accesses within L1/L2 cache. Within each tile, the
/// inner loop iterates over source rows (stride = cols in source,
/// stride = 1 in destination), giving sequential writes per tile.
fn transpose_2d<T: Copy>(data: &[T], rows: usize, cols: usize) -> Vec<T> {
    let n = rows * cols;
    let mut out = Vec::with_capacity(n);
    // SAFETY: every index in 0..n is written exactly once.
    unsafe { out.set_len(n) };

    // Process in BLOCK_SIZE × BLOCK_SIZE tiles.
    let mut r0 = 0;
    while r0 < rows {
        let r1 = (r0 + BLOCK_SIZE).min(rows);
        let mut c0 = 0;
        while c0 < cols {
            let c1 = (c0 + BLOCK_SIZE).min(cols);

            // Transpose the tile [r0..r1, c0..c1].
            for r in r0..r1 {
                let src_row_start = r * cols;
                for c in c0..c1 {
                    // Source: row-major (r, c) → r * cols + c
                    // Dest: transposed row-major (c, r) → c * rows + r
                    out[c * rows + r] = data[src_row_start + c];
                }
            }

            c0 = c1;
        }
        r0 = r1;
    }

    out
}

/// nD C→F transpose using nested iteration in F-order (output order).
///
/// Iterates through all output positions sequentially (cache-friendly
/// writes). For each F-order position, computes the corresponding
/// C-order source position and copies the element. The source access
/// pattern is strided but reads are more cache-friendly than scattered
/// writes due to hardware prefetching.
fn transpose_nd_c_to_f<T: Copy>(data: &[T], shape: &[u64]) -> Vec<T> {
    let ndim = shape.len();
    let n = data.len();
    let mut out = Vec::with_capacity(n);
    // SAFETY: every index in 0..n is written exactly once.
    unsafe { out.set_len(n) };

    // Precompute C-order strides (product of trailing dimensions).
    let mut c_strides = vec![1usize; ndim];
    for i in (0..ndim - 1).rev() {
        c_strides[i] = c_strides[i + 1] * shape[i + 1] as usize;
    }

    // Iterate in F-order: dimension 0 varies fastest, dimension ndim-1 slowest.
    // Use a coordinate vector that we increment manually.
    let mut coords = vec![0usize; ndim];
    let mut c_idx = 0usize; // tracks the C-order index via incremental updates

    for f_idx in 0..n {
        out[f_idx] = data[c_idx];

        // Increment coords in F-order (dim 0 first).
        // Update c_idx incrementally to avoid recomputing from scratch.
        for d in 0..ndim {
            coords[d] += 1;
            c_idx += c_strides[d];
            if coords[d] < shape[d] as usize {
                break;
            }
            // This dimension rolled over: reset and carry.
            c_idx -= coords[d] * c_strides[d];
            coords[d] = 0;
        }
    }

    out
}

/// nD F→C transpose using nested iteration in C-order (output order).
///
/// Iterates through all output positions sequentially (cache-friendly
/// writes). For each C-order position, computes the corresponding
/// F-order source position and copies the element.
fn transpose_nd_f_to_c<T: Copy>(data: &[T], shape: &[u64]) -> Vec<T> {
    let ndim = shape.len();
    let n = data.len();
    let mut out = Vec::with_capacity(n);
    // SAFETY: every index in 0..n is written exactly once.
    unsafe { out.set_len(n) };

    // Precompute F-order strides (product of leading dimensions).
    let mut f_strides = vec![1usize; ndim];
    for i in 1..ndim {
        f_strides[i] = f_strides[i - 1] * shape[i - 1] as usize;
    }

    // Iterate in C-order: dimension ndim-1 varies fastest, dimension 0 slowest.
    // Use a coordinate vector that we increment manually.
    let mut coords = vec![0usize; ndim];
    let mut f_idx = 0usize; // tracks the F-order index via incremental updates

    for c_idx in 0..n {
        out[c_idx] = data[f_idx];

        // Increment coords in C-order (last dim first).
        // Update f_idx incrementally to avoid recomputing from scratch.
        for d in (0..ndim).rev() {
            coords[d] += 1;
            f_idx += f_strides[d];
            if coords[d] < shape[d] as usize {
                break;
            }
            // This dimension rolled over: reset and carry.
            f_idx -= coords[d] * f_strides[d];
            coords[d] = 0;
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_1d_no_change() {
        let data = vec![1, 2, 3, 4, 5];
        assert_eq!(c_to_f_order(&data, &[5]), data);
        assert_eq!(f_to_c_order(&data, &[5]), data);
    }

    #[test]
    fn test_2d_transpose() {
        // 2x3 matrix in C-order: [[1,2,3],[4,5,6]]
        let c_data = vec![1, 2, 3, 4, 5, 6];
        // F-order: column-major: [1,4,2,5,3,6]
        let f_data = vec![1, 4, 2, 5, 3, 6];
        let shape = [2, 3];

        assert_eq!(c_to_f_order(&c_data, &shape), f_data);
        assert_eq!(f_to_c_order(&f_data, &shape), c_data);
    }

    #[test]
    fn test_2d_large_blocked() {
        // Larger than BLOCK_SIZE to exercise tiling.
        let rows = 100u64;
        let cols = 150u64;
        let c_data: Vec<i32> = (0..(rows * cols) as i32).collect();
        let shape = [rows, cols];

        let f_data = c_to_f_order(&c_data, &shape);
        let roundtrip = f_to_c_order(&f_data, &shape);
        assert_eq!(roundtrip, c_data);

        // Spot-check: element at [2, 3] in C-order is 2*150+3=303.
        // In F-order it's at position 2+3*100=302.
        assert_eq!(f_data[302], 303);
    }

    #[test]
    fn test_3d_transpose() {
        // 2x3x4 array in C-order: elements 0..23
        let c_data: Vec<i32> = (0..24).collect();
        let shape = [2u64, 3, 4];

        let f_data = c_to_f_order(&c_data, &shape);
        // Round-trip: F→C should give back the original.
        let roundtrip = f_to_c_order(&f_data, &shape);
        assert_eq!(roundtrip, c_data);

        // Verify specific elements. C-order index [i,j,k] = i*12 + j*4 + k.
        // F-order index [i,j,k] = i + j*2 + k*6.
        // Element at [0,1,2] in C-order: 0*12+1*4+2 = 6, in F-order: 0+1*2+2*6 = 14.
        assert_eq!(f_data[14], c_data[6]); // [0,1,2]
    }

    #[test]
    fn test_3d_large() {
        // 50x60x70 — large enough to test cache behavior
        let shape = [50u64, 60, 70];
        let n = 50 * 60 * 70;
        let c_data: Vec<i32> = (0..n as i32).collect();

        let f_data = c_to_f_order(&c_data, &shape);
        let roundtrip = f_to_c_order(&f_data, &shape);
        assert_eq!(roundtrip, c_data);
    }

    #[test]
    fn test_scalar_no_change() {
        let data = vec![42.0f64];
        assert_eq!(c_to_f_order(&data, &[1, 1]), data);
        assert_eq!(f_to_c_order(&data, &[1, 1]), data);
    }

    #[test]
    fn test_empty() {
        let data: Vec<f64> = vec![];
        assert_eq!(c_to_f_order(&data, &[0]), data);
    }

    #[test]
    fn test_4d_roundtrip() {
        let shape = [2u64, 3, 4, 5];
        let n: usize = shape.iter().map(|&s| s as usize).product();
        let c_data: Vec<i32> = (0..n as i32).collect();

        let f_data = c_to_f_order(&c_data, &shape);
        let roundtrip = f_to_c_order(&f_data, &shape);
        assert_eq!(roundtrip, c_data);
    }
}
