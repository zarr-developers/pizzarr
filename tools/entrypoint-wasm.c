/* Stub entrypoint for WebR/Wasm builds.
 *
 * The r-universe Wasm tier ships pizzarr as pure R — the zarrs Rust
 * backend is not compiled because several dependencies
 * (reqwest::blocking, tokio, rayon, object_store) are incompatible
 * with wasm32. No native routines to register here.
 *
 * is_zarrs_available() in R/zarrs-bridge.R catches the missing-symbol
 * error when extendr wrappers call .Call() and switches to the pure-R
 * path — same runtime behaviour as the CRAN tier.
 */
void R_init_pizzarr(void *dll) { (void)dll; }
