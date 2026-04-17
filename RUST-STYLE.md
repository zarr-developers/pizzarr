# Rust style guide for pizzarr

Conventions for the extendr/zarrs bridge code. Patterns are drawn from the zarrs
monorepo, zarrs_ffi, and the official rextendr documentation. A zarrs contributor
reading pizzarr's Rust should encounter no surprises.

## Toolchain and lints

**rustfmt.toml:**
```toml
imports_granularity = "Module"
```

**Workspace lints** (match zarrs):
```toml
[lints.rust]
unused_variables = "warn"
dead_code = "warn"
missing_docs = "warn"
unreachable_pub = "warn"
unsafe_op_in_unsafe_fn = "warn"

[lints.clippy]
pedantic = { level = "warn", priority = -1 }
module_name_repetitions = "allow"
missing_panics_doc = "warn"
missing_errors_doc = "warn"
```

Run `cargo clippy --all-features -- -D warnings` before every commit.

## Naming

Standard Rust conventions (zarrs follows these without exception):

- **Types**: `UpperCamelCase` -- `PizzarrError`, `StoreCache`, `TokioBlockOn`
- **Functions/methods**: `snake_case` -- `retrieve_subset`, `open_store`
- **Constants**: `SCREAMING_SNAKE_CASE` -- `STORE_CACHE`, `TOKIO_RT`
- **Modules**: `snake_case` -- `store_cache`, `error`, `dtype_dispatch`
- **Feature flags**: `snake_case` -- `cran`, `full`, `s3`, `gcs`
- **Enum variants**: `UpperCamelCase` -- `StoreOpen`, `ArrayOpen`

Error enum variants named after the operation that fails:
`PizzarrError::StoreOpen(...)`, `PizzarrError::Retrieve(...)`.

## Error handling

Use `thiserror`. Every variant gets `#[error("...")]` with context. Convert to
`extendr_api::Error` via `From`:

```rust
impl From<PizzarrError> for extendr_api::Error {
    fn from(err: PizzarrError) -> Self {
        extendr_api::Error::Other(err.to_string())
    }
}
```

Every error message names what failed and where.

## Doc comments

- `///` for items, `//!` for modules.
- First line: imperative mood summary ("Create a new store cache.")
- `# Errors` and `# Panics` sections where applicable.
- `#[must_use]` on pure accessors. `const fn` where possible.

## Visibility

Only `pub` items: `#[extendr]`-annotated functions and `extendr_module!`.
Everything else `pub(crate)` or private.

## Feature-flag patterns

Gate individual items, not entire modules:
```rust
#[cfg(feature = "full")]
use zarrs_object_store::AsyncObjectStore;
```

## Imports

Group by origin: std, external crates, crate-internal. `cargo fmt` handles
ordering per `imports_granularity = "Module"`.

## Testing

Unit tests in `#[cfg(test)] mod tests` at the bottom of each file. Fixtures use
`zarrs::storage::store::MemoryStore`. Descriptive names:
`fn retrieve_subset_f64()`, `fn error_on_missing_array()`.

Note: Rust unit tests cannot be run via `cargo test` directly (build.rs
requires R environment variables). They compile only through
`rextendr::document()` or `R CMD INSTALL`.

## Unsafe code

Zero `unsafe` except the `set_len` calls in `transpose.rs` (each index
written exactly once). Every `unsafe` block gets `// SAFETY:` comments.

## Build pipeline

Scaffolded via `rextendr::use_extendr()` (rextendr 0.4.2), the community
standard used by CRAN extendr packages.

### Build flow

```
R CMD INSTALL
  -> configure(.win) calls Rscript tools/config.R
  -> config.R sources tools/msrv.R (checks rustc version)
  -> config.R reads Makevars(.win).in, substitutes @PLACEHOLDER@s,
     writes Makevars(.win)
  -> make runs cargo build --lib (produces static library)
  -> gcc compiles entrypoint.c, links with Rust static lib -> pizzarr.dll/.so
```

### Key files

| File | Purpose |
|------|---------|
| `src/rust/Cargo.toml` | Rust dependencies: extendr-api, zarrs, serde_json, etc. |
| `src/rust/src/lib.rs` | `#[extendr]` functions + `extendr_module!` |
| `src/entrypoint.c` | Forwards `R_init_pizzarr` -> `R_init_pizzarr_extendr` |
| `src/Makevars.in` | Unix build template (substituted by config.R) |
| `src/Makevars.win.in` | Windows build template |
| `R/extendr-wrappers.R` | Auto-generated `.Call()` wrappers -- never edit by hand |
| `tools/config.R` | Makevars template substitution + `@CARGO_FEATURES@` |
| `tools/msrv.R` | Minimum rustc version check |
| `tools/cran-build.sh` | Produces CRAN tarball without `src/` (pure R) |
| `configure` / `configure.win` | Shell scripts calling `tools/config.R` |

### Cargo.toml conventions

- `crate-type = ["staticlib"]` -- R links the static library
- `edition = "2021"` -- compatible with extendr-api 0.8.x
- `rust-version = "1.91"` -- required by zarrs 0.23.x
- `publish = false` -- not a standalone crate
- Feature flags forward to zarrs: `filesystem = ["zarrs/filesystem"]`, etc.
- `[profile.release]` uses `lto = true`, `codegen-units = 1`
- `extendr-api` uses **git master** (not crates.io 0.8.1) -- the crates.io
  release references `OBJSXP` unconditionally, which fails on R 4.5.x

### Regenerating wrappers

After modifying `#[extendr]` functions in `lib.rs`:

```r
rextendr::document()
```

This rebuilds the Rust library and regenerates `R/extendr-wrappers.R`.

### Windows build

The Rust toolchain must have the `x86_64-pc-windows-gnu` target:

```
rustup target add x86_64-pc-windows-gnu
```

### CRAN compatibility

CRAN macOS builders are at rustc 1.84.1, too old for zarrs 0.23.x.
The Rust code compiles only on r-universe (pre-built binaries); CRAN
ships pure R via `tools/cran-build.sh`.

## Architecture patterns

### Free functions, not extendr structs

`extendr_module!` lists free functions with a process-global store cache
(no `#[extendr]` structs). This keeps the FFI surface minimal.

### Store cache

`store_cache.rs` maps normalized URLs to opened zarrs stores. Stores
persist until explicitly removed. The `StorageEntry` enum holds both
read-write (filesystem) and read-only (HTTP) handles.

### JSON-based metadata construction

Array metadata is built as `serde_json::Value` and deserialized into
`ArrayMetadataV2` / `ArrayMetadataV3` rather than using `ArrayBuilder`.
This decouples from the builder's exact API shape across zarrs versions.

### C-to-F transpose in Rust

zarrs returns C-order data; R uses F-order. The transpose happens in
`transpose.rs` -- the R side receives/sends flat F-order data with no
`aperm()` needed.

## zarrs API notes

Gotchas specific to the zarrs crate versions used by pizzarr. These are
also documented as comments at the relevant call sites.

- **`retrieve_array_subset_opt::<Vec<T>>`**: the generic parameter is
  the output container, not the element type. `T: ElementOwned`.
- **`store_array_subset_opt`**: takes `impl IntoArrayBytes` -- no
  turbofish needed. `Vec<T: Element>` satisfies this.
- **`()` maps to R `NULL`**: indistinguishable from `tryCatch` error
  returns. Use `bool` return for functions where success must be
  detectable.
- **V2 compressor id**: zarrs treats `"zlib"` and `"gzip"` as separate
  codecs. Use `"gzip"` in V2 metadata; zarrs reads both on input.
- **`DataType::to_string()`** returns `"float64 / <f8"`. Split on
  `" / "` to extract the V3 name.
- **Bool fill value**: zarrs rejects integer `0`/`1` for bool dtype.
  Must use JSON `true`/`false`.
- **`FillValue` has no `Serialize` impl**: use `as_ne_bytes()` debug
  format for serialization.
- **`object_store` is bucket-level**: `with_url("s3://bucket/prefix")`
  sets the bucket only. Use `PrefixStore` for sub-bucket paths.
- **S3 anonymous access**: `with_skip_signature(true)`. GCS has no
  equivalent -- use the HTTPS endpoint for public data without
  credentials.
- **`blosc` feature on Windows**: requires `-lstdc++` and
  `-Wl,--allow-multiple-definition` (snappy/pthread conflicts).
- **`zarrs/sharding` requires `zarrs/crc32c`**: sharding references
  `Crc32cCodec` behind the crc32c feature gate.
- **`PIZZARR_FEATURES` env var**: controls extra Cargo features at
  build time. Set to `s3,gcs` for full cloud support.
