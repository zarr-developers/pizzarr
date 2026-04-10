# Phase 1, Iteration 1: extendr scaffolding + compile check

## Context

pizzarr v0.2.0 adds a zarrs Rust backend via extendr (TODO.md §8, Phase 1).
This first iteration gets the build pipeline working end-to-end: Rust source
compiles, shared library loads, and a single probe function is callable from R.
No store cache, no metadata, no dispatch — just the plumbing.

Rustc 1.94.1 (x86_64-pc-windows-msvc) is on this machine. rextendr 0.4.2 is
installed. CRAN never compiles Rust (pure R tarball via `tools/cran-build.sh`).

---

## Increment 1 — rextendr scaffolding

Run `rextendr::use_extendr()` from the package root. This creates the
official file tree:

```
src/
  entrypoint.c
  Makevars.in
  Makevars.win.in
  pizzarr-win.def
  .gitignore
  rust/
    Cargo.toml          (extendr-api dep, staticlib + rlib)
    document.rs         (auto-generates R/extendr-wrappers.R at build time)
    src/lib.rs          (hello_world stub)
R/extendr-wrappers.R    (auto-generated .Call wrappers)
tools/
  config.R              (Makevars.in → Makevars substitution)
  msrv.R                (Rust version check)
configure               (calls tools/config.R)
configure.win
cleanup
cleanup.win
```

Also modifies: `.Rbuildignore`, `.gitignore`, DESCRIPTION (R >= 4.2 dep).

### Verify

```r
rextendr::document()   # generates wrappers
devtools::load_all()   # loads the shared library
hello_world()          # returns "Hello world!"
```

---

## Increment 2 — Add zarrs dependency + `zarrs_compiled_features()`

Edit `src/rust/Cargo.toml`:
- Add `zarrs` 0.23.x (default-features off, enable `filesystem` + `gzip`)
- Add `serde_json` for later metadata work
- Set `edition = "2021"` (keeps compat with extendr-api 0.8)
- Add `[profile.release]` with `lto = true`, `codegen-units = 1`

Replace the hello_world stub in `src/rust/src/lib.rs` with:

```rust
/// Return compiled zarrs feature flags as a character vector.
/// @export
#[extendr]
fn zarrs_compiled_features() -> Vec<String> {
    let mut features = Vec::new();
    if cfg!(feature = "filesystem") { features.push("filesystem".into()); }
    // ... gzip, blosc, etc.
    features
}

extendr_module! {
    mod pizzarr;
    fn zarrs_compiled_features;
}
```

### Verify

```
cargo build --lib --release --manifest-path=src/rust/Cargo.toml
```

Then:

```r
rextendr::document()
devtools::load_all()
zarrs_compiled_features()
# expect: c("filesystem", "gzip")
```

---

## Increment 3 — R-side availability probe + .onLoad/.onAttach

### Files modified

**`R/onload.R`** — add `.pizzarr_env`, zarrs detection, `.onAttach` message:
- Create `.pizzarr_env <- new.env(parent = emptyenv())` at file top
- In `.onLoad`: set `.pizzarr_env$zarrs_available <- is_zarrs_available()`
- Add `.onAttach` that prints one-time r-universe message when zarrs absent

**`R/zarrs-bridge.R`** (new) — public helpers:
- `is_zarrs_available()` — tries `.Call(wrap__zarrs_compiled_features)`,
  returns TRUE/FALSE
- `pizzarr_compiled_features()` — exported, returns features or
  `character(0)` with message
- `pizzarr_upgrade()` — prints r-universe install command

### Verify

```r
devtools::load_all()
is_zarrs_available()      # TRUE
pizzarr_compiled_features()  # c("filesystem", "gzip")
```

---

## Increment 4 — CRAN tarball script + dual-path validation

**`tools/cran-build.sh`** (new):
1. Copy package tree to temp dir
2. Remove `src/`, `configure`, `configure.win`, `cleanup`, `cleanup.win`,
   `tools/config.R`, `tools/msrv.R`
3. Remove `SystemRequirements` line from DESCRIPTION
4. `R CMD build` on cleaned copy
5. Output: `pizzarr_<version>.tar.gz`

### Verify (two checks)

Full repo (with Rust):
```r
devtools::check()
# Existing 1006+ tests pass, no new errors
```

CRAN tarball (pure R):
```bash
bash tools/cran-build.sh
R CMD check pizzarr_*.tar.gz
# Installs pure R, is_zarrs_available() returns FALSE, all tests pass
```

---

## Increment 5 — RUST-STYLE.md update + .Rbuildignore + DESCRIPTION cleanup

Update `RUST-STYLE.md` with a new "Build pipeline" section documenting
the scaffolding approach and the research behind it:

**Research summary to include:**

Three approaches were evaluated:

1. **Official rextendr scaffolding** (rextendr 0.4.2 docs,
   `rextendr::use_extendr()`): creates `configure` / `configure.win`
   calling `tools/config.R` + `tools/msrv.R`. Uses `Makevars.in` /
   `Makevars.win.in` templates with `@PLACEHOLDER@` substitution for
   debug/release profiles, CRAN offline builds, and WebR targets.
   Includes a `document.rs` binary that auto-generates
   `R/extendr-wrappers.R` at build time. This is the community standard
   used by CRAN packages.

2. **arcgisutils** (extendr, on CRAN): follows rextendr standard exactly.
   Adds `[profile.release]` with `lto = true`, `codegen-units = 1`.
   Uses `rextendr::vendor_pkgs()` for CRAN offline builds.
   `SystemRequirements: Cargo (Rust's package manager), rustc >= 1.67, xz`.

3. **zr** (extendr, not on CRAN): skips configure scripts entirely,
   uses static `Makevars` / `Makevars.win` / `Makevars.ucrt` (Pattern A
   from helloextendr). Simpler but diverges from the rextendr standard.
   No auto-generated wrappers (`R/extendr-wrappers.R` is hand-written).

**Decision:** Use the rextendr standard (approach 1). pizzarr's two-tier
model (CRAN = pure R, r-universe = zarrs) doesn't need vendoring or
CRAN offline builds, but the configure/template approach costs little
and keeps us aligned with the extendr ecosystem. The `document.rs`
auto-wrapper pattern avoids hand-maintaining `.Call()` bindings.

**Specific details to document in RUST-STYLE.md:**
- `document.rs` auto-wrapper binary pattern
- `Makevars.in` / `Makevars.win.in` template approach
- `configure` / `configure.win` → `tools/config.R` → `tools/msrv.R` flow
- `crate-type = ["staticlib", "rlib"]` (rlib needed for document binary)
- Updated module layout reflecting actual `src/rust/src/` structure
- `src/rust/rustfmt.toml` with `imports_granularity = "Module"` (per
  existing RUST-STYLE.md spec)
- Windows link libraries: `-lws2_32 -ladvapi32 -luserenv -lbcrypt -lntdll`
- `entrypoint.c` forwards `R_init_pizzarr` → `R_init_pizzarr_extendr`
  (plus `register_extendr_panic_hook` for panic-to-R-error conversion)

Other cleanup:
- Add `dev.md`, `RUST-STYLE.md` to `.Rbuildignore`
- Confirm `SystemRequirements: Cargo (Rust's package manager), rustc`
  is in DESCRIPTION (rextendr may have added it)
- Run `devtools::document()` to update NAMESPACE with `useDynLib`
- Final `devtools::check()` — clean pass

---

## Files touched (summary)

| File | Action | Source |
|------|--------|--------|
| `src/rust/Cargo.toml` | created by rextendr, then edited | increment 1–2 |
| `src/rust/src/lib.rs` | created by rextendr, then edited | increment 1–2 |
| `src/rust/document.rs` | created by rextendr | increment 1 |
| `src/entrypoint.c` | created by rextendr | increment 1 |
| `src/Makevars.in` | created by rextendr | increment 1 |
| `src/Makevars.win.in` | created by rextendr | increment 1 |
| `src/pizzarr-win.def` | created by rextendr | increment 1 |
| `src/.gitignore` | created by rextendr | increment 1 |
| `R/extendr-wrappers.R` | created by rextendr, regenerated | increment 1–2 |
| `R/onload.R` | modified | increment 3 |
| `R/zarrs-bridge.R` | new | increment 3 |
| `tools/config.R` | created by rextendr | increment 1 |
| `tools/msrv.R` | created by rextendr | increment 1 |
| `tools/cran-build.sh` | new | increment 4 |
| `configure` | created by rextendr | increment 1 |
| `configure.win` | created by rextendr | increment 1 |
| `cleanup` | created by rextendr | increment 1 |
| `cleanup.win` | created by rextendr | increment 1 |
| `DESCRIPTION` | modified (R >= 4.2, SystemRequirements) | increment 1, 5 |
| `.Rbuildignore` | modified | increment 1, 5 |
| `.gitignore` | modified | increment 1 |
| `RUST-STYLE.md` | updated to reflect rextendr scaffolding | increment 5 |
| `src/rust/rustfmt.toml` | new (per RUST-STYLE.md spec) | increment 5 |
| `NAMESPACE` | regenerated by roxygen | increment 5 |

---

## Done criteria

1. `cargo build` in `src/rust/` succeeds
2. `devtools::load_all()` loads the Rust shared library
3. `zarrs_compiled_features()` returns `c("filesystem", "gzip")` from R
4. `is_zarrs_available()` returns `TRUE`
5. `devtools::check()` passes (no new errors/warnings beyond existing skips)
6. `tools/cran-build.sh` produces a tarball that installs and checks pure R
