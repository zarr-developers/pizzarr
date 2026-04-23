#!/usr/bin/env bash
# Build a CRAN-ready tarball without the Rust src/ directory.
#
# The CRAN tier ships pure R. This script copies the package tree to a
# temp dir, invokes tools/cran-strip.sh to remove Rust-related files,
# and runs R CMD build on the clean copy.
#
# Usage:
#   bash tools/cran-build.sh
#
# Output:
#   pizzarr_<version>.tar.gz in the current directory

set -euo pipefail

PKG_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PKG_NAME="$(grep '^Package:' "$PKG_DIR/DESCRIPTION" | sed 's/Package: *//')"
PKG_VERSION="$(grep '^Version:' "$PKG_DIR/DESCRIPTION" | sed 's/Version: *//')"

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

echo "=== Building CRAN tarball for ${PKG_NAME} ${PKG_VERSION} ==="
echo "    Source: $PKG_DIR"
echo "    Temp:   $TMPDIR"

# Copy package tree, skipping .git and the Rust target dir (can be >1 GB).
# We delete src/ entirely below, so skip the Rust target dir now.
mkdir -p "$TMPDIR/$PKG_NAME"
for item in "$PKG_DIR"/*; do
  base="$(basename "$item")"
  case "$base" in
    .git) continue ;;
    src)
      mkdir -p "$TMPDIR/$PKG_NAME/src"
      for sub in "$item"/*; do
        subbase="$(basename "$sub")"
        if [ "$subbase" = "rust" ]; then
          mkdir -p "$TMPDIR/$PKG_NAME/src/rust"
          for rsub in "$sub"/*; do
            rsubbase="$(basename "$rsub")"
            [ "$rsubbase" = "target" ] && continue
            cp -r "$rsub" "$TMPDIR/$PKG_NAME/src/rust/"
          done
        else
          cp -r "$sub" "$TMPDIR/$PKG_NAME/src/"
        fi
      done
      ;;
    *) cp -r "$item" "$TMPDIR/$PKG_NAME/" ;;
  esac
done
# Copy dotfiles
for item in "$PKG_DIR"/.[!.]*; do
  base="$(basename "$item")"
  [ "$base" = ".git" ] && continue
  cp -r "$item" "$TMPDIR/$PKG_NAME/" 2>/dev/null || true
done

# Strip Rust-related files from the tmp copy.
bash "$PKG_DIR/tools/cran-strip.sh" "$TMPDIR/$PKG_NAME"

echo "=== Running R CMD build ==="
cd "$TMPDIR"

# Find R on PATH or use R_HOME
if command -v R &> /dev/null; then
  R_CMD="R"
elif [ -n "${R_HOME:-}" ]; then
  R_CMD="${R_HOME}/bin/R"
else
  # Common Windows location
  R_CMD="$(ls -d /c/Users/*/AppData/Local/Programs/R/R-*/bin/R.exe 2>/dev/null | tail -1)"
  if [ -z "$R_CMD" ]; then
    echo "ERROR: R not found. Set R_HOME or add R to PATH."
    exit 1
  fi
fi

"$R_CMD" CMD build "$PKG_NAME"

# Copy tarball to original directory
cp "${PKG_NAME}_${PKG_VERSION}.tar.gz" "$PKG_DIR/"

echo "=== Done: ${PKG_NAME}_${PKG_VERSION}.tar.gz ==="
