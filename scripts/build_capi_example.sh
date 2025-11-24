#!/usr/bin/env bash
set -euo pipefail

# Build the C example against the released cdylib.
# Usage: scripts/build_capi_example.sh

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pushd "$root" >/dev/null

echo "[1/2] cargo build --release --features \"capi\""
cargo build --release --features "capi"

echo "[2/2] gcc example"
gcc -Iinclude examples/capi_example.c target/release/libferric_cache.so -o examples/capi_example \
  -Ltarget/release -Wl,-rpath,'$ORIGIN/../target/release'

echo "Built examples/capi_example (set LD_LIBRARY_PATH=target/release when running)."

popd >/dev/null
