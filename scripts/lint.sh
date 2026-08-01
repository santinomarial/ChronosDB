#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
preset="${1:-dev}"
if (($# > 0)); then
  shift
fi

clang_tidy="${CLANG_TIDY:-clang-tidy}"
if ! command -v "${clang_tidy}" >/dev/null 2>&1; then
  echo "error: ${clang_tidy} was not found; set CLANG_TIDY to a compatible executable" >&2
  exit 1
fi

cd "${repo_root}"
cmake \
  --preset "${preset}" \
  -DCHRONOS_ENABLE_CLANG_TIDY=ON \
  -DCHRONOS_CLANG_TIDY_EXECUTABLE="$(command -v "${clang_tidy}")" \
  "$@"
cmake --build --preset "${preset}"
