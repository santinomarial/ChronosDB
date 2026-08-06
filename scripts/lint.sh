#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
preset="${1:-dev}"
if (($# > 0)); then
  shift
fi

supported_major=18

if [[ -n "${CLANG_TIDY:-}" ]]; then
  candidates=("${CLANG_TIDY}")
else
  candidates=(
    clang-tidy-18
    /opt/homebrew/opt/llvm@18/bin/clang-tidy
    /usr/local/opt/llvm@18/bin/clang-tidy
  )
fi

clang_tidy=""
for candidate in "${candidates[@]}"; do
  if command -v "${candidate}" >/dev/null 2>&1; then
    clang_tidy="$(command -v "${candidate}")"
    break
  fi
done
if [[ -z "${clang_tidy}" ]]; then
  echo "error: clang-tidy ${supported_major} was not found; install it or set CLANG_TIDY" >&2
  exit 1
fi
version="$("${clang_tidy}" --version)"
if [[ ! "${version}" =~ version[[:space:]]+${supported_major}\. ]]; then
  echo "error: ChronosDB requires clang-tidy ${supported_major}.x, found: ${version}" >&2
  exit 1
fi

cd "${repo_root}"
cmake \
  --preset "${preset}" \
  -DCHRONOS_ENABLE_CLANG_TIDY=ON \
  -DCHRONOS_CLANG_TIDY_EXECUTABLE="$(command -v "${clang_tidy}")" \
  "$@"
cmake --build --preset "${preset}"
