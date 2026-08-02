#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
mode="${1:-format}"

if [[ "${mode}" != "format" && "${mode}" != "--check" ]]; then
  echo "usage: $0 [format|--check]" >&2
  exit 2
fi

supported_major=18

if [[ -n "${CLANG_FORMAT:-}" ]]; then
  candidates=("${CLANG_FORMAT}")
else
  candidates=(
    clang-format-18
    /opt/homebrew/opt/llvm@18/bin/clang-format
    /usr/local/opt/llvm@18/bin/clang-format
    clang-format
  )
fi

clang_format=""
for candidate in "${candidates[@]}"; do
  if command -v "${candidate}" >/dev/null 2>&1; then
    clang_format="$(command -v "${candidate}")"
    break
  fi
done
if [[ -z "${clang_format}" ]]; then
  echo "error: clang-format ${supported_major} was not found; install it or set CLANG_FORMAT" >&2
  exit 1
fi

version="$("${clang_format}" --version)"
if [[ ! "${version}" =~ version[[:space:]]+${supported_major}\. ]]; then
  echo "error: ChronosDB requires clang-format ${supported_major}.x, found: ${version}" >&2
  exit 1
fi

files=()
while IFS= read -r -d '' file; do
  files+=("${file}")
done < <(
  find \
    "${repo_root}/include" \
    "${repo_root}/src" \
    "${repo_root}/tools" \
    "${repo_root}/tests" \
    "${repo_root}/benchmarks" \
    -type f \( -name '*.cpp' -o -name '*.hpp' \) -print0
)

if ((${#files[@]} == 0)); then
  echo "error: no C++ source files found" >&2
  exit 1
fi

if [[ "${mode}" == "--check" ]]; then
  "${clang_format}" --dry-run --Werror "${files[@]}"
else
  "${clang_format}" -i "${files[@]}"
fi
