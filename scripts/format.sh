#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
mode="${1:-format}"

if [[ "${mode}" != "format" && "${mode}" != "--check" ]]; then
  echo "usage: $0 [format|--check]" >&2
  exit 2
fi

clang_format="${CLANG_FORMAT:-clang-format}"
if ! command -v "${clang_format}" >/dev/null 2>&1; then
  echo "error: ${clang_format} was not found; set CLANG_FORMAT to a compatible executable" >&2
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
