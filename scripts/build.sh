#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
preset="${1:-dev}"
if (($# > 0)); then
  shift
fi

cd "${repo_root}"
cmake --build --preset "${preset}" "$@"
