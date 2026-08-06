#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"

if (($# < 1)); then
  echo "usage: $0 OUTPUT_DIRECTORY [chronos-flushbench options]" >&2
  exit 2
fi

requested_output="$1"
shift
for argument in "$@"; do
  case "${argument}" in
    --output-dir | --allow-dirty | --allow-non-release)
      echo "error: ${argument} is controlled by this wrapper" >&2
      exit 2
      ;;
  esac
done

output_parent="$(dirname -- "${requested_output}")"
output_name="$(basename -- "${requested_output}")"
if [[ ! -d "${output_parent}" || "${output_name}" == "." || "${output_name}" == ".." ]]; then
  echo "error: OUTPUT_DIRECTORY must name a new directory beneath an existing parent" >&2
  exit 2
fi
output_parent="$(cd -- "${output_parent}" && pwd -P)"
output_directory="${output_parent}/${output_name}"
if [[ -e "${output_directory}" ]]; then
  echo "error: refusing to overwrite existing path ${output_directory}" >&2
  exit 2
fi
case "${output_directory}" in
  "${repo_root}" | "${repo_root}"/*)
    echo "error: benchmark artifacts must be written outside the source repository" >&2
    exit 2
    ;;
esac

cd "${repo_root}"
source_status="$(git status --porcelain=v1 --untracked-files=all)"
if [[ -n "${source_status}" ]]; then
  if [[ "${CHRONOS_BENCHMARK_ALLOW_DIRTY:-0}" != "1" ]]; then
    echo "error: source tree is dirty; commit/stash changes or set CHRONOS_BENCHMARK_ALLOW_DIRTY=1" >&2
    exit 2
  fi
  if [[ -n "$(git ls-files --others --exclude-standard)" ]]; then
    echo "error: dirty benchmark override does not permit untracked files" >&2
    exit 2
  fi
fi

cmake --preset flush-benchmark
cmake --build --preset flush-benchmark --target chronos-flushbench

flushbench="${repo_root}/build/flush-benchmark/chronos-flushbench"
temporary_log="$(mktemp "${output_parent}/.chronos-flushbench-log.XXXXXX")"
cleanup_log() {
  if [[ -f "${temporary_log}" ]]; then
    rm -f -- "${temporary_log}"
  fi
}
trap cleanup_log EXIT

command=(
  "${flushbench}"
  --output-dir "${output_directory}"
)
if [[ -n "${source_status}" ]]; then
  command+=(--allow-dirty)
fi
command+=("$@")
set +e
"${command[@]}" >"${temporary_log}" 2>&1
run_status=$?
set -e
cat "${temporary_log}"

if [[ ! -d "${output_directory}" ]]; then
  exit "${run_status}"
fi
mv -- "${temporary_log}" "${output_directory}/flushbench.log"

{
  printf 'cwd='
  printf '%q' "${repo_root}"
  printf '\nargv='
  printf '%q ' "${command[@]}"
  printf '\n'
} >"${output_directory}/invocation.txt"

git status --short --branch --untracked-files=all >"${output_directory}/git-status.txt"
git log -1 --format=fuller >"${output_directory}/git-head.txt"
if [[ -n "${source_status}" ]]; then
  git diff --binary HEAD >"${output_directory}/source.diff"
fi
cp -- "${repo_root}/build/flush-benchmark/CMakeCache.txt" "${output_directory}/CMakeCache.txt"
cp -- "${repo_root}/build/flush-benchmark/compile_commands.json" \
  "${output_directory}/compile_commands.json"

{
  date -u '+captured_utc=%Y-%m-%dT%H:%M:%SZ'
  uname -a
  if command -v sw_vers >/dev/null 2>&1; then sw_vers; fi
  if command -v lscpu >/dev/null 2>&1; then lscpu; fi
  if command -v sysctl >/dev/null 2>&1; then
    sysctl -n machdep.cpu.brand_string 2>/dev/null || true
    sysctl -n hw.logicalcpu 2>/dev/null || true
    sysctl -n hw.memsize 2>/dev/null || true
  fi
  if command -v lsblk >/dev/null 2>&1; then lsblk -O; fi
  df -h "${output_directory}"
  mount
} >"${output_directory}/system-inventory.txt" 2>&1

{
  printf 'LANG=%s\n' "${LANG:-unset}"
  printf 'LC_ALL=%s\n' "${LC_ALL:-unset}"
  printf 'TZ=%s\n' "${TZ:-unset}"
  printf 'CHRONOS_BENCHMARK_ALLOW_DIRTY=%s\n' "${CHRONOS_BENCHMARK_ALLOW_DIRTY:-unset}"
} >"${output_directory}/environment-allowlist.txt"

exit "${run_status}"
