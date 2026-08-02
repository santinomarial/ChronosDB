#!/usr/bin/env bash
set -euo pipefail

source_root="${1:?repository source root is required}"
cmake_command="${2:?CMake executable is required}"
test_root="$(mktemp -d "${TMPDIR:-/tmp}/chronos-version-metadata.XXXXXX")"
repository="${test_root}/repository"
output="${test_root}/generated/version_config.hpp"

cleanup() {
  rm -rf -- "${test_root}"
}
trap cleanup EXIT

git init --quiet "${repository}"
git -C "${repository}" config user.name "ChronosDB Test"
git -C "${repository}" config user.email "chronosdb-test@example.invalid"
printf 'initial\n' >"${repository}/tracked.txt"
git -C "${repository}" add tracked.txt
git -C "${repository}" commit --quiet --message initial

generate_header() {
  "${cmake_command}" \
    "-DCHRONOS_SOURCE_DIR=${repository}" \
    "-DCHRONOS_OUTPUT_FILE=${output}" \
    "-DCHRONOS_TEMPLATE_FILE=${source_root}/cmake/version.hpp.in" \
    -DCHRONOS_VERSION=0.1.0 \
    -DCHRONOS_BUILD_TYPE=Test \
    "-DCHRONOS_COMPILER=Test Compiler" \
    -DCHRONOS_TARGET_ARCHITECTURE=test-architecture \
    -DCHRONOS_OPERATING_SYSTEM=TestOS \
    -P "${source_root}/cmake/GenerateVersionHeader.cmake"
}

generate_header
first_commit="$(git -C "${repository}" rev-parse --short=12 HEAD)"
grep -Fq "kGitCommit{\"${first_commit}\"}" "${output}"
grep -Fq 'kGitMetadataAvailable = 1' "${output}"
grep -Fq 'kGitDirty = 0' "${output}"

printf 'dirty\n' >>"${repository}/tracked.txt"
generate_header
grep -Fq 'kGitDirty = 1' "${output}"

git -C "${repository}" add tracked.txt
git -C "${repository}" commit --quiet --message update
generate_header
second_commit="$(git -C "${repository}" rev-parse --short=12 HEAD)"
grep -Fq "kGitCommit{\"${second_commit}\"}" "${output}"
grep -Fq 'kGitDirty = 0' "${output}"

if [[ "${first_commit}" == "${second_commit}" ]]; then
  echo "error: version metadata did not observe the new commit" >&2
  exit 1
fi

printf 'invalid index\n' >"${repository}/.git/index"
generate_header
grep -Fq 'kGitCommit{"unknown"}' "${output}"
grep -Fq 'kGitMetadataAvailable = 0' "${output}"
grep -Fq 'kGitDirty = 0' "${output}"
