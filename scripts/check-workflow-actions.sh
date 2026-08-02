#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
workflow_files=("${repo_root}"/.github/workflows/*.yml "${repo_root}"/.github/workflows/*.yaml)
found_workflow=false
failed=false

for workflow in "${workflow_files[@]}"; do
  if [[ ! -f "${workflow}" ]]; then
    continue
  fi
  found_workflow=true
  line_number=0
  while IFS= read -r line; do
    ((line_number += 1))
    if [[ ! "${line}" =~ uses:[[:space:]]*([^[:space:]#]+) ]]; then
      continue
    fi
    action="${BASH_REMATCH[1]}"
    if [[ "${action}" == ./* || "${action}" == docker://* ]]; then
      continue
    fi
    reference="${action##*@}"
    if [[ ! "${reference}" =~ ^[0-9a-f]{40}$ ]]; then
      echo "error: ${workflow}:${line_number}: external action '${action}' is not pinned to a full commit" >&2
      failed=true
    fi
  done <"${workflow}"
done

if [[ "${found_workflow}" != true ]]; then
  echo "error: no GitHub Actions workflows found" >&2
  exit 1
fi

if [[ "${failed}" == true ]]; then
  exit 1
fi
