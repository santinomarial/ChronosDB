#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

"${script_dir}/format.sh" --check
"${script_dir}/check-workflow-actions.sh"
"${script_dir}/configure.sh" dev
"${script_dir}/build.sh" dev
"${script_dir}/test.sh" dev
"${script_dir}/lint.sh" dev
