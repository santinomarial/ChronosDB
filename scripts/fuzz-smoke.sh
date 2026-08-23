#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
build_dir="${1:-${repo_root}/build/fuzz}"
runs="${FUZZ_RUNS:-1000}"
seed="${FUZZ_SEED:-424242}"
max_len="${FUZZ_MAX_LEN:-4096}"
timeout="${FUZZ_TIMEOUT_SECONDS:-10}"
entropic="${FUZZ_ENTROPIC:-0}"
artifact_dir="${CHRONOS_FUZZ_ARTIFACT_DIR:-${build_dir}/artifacts}"

for numeric in "${runs}" "${seed}" "${max_len}" "${timeout}"; do
  if [[ ! "${numeric}" =~ ^[0-9]+$ ]]; then
    echo "error: fuzz smoke numeric settings must be nonnegative integers" >&2
    exit 2
  fi
done
if [[ "${runs}" == 0 || "${max_len}" == 0 || "${timeout}" == 0 ]]; then
  echo "error: FUZZ_RUNS, FUZZ_MAX_LEN, and FUZZ_TIMEOUT_SECONDS must be positive" >&2
  exit 2
fi
if [[ "${entropic}" != 0 && "${entropic}" != 1 ]]; then
  echo "error: FUZZ_ENTROPIC must be 0 or 1" >&2
  exit 2
fi

targets=(
  chronos_byte_reader_fuzz
  chronos_wal_codec_fuzz
  chronos_raft_transport_fuzz
  chronos_metadata_snapshot_fuzz
  chronos_network_protocol_fuzz
  chronos_resume_token_fuzz
  chronos_multi_tablet_subscription_checkpoint_fuzz
  chronos_materialized_view_checkpoint_fuzz
  chronos_columnar_batch_codec_fuzz
  chronos_distributed_vector_result_exchange_fuzz
  chronos_distributed_vector_fragment_v2_fuzz
  chronos_distributed_vector_aggregate_state_fuzz
  chronos_distributed_vector_aggregate_exchange_fuzz
  chronos_distributed_vector_query_transport_v2_fuzz
  chronos_columnar_append_fuzz
  chronos_temporal_command_fuzz
  chronos_cseg_metadata_codec_fuzz
  chronos_cseg_plain_page_fuzz
  chronos_cseg_page_codec_fuzz
  chronos_cseg_part_codec_fuzz
  chronos_cseg_scan_fuzz
  chronos_head_scan_fuzz
  chronos_manifest_codec_fuzz
  chronos_sql_lexer_fuzz
  chronos_sql_parser_fuzz
  chronos_sql_binder_fuzz
  chronos_vector_chunk_fuzz
  chronos_grouped_aggregate_fuzz
  chronos_asof_join_fuzz
  chronos_relational_plan_fuzz
  chronos_physical_plan_fuzz
  chronos_physical_optimizer_fuzz
  chronos_physical_lowering_fuzz
  chronos_parallel_scheduler_fuzz
  chronos_spill_sort_fuzz
)

configured_targets="$(
  ninja -C "${build_dir}" -t targets all |
    sed -n 's/^\(chronos_[[:alnum:]_]*_fuzz\):.*/\1/p' |
    LC_ALL=C sort
)"
listed_targets="$(printf '%s\n' "${targets[@]}" | LC_ALL=C sort)"
if [[ -z "${configured_targets}" ]]; then
  echo "error: no configured fuzz targets found in ${build_dir}" >&2
  exit 1
fi
if [[ "${configured_targets}" != "${listed_targets}" ]]; then
  echo "error: fuzz smoke target list does not match the configured build" >&2
  diff -u <(printf '%s\n' "${configured_targets}") <(printf '%s\n' "${listed_targets}") >&2 ||
    true
  exit 1
fi

campaign_root="$(mktemp -d "${TMPDIR:-/tmp}/chronos-fuzz-smoke.XXXXXX")"
cleanup() {
  rm -rf -- "${campaign_root}"
}
trap cleanup EXIT
mkdir -p "${artifact_dir}"

for target in "${targets[@]}"; do
  executable="${build_dir}/${target}"
  if [[ ! -x "${executable}" ]]; then
    echo "error: missing fuzz executable: ${executable}" >&2
    exit 1
  fi
  corpus_kind=binary
  if [[ "${target}" == chronos_sql_* ]]; then
    corpus_kind=sql
  fi
  source_corpus="${repo_root}/tests/fuzz/corpus/${corpus_kind}"
  run_corpus="${campaign_root}/${target}"
  mkdir "${run_corpus}"
  cp "${source_corpus}"/* "${run_corpus}/"
  echo "fuzz-smoke: ${target} runs=${runs} seed=${seed} max_len=${max_len} entropic=${entropic}"
  "${executable}" "${run_corpus}" \
    -runs="${runs}" \
    -seed="${seed}" \
    -max_len="${max_len}" \
    -timeout="${timeout}" \
    -entropic="${entropic}" \
    -artifact_prefix="${artifact_dir}/" \
    -print_final_stats=1
done
