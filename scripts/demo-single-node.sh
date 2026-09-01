#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repository_root="$(cd -- "${script_dir}/.." && pwd)"
build_dir="${CHRONOS_DEMO_BUILD_DIR:-${repository_root}/build/dev}"
daemon="${build_dir}/chronosd"
client="${build_dir}/chronosctl"

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "ChronosDB demo requires Linux because chronosd uses the epoll reactor." >&2
  echo "Run it in a Linux VM/container after building chronosd and chronosctl there." >&2
  exit 2
fi

if [[ ! -x "${daemon}" || ! -x "${client}" ]]; then
  echo "ChronosDB demo binaries are missing from ${build_dir}." >&2
  echo "Build them with: cmake --build --preset dev --target chronosd chronosctl" >&2
  exit 2
fi

demo_root="$(mktemp -d "${TMPDIR:-/tmp}/chronosdb-interview-demo.XXXXXX")"
data_dir="${demo_root}/data"
mkdir "${data_dir}"
daemon_pid=""
daemon_port=""

stop_daemon() {
  if [[ -n "${daemon_pid}" ]] && kill -0 "${daemon_pid}" 2>/dev/null; then
    kill -TERM "${daemon_pid}"
    wait "${daemon_pid}"
  fi
  daemon_pid=""
}

cleanup() {
  stop_daemon
}

interrupted() {
  cleanup
  trap - EXIT
  exit 130
}

trap cleanup EXIT
trap interrupted INT TERM

start_daemon() {
  local log_path="$1"
  "${daemon}" --data-dir "${data_dir}" --port 0 >"${log_path}" 2>&1 &
  daemon_pid=$!
  daemon_port=""
  for _ in {1..100}; do
    daemon_port="$(sed -n 's/.*chronosd listening on 127\.0\.0\.1:\([0-9][0-9]*\).*/\1/p' "${log_path}" | head -1)"
    if [[ -n "${daemon_port}" ]]; then
      return
    fi
    if ! kill -0 "${daemon_pid}" 2>/dev/null; then
      wait "${daemon_pid}" || true
      echo "chronosd exited before becoming ready:" >&2
      tail -n 40 "${log_path}" >&2
      exit 1
    fi
    sleep 0.1
  done
  echo "chronosd did not become ready within 10 seconds:" >&2
  tail -n 40 "${log_path}" >&2
  exit 1
}

run_sql() {
  "${client}" sql --host 127.0.0.1 --port "${daemon_port}" --execute "$1"
}

echo "==> Starting a fresh single-node ChronosDB"
start_daemon "${demo_root}/chronosd.log"

echo "==> CREATE TABLE"
run_sql "CREATE TABLE trades (ts TIMESTAMP_NS NOT NULL, symbol SYMBOL NOT NULL, price_cents INT64 NOT NULL, note STRING) EVENT TIME ts ORDER KEY (symbol, ts) PARTITION BY time_bucket(INTERVAL '1 day', ts) SHARD KEY (symbol) DEDUP KEY (symbol, ts) RETENTION INTERVAL '30 days' SYSTEM HISTORY RETENTION INTERVAL '7 days' ALLOWED LATENESS INTERVAL '0 seconds'"

echo "==> INSERT two LOCAL_SYNC rows"
run_sql "INSERT INTO trades VALUES (TIMESTAMP '2026-08-24 12:00:00Z', CAST('AAPL' AS SYMBOL), 22716, 'opening row'), (TIMESTAMP '2026-08-24 12:00:01Z', CAST('MSFT' AS SYMBOL), 50426, NULL)"

echo "==> SELECT stored rows"
run_sql "SELECT symbol, price_cents, note FROM trades ORDER BY symbol"

echo "==> Graceful shutdown and restart with the same data directory"
stop_daemon
start_daemon "${demo_root}/chronosd-restarted.log"

echo "==> SELECT after recovery"
run_sql "SELECT count(*) AS persisted_rows FROM trades"
run_sql "SELECT symbol, price_cents, note FROM trades ORDER BY symbol"

stop_daemon
trap - EXIT INT TERM

echo "==> Demo complete"
echo "Data and daemon logs are retained at ${demo_root}"
