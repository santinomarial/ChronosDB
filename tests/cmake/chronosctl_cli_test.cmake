if(NOT DEFINED CHRONOSCTL OR CHRONOSCTL STREQUAL "")
  message(FATAL_ERROR "CHRONOSCTL is required")
endif()

execute_process(
  COMMAND "${CHRONOSCTL}" --help
  RESULT_VARIABLE help_result
  OUTPUT_VARIABLE help_stdout
  ERROR_VARIABLE help_stderr
)
if(NOT help_result EQUAL 0 OR NOT help_stderr STREQUAL "")
  message(FATAL_ERROR "chronosctl --help failed")
endif()
if(NOT help_stdout MATCHES "quorum-sync.*--group UUID")
  message(FATAL_ERROR "chronosctl --help omits the quorum-sync contract")
endif()
if(NOT help_stdout MATCHES
   "sql --host 127.0.0.1 --port PORT --execute \"SQL\""
)
  message(FATAL_ERROR "chronosctl --help omits the SQL contract")
endif()
if(NOT help_stdout MATCHES "routed-sql --group UUID --initial-node NODE_ID")
  message(FATAL_ERROR "chronosctl --help omits the routed-sql contract")
endif()

execute_process(
  COMMAND "${CHRONOSCTL}" sql --help
  RESULT_VARIABLE sql_help_result
  OUTPUT_VARIABLE sql_help_stdout
  ERROR_VARIABLE sql_help_stderr
)
if(NOT sql_help_result EQUAL 0 OR NOT sql_help_stderr STREQUAL "")
  message(FATAL_ERROR "chronosctl sql --help failed")
endif()
if(NOT sql_help_stdout MATCHES "tab-separated column names and rows")
  message(FATAL_ERROR "SQL help omits its result format")
endif()

function(expect_sql_option_failure case_name expected_error)
  execute_process(
    COMMAND "${CHRONOSCTL}" sql ${ARGN}
    RESULT_VARIABLE result
    OUTPUT_VARIABLE stdout
    ERROR_VARIABLE stderr
  )
  if(NOT result EQUAL 2 OR NOT stdout STREQUAL "")
    message(FATAL_ERROR "${case_name} changed its SQL option exit/output contract")
  endif()
  if(NOT stderr MATCHES "^chronosctl: ${expected_error}\nUsage:")
    message(FATAL_ERROR "${case_name} returned an unexpected SQL diagnostic: ${stderr}")
  endif()
endfunction()

expect_sql_option_failure(missing_sql_options "sql requires --host, --port, and --execute")
expect_sql_option_failure(
  non_loopback_sql_host
  "--host must be the plaintext loopback address 127.0.0.1"
  --host localhost
)
expect_sql_option_failure(
  invalid_sql_port
  "--port requires a canonical decimal in the range 1..65535"
  --port 0
)
expect_sql_option_failure(
  empty_sql
  "--execute requires a nonempty SQL statement"
  --execute ""
)

execute_process(
  COMMAND "${CHRONOSCTL}" routed-sql --help
  RESULT_VARIABLE routed_sql_help_result
  OUTPUT_VARIABLE routed_sql_help_stdout
  ERROR_VARIABLE routed_sql_help_stderr
)
if(NOT routed_sql_help_result EQUAL 0 OR NOT routed_sql_help_stderr STREQUAL "")
  message(FATAL_ERROR "chronosctl routed-sql --help failed")
endif()
if(NOT routed_sql_help_stdout MATCHES "only after QUERY_END")
  message(FATAL_ERROR "routed-sql help omits its terminal result contract")
endif()

function(expect_routed_sql_option_failure case_name expected_error)
  execute_process(
    COMMAND "${CHRONOSCTL}" routed-sql ${ARGN}
    RESULT_VARIABLE result
    OUTPUT_VARIABLE stdout
    ERROR_VARIABLE stderr
  )
  if(NOT result EQUAL 2 OR NOT stdout STREQUAL "")
    message(FATAL_ERROR "${case_name} changed its routed-sql option exit/output contract")
  endif()
  if(NOT stderr MATCHES "^chronosctl: ${expected_error}\nUsage:")
    message(FATAL_ERROR "${case_name} returned an unexpected routed-sql diagnostic: ${stderr}")
  endif()
endfunction()

expect_routed_sql_option_failure(
  missing_routed_sql_bundle
  "routed-sql requires every group, route, TLS, query, and timeout option"
)
expect_routed_sql_option_failure(
  empty_routed_sql
  "--execute requires a nonempty SQL statement"
  --execute ""
)
expect_routed_sql_option_failure(
  duplicate_routed_sql_group
  "--group was specified more than once"
  --group 00000000-0000-0000-0000-000000000001
  --group 00000000-0000-0000-0000-000000000001
)
expect_routed_sql_option_failure(
  excessive_routed_sql_timeout
  "--timeout-ms exceeds the one-hour command limit"
  --timeout-ms 3600001
)
expect_routed_sql_option_failure(
  unknown_routed_sql_option
  "unknown routed-sql option: --future"
  --future value
)

execute_process(
  COMMAND "${CHRONOSCTL}" quorum-sync --help
  RESULT_VARIABLE command_help_result
  OUTPUT_VARIABLE command_help_stdout
  ERROR_VARIABLE command_help_stderr
)
if(NOT command_help_result EQUAL 0 OR NOT command_help_stderr STREQUAL "")
  message(FATAL_ERROR "chronosctl quorum-sync --help failed")
endif()
if(NOT command_help_stdout MATCHES "exact canonical Columnar Append v1")
  message(FATAL_ERROR "quorum-sync help omits its append input contract")
endif()

function(expect_option_failure case_name expected_error)
  execute_process(
    COMMAND "${CHRONOSCTL}" quorum-sync ${ARGN}
    RESULT_VARIABLE result
    OUTPUT_VARIABLE stdout
    ERROR_VARIABLE stderr
  )
  if(NOT result EQUAL 2 OR NOT stdout STREQUAL "")
    message(FATAL_ERROR "${case_name} changed its exit/output contract")
  endif()
  if(NOT stderr MATCHES "^chronosctl: ${expected_error}\nUsage:")
    message(FATAL_ERROR "${case_name} returned an unexpected diagnostic: ${stderr}")
  endif()
endfunction()

expect_option_failure(
  missing_bundle
  "quorum-sync requires every group, route, TLS, append, and timeout option"
)
expect_option_failure(
  uppercase_uuid
  "--group requires a non-nil lowercase canonical UUID"
  --group 00000000-0000-0000-0000-00000000000A
)
expect_option_failure(
  leading_zero_node
  "--initial-node requires a positive canonical decimal"
  --initial-node 01
)
expect_option_failure(
  excessive_timeout
  "--timeout-ms exceeds the one-hour command limit"
  --timeout-ms 3600001
)
expect_option_failure(
  duplicate_json
  "--json was specified more than once"
  --json --json
)
expect_option_failure(
  unknown_option
  "unknown quorum-sync option: --future"
  --future value
)

set(invalid_append "${CMAKE_CURRENT_BINARY_DIR}/chronosctl-invalid-append.bin")
file(WRITE "${invalid_append}" "not-a-columnar-append")
execute_process(
  COMMAND
    "${CHRONOSCTL}" quorum-sync
    --group 00000000-0000-0000-0000-000000000001
    --initial-node 1
    --minimum-placement-epoch 1
    --routes /does/not/exist/routes
    --tls-cert /does/not/exist/cert
    --tls-key /does/not/exist/key
    --tls-ca /does/not/exist/ca
    --append-file "${invalid_append}"
    --timeout-ms 1
  RESULT_VARIABLE malformed_result
  OUTPUT_VARIABLE malformed_stdout
  ERROR_VARIABLE malformed_stderr
)
file(REMOVE "${invalid_append}")
if(NOT malformed_result EQUAL 1 OR NOT malformed_stdout STREQUAL "")
  message(FATAL_ERROR "malformed append changed its exit/output contract")
endif()
if(NOT malformed_stderr MATCHES
   "^chronosctl: not_supported: WAL application envelope is not COLUMNAR_APPEND v1"
)
  message(FATAL_ERROR "malformed append was not rejected before route loading: ${malformed_stderr}")
endif()

execute_process(
  COMMAND "${CHRONOSCTL}" version --json
  RESULT_VARIABLE version_result
  OUTPUT_VARIABLE version_stdout
  ERROR_VARIABLE version_stderr
)
if(NOT version_result EQUAL 0 OR NOT version_stderr STREQUAL "")
  message(FATAL_ERROR "chronosctl version --json compatibility failed")
endif()
string(STRIP "${version_stdout}" version_json)
string(JSON version_value ERROR_VARIABLE version_error GET "${version_json}" version)
if(version_error OR version_value STREQUAL "")
  message(FATAL_ERROR "chronosctl version --json is not valid version metadata")
endif()
