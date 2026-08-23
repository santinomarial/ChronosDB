if(NOT DEFINED CHRONOSD OR CHRONOSD STREQUAL "")
  message(FATAL_ERROR "CHRONOSD is required")
endif()

execute_process(
  COMMAND "${CHRONOSD}" --log-format json --port invalid
  RESULT_VARIABLE json_result
  OUTPUT_VARIABLE json_stdout
  ERROR_VARIABLE json_stderr
)
if(NOT json_result EQUAL 2)
  message(FATAL_ERROR "JSON option failure returned ${json_result}, expected 2")
endif()
if(NOT json_stdout STREQUAL "")
  message(FATAL_ERROR "JSON option failure unexpectedly wrote stdout")
endif()
string(STRIP "${json_stderr}" json_line)
string(JSON json_severity ERROR_VARIABLE json_error GET "${json_line}" severity)
if(json_error OR NOT json_severity STREQUAL "ERROR")
  message(FATAL_ERROR "JSON option failure has invalid severity: ${json_error}")
endif()
string(JSON json_component GET "${json_line}" component)
string(JSON json_event GET "${json_line}" event)
string(JSON json_message GET "${json_line}" message)
string(JSON json_timestamp GET "${json_line}" timestamp)
if(NOT json_component STREQUAL "chronosd" OR NOT json_event STREQUAL "invalid_options")
  message(FATAL_ERROR "JSON option failure has incorrect identity fields")
endif()
if(NOT json_message STREQUAL "port must be an integer from 0 through 65535")
  message(FATAL_ERROR "JSON option failure has an unexpected message")
endif()
if(NOT json_timestamp MATCHES "^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T")
  message(FATAL_ERROR "JSON option failure has an invalid timestamp")
endif()

execute_process(
  COMMAND "${CHRONOSD}" --port invalid
  RESULT_VARIABLE text_result
  OUTPUT_VARIABLE text_stdout
  ERROR_VARIABLE text_stderr
)
if(NOT text_result EQUAL 2 OR NOT text_stdout STREQUAL "")
  message(FATAL_ERROR "text option failure changed its exit/output contract")
endif()
if(NOT text_stderr MATCHES "^chronosd: port must be an integer from 0 through 65535\nUsage:")
  message(FATAL_ERROR "text option failure changed its compatibility prefix")
endif()

execute_process(
  COMMAND "${CHRONOSD}" --help
  RESULT_VARIABLE help_result
  OUTPUT_VARIABLE help_stdout
  ERROR_VARIABLE help_stderr
)
if(NOT help_result EQUAL 0 OR NOT help_stderr STREQUAL "")
  message(FATAL_ERROR "chronosd --help failed")
endif()
if(NOT help_stdout MATCHES "--log-format text\\|json")
  message(FATAL_ERROR "chronosd --help omits the log format option")
endif()
if(NOT help_stdout MATCHES "--native-client-principals FILE" OR
   NOT help_stdout MATCHES "--native-tls-cert FILE" OR
   NOT help_stdout MATCHES "--native-tls-key FILE" OR
   NOT help_stdout MATCHES "--native-tls-ca FILE")
  message(FATAL_ERROR "chronosd --help omits the native TLS option bundle")
endif()

execute_process(
  COMMAND "${CHRONOSD}" --native-tls-cert server.pem
  RESULT_VARIABLE partial_native_tls_result
  OUTPUT_VARIABLE partial_native_tls_stdout
  ERROR_VARIABLE partial_native_tls_stderr
)
if(NOT partial_native_tls_result EQUAL 2 OR NOT partial_native_tls_stdout STREQUAL "" OR
   NOT partial_native_tls_stderr MATCHES
     "native client principals and all native TLS files must be configured together")
  message(FATAL_ERROR "partial native TLS bundle did not fail closed")
endif()

execute_process(
  COMMAND "${CHRONOSD}" --native-tls-cert ""
  RESULT_VARIABLE empty_native_tls_result
  OUTPUT_VARIABLE empty_native_tls_stdout
  ERROR_VARIABLE empty_native_tls_stderr
)
if(NOT empty_native_tls_result EQUAL 2 OR NOT empty_native_tls_stdout STREQUAL "" OR
   NOT empty_native_tls_stderr MATCHES "native TLS certificate path must be nonempty")
  message(FATAL_ERROR "empty native TLS path could disable the requested security bundle")
endif()

execute_process(
  COMMAND "${CHRONOSD}" --listen 192.0.2.1
  RESULT_VARIABLE remote_plaintext_result
  OUTPUT_VARIABLE remote_plaintext_stdout
  ERROR_VARIABLE remote_plaintext_stderr
)
if(NOT remote_plaintext_result EQUAL 2 OR NOT remote_plaintext_stdout STREQUAL "" OR
   NOT remote_plaintext_stderr MATCHES "plaintext service is restricted to IPv4 loopback")
  message(FATAL_ERROR "remote plaintext listen did not fail closed")
endif()

execute_process(
  COMMAND
    "${CHRONOSD}" --backend io_uring
    --native-client-principals principals.conf
    --native-tls-cert server.pem
    --native-tls-key server-key.pem
    --native-tls-ca ca.pem
  RESULT_VARIABLE io_uring_tls_result
  OUTPUT_VARIABLE io_uring_tls_stdout
  ERROR_VARIABLE io_uring_tls_stderr
)
if(NOT io_uring_tls_result EQUAL 2 OR NOT io_uring_tls_stdout STREQUAL "" OR
   NOT io_uring_tls_stderr MATCHES "native TLS requires the epoll backend")
  message(FATAL_ERROR "io_uring native TLS did not fail before file access")
endif()

execute_process(
  COMMAND "${CHRONOSD}" --listen 127.00.0.1
  RESULT_VARIABLE noncanonical_listen_result
  OUTPUT_VARIABLE noncanonical_listen_stdout
  ERROR_VARIABLE noncanonical_listen_stderr
)
if(NOT noncanonical_listen_result EQUAL 2 OR NOT noncanonical_listen_stdout STREQUAL "" OR
   NOT noncanonical_listen_stderr MATCHES "listen address must be canonical nonzero IPv4")
  message(FATAL_ERROR "noncanonical IPv4 listen address was accepted")
endif()

set(native_principals "${CMAKE_CURRENT_BINARY_DIR}/chronosd-cli-native-principals.conf")
file(WRITE "${native_principals}" "CHRONOSDB_NATIVE_SERVER_PRINCIPALS_V0\n")
execute_process(
  COMMAND
    "${CHRONOSD}"
    --native-client-principals "${native_principals}"
    --native-tls-cert missing-cert.pem
    --native-tls-key missing-key.pem
    --native-tls-ca missing-ca.pem
  RESULT_VARIABLE malformed_principals_result
  OUTPUT_VARIABLE malformed_principals_stdout
  ERROR_VARIABLE malformed_principals_stderr
)
if(NOT malformed_principals_result EQUAL 1 OR NOT malformed_principals_stdout STREQUAL "" OR
   NOT malformed_principals_stderr MATCHES "native client principal config is invalid")
  file(REMOVE "${native_principals}")
  message(FATAL_ERROR "malformed native principal authority did not fail before TLS file access")
endif()

file(WRITE "${native_principals}"
  "CHRONOSDB_NATIVE_SERVER_PRINCIPALS_V1\n"
  "7=30aa529b935af809084e419d00f39bce2bf5641da93d7bd9ad71e67bc21de368\n"
)
file(CHMOD "${native_principals}" PERMISSIONS OWNER_READ OWNER_WRITE GROUP_WRITE)
execute_process(
  COMMAND
    "${CHRONOSD}"
    --native-client-principals "${native_principals}"
    --native-tls-cert missing-cert.pem
    --native-tls-key missing-key.pem
    --native-tls-ca missing-ca.pem
  RESULT_VARIABLE writable_principals_result
  OUTPUT_VARIABLE writable_principals_stdout
  ERROR_VARIABLE writable_principals_stderr
)
file(REMOVE "${native_principals}")
if(NOT writable_principals_result EQUAL 1 OR NOT writable_principals_stdout STREQUAL "" OR
   NOT writable_principals_stderr MATCHES "not writable by group/other")
  message(FATAL_ERROR "group-writable native principal authority did not fail qualification")
endif()
