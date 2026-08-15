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
