cmake_minimum_required(VERSION 3.25)

foreach(
  required_variable
  IN ITEMS
    CHRONOS_SOURCE_DIR
    CHRONOS_OUTPUT_FILE
    CHRONOS_TEMPLATE_FILE
    CHRONOS_VERSION
    CHRONOS_BUILD_TYPE
    CHRONOS_COMPILER
    CHRONOS_TARGET_ARCHITECTURE
    CHRONOS_OPERATING_SYSTEM
)
  if(NOT DEFINED "${required_variable}")
    message(FATAL_ERROR "${required_variable} is required to generate version metadata")
  endif()
endforeach()

set(CHRONOS_GIT_COMMIT "unknown")
set(CHRONOS_GIT_METADATA_AVAILABLE 0)
set(CHRONOS_GIT_DIRTY 0)

find_package(Git QUIET)
if(Git_FOUND)
  execute_process(
    COMMAND "${GIT_EXECUTABLE}" rev-parse --short=12 HEAD
    WORKING_DIRECTORY "${CHRONOS_SOURCE_DIR}"
    RESULT_VARIABLE CHRONOS_GIT_REVISION_RESULT
    OUTPUT_VARIABLE CHRONOS_GIT_REVISION
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
  )
  if(CHRONOS_GIT_REVISION_RESULT EQUAL 0)
    set(CHRONOS_GIT_COMMIT "${CHRONOS_GIT_REVISION}")
    set(CHRONOS_GIT_METADATA_AVAILABLE 1)
    execute_process(
      COMMAND "${GIT_EXECUTABLE}" status --porcelain
      WORKING_DIRECTORY "${CHRONOS_SOURCE_DIR}"
      RESULT_VARIABLE CHRONOS_GIT_STATUS_RESULT
      OUTPUT_VARIABLE CHRONOS_GIT_STATUS
      OUTPUT_STRIP_TRAILING_WHITESPACE
      ERROR_QUIET
    )
    if(CHRONOS_GIT_STATUS_RESULT EQUAL 0 AND NOT CHRONOS_GIT_STATUS STREQUAL "")
      set(CHRONOS_GIT_DIRTY 1)
    endif()
  endif()
endif()

function(chronos_escape_cpp_string input output)
  set(value "${input}")
  string(REPLACE "\\" "\\\\" value "${value}")
  string(REPLACE "\"" "\\\"" value "${value}")
  string(REPLACE "\n" "\\n" value "${value}")
  string(REPLACE "\r" "\\r" value "${value}")
  set("${output}" "${value}" PARENT_SCOPE)
endfunction()

chronos_escape_cpp_string("${CHRONOS_VERSION}" CHRONOS_VERSION_ESCAPED)
chronos_escape_cpp_string("${CHRONOS_GIT_COMMIT}" CHRONOS_GIT_COMMIT_ESCAPED)
chronos_escape_cpp_string("${CHRONOS_BUILD_TYPE}" CHRONOS_BUILD_TYPE_ESCAPED)
chronos_escape_cpp_string("${CHRONOS_COMPILER}" CHRONOS_COMPILER_ESCAPED)
chronos_escape_cpp_string("${CHRONOS_TARGET_ARCHITECTURE}" CHRONOS_TARGET_ARCHITECTURE_ESCAPED)
chronos_escape_cpp_string("${CHRONOS_OPERATING_SYSTEM}" CHRONOS_OPERATING_SYSTEM_ESCAPED)

get_filename_component(CHRONOS_OUTPUT_DIRECTORY "${CHRONOS_OUTPUT_FILE}" DIRECTORY)
file(MAKE_DIRECTORY "${CHRONOS_OUTPUT_DIRECTORY}")
configure_file("${CHRONOS_TEMPLATE_FILE}" "${CHRONOS_OUTPUT_FILE}" @ONLY)
