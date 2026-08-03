if(NOT DEFINED CHRONOS_TEST_BINARY_DIR)
  message(FATAL_ERROR "CHRONOS_TEST_BINARY_DIR is required")
endif()

set(install_prefix "${CHRONOS_TEST_BINARY_DIR}/install-layout-test")
file(REMOVE_RECURSE "${install_prefix}")
execute_process(
  COMMAND
    "${CMAKE_COMMAND}" --install "${CHRONOS_TEST_BINARY_DIR}" --prefix "${install_prefix}"
  RESULT_VARIABLE install_result
)
if(NOT install_result EQUAL 0)
  message(FATAL_ERROR "ChronosDB staging install failed with status ${install_result}")
endif()

if(NOT DEFINED CHRONOS_TEST_INSTALL_INCLUDEDIR)
  message(FATAL_ERROR "CHRONOS_TEST_INSTALL_INCLUDEDIR is required")
endif()
if(NOT DEFINED CHRONOS_TEST_INSTALL_LIBDIR)
  message(FATAL_ERROR "CHRONOS_TEST_INSTALL_LIBDIR is required")
endif()

foreach(tool IN ITEMS chronosctl chronos-waldump chronos-walbench)
  set(installed_tool
      "${install_prefix}/${CHRONOS_TEST_INSTALL_BINDIR}/${tool}${CHRONOS_TEST_EXECUTABLE_SUFFIX}")
  if(NOT EXISTS "${installed_tool}")
    message(FATAL_ERROR "staging install omitted ${installed_tool}")
  endif()
endforeach()

set(installed_schema_header
    "${install_prefix}/${CHRONOS_TEST_INSTALL_INCLUDEDIR}/chronos/schema/table_schema.hpp")
if(NOT EXISTS "${installed_schema_header}")
  message(FATAL_ERROR "staging install omitted ${installed_schema_header}")
endif()

set(installed_targets
    "${install_prefix}/${CHRONOS_TEST_INSTALL_LIBDIR}/cmake/ChronosDB/ChronosTargets.cmake")
file(READ "${installed_targets}" installed_targets_contents)
string(FIND "${installed_targets_contents}" "chronos::schema" schema_target_offset)
if(schema_target_offset EQUAL -1)
  message(FATAL_ERROR "installed CMake package omitted chronos::schema")
endif()

set(installed_walbench
    "${install_prefix}/${CHRONOS_TEST_INSTALL_BINDIR}/chronos-walbench${CHRONOS_TEST_EXECUTABLE_SUFFIX}")
execute_process(
  COMMAND "${installed_walbench}" --help
  RESULT_VARIABLE walbench_help_result
  OUTPUT_QUIET
  ERROR_QUIET
)
if(NOT walbench_help_result EQUAL 0)
  message(FATAL_ERROR "installed chronos-walbench --help failed with status ${walbench_help_result}")
endif()
