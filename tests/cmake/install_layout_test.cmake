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

foreach(tool IN ITEMS chronosctl chronos-waldump chronos-walbench)
  set(installed_tool
      "${install_prefix}/${CHRONOS_TEST_INSTALL_BINDIR}/${tool}${CHRONOS_TEST_EXECUTABLE_SUFFIX}")
  if(NOT EXISTS "${installed_tool}")
    message(FATAL_ERROR "staging install omitted ${installed_tool}")
  endif()
endforeach()

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
