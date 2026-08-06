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

set(installed_columnar_header
    "${install_prefix}/${CHRONOS_TEST_INSTALL_INCLUDEDIR}/chronos/columnar/columnar_batch.hpp")
if(NOT EXISTS "${installed_columnar_header}")
  message(FATAL_ERROR "staging install omitted ${installed_columnar_header}")
endif()

set(installed_columnar_codec_header
    "${install_prefix}/${CHRONOS_TEST_INSTALL_INCLUDEDIR}/chronos/columnar/columnar_batch_codec.hpp")
if(NOT EXISTS "${installed_columnar_codec_header}")
  message(FATAL_ERROR "staging install omitted ${installed_columnar_codec_header}")
endif()

set(installed_cseg_layout_header
    "${install_prefix}/${CHRONOS_TEST_INSTALL_INCLUDEDIR}/chronos/cseg/layout.hpp")
if(NOT EXISTS "${installed_cseg_layout_header}")
  message(FATAL_ERROR "staging install omitted ${installed_cseg_layout_header}")
endif()

set(installed_cseg_compression_header
    "${install_prefix}/${CHRONOS_TEST_INSTALL_INCLUDEDIR}/chronos/cseg/compression.hpp")
if(NOT EXISTS "${installed_cseg_compression_header}")
  message(FATAL_ERROR "staging install omitted ${installed_cseg_compression_header}")
endif()

set(installed_ingest_header
    "${install_prefix}/${CHRONOS_TEST_INSTALL_INCLUDEDIR}/chronos/ingest/columnar_append.hpp")
if(NOT EXISTS "${installed_ingest_header}")
  message(FATAL_ERROR "staging install omitted ${installed_ingest_header}")
endif()
set(installed_ingest_recovery_header
    "${install_prefix}/${CHRONOS_TEST_INSTALL_INCLUDEDIR}/chronos/ingest/columnar_append_recovery.hpp")
if(NOT EXISTS "${installed_ingest_recovery_header}")
  message(FATAL_ERROR "staging install omitted ${installed_ingest_recovery_header}")
endif()

set(installed_head_header
    "${install_prefix}/${CHRONOS_TEST_INSTALL_INCLUDEDIR}/chronos/head/mutable_head.hpp")
if(NOT EXISTS "${installed_head_header}")
  message(FATAL_ERROR "staging install omitted ${installed_head_header}")
endif()

set(installed_targets
    "${install_prefix}/${CHRONOS_TEST_INSTALL_LIBDIR}/cmake/ChronosDB/ChronosTargets.cmake")
file(READ "${installed_targets}" installed_targets_contents)
string(FIND "${installed_targets_contents}" "chronos::schema" schema_target_offset)
if(schema_target_offset EQUAL -1)
  message(FATAL_ERROR "installed CMake package omitted chronos::schema")
endif()

set(installed_package_config
    "${install_prefix}/${CHRONOS_TEST_INSTALL_LIBDIR}/cmake/ChronosDB/ChronosDBConfig.cmake")
if(NOT EXISTS "${installed_package_config}")
  message(FATAL_ERROR "staging install omitted ${installed_package_config}")
endif()
set(installed_package_version
    "${install_prefix}/${CHRONOS_TEST_INSTALL_LIBDIR}/cmake/ChronosDB/ChronosDBConfigVersion.cmake")
if(NOT EXISTS "${installed_package_version}")
  message(FATAL_ERROR "staging install omitted ${installed_package_version}")
endif()

string(FIND "${installed_targets_contents}" "chronos::columnar" columnar_target_offset)
if(columnar_target_offset EQUAL -1)
  message(FATAL_ERROR "installed CMake package omitted chronos::columnar")
endif()

string(FIND "${installed_targets_contents}" "chronos::cseg" cseg_target_offset)
if(cseg_target_offset EQUAL -1)
  message(FATAL_ERROR "installed CMake package omitted chronos::cseg")
endif()

string(FIND "${installed_targets_contents}" "chronos::ingest" ingest_target_offset)
if(ingest_target_offset EQUAL -1)
  message(FATAL_ERROR "installed CMake package omitted chronos::ingest")
endif()

string(FIND "${installed_targets_contents}" "chronos::head" head_target_offset)
if(head_target_offset EQUAL -1)
  message(FATAL_ERROR "installed CMake package omitted chronos::head")
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
