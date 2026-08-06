cmake_minimum_required(VERSION 3.25)

project(
  ChronosDB
  VERSION 0.1.0
  DESCRIPTION "A pre-alpha event-time analytical database"
  LANGUAGES CXX
)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(ChronosOptions)
include(ChronosWarnings)
include(ChronosSanitizers)
include(ChronosStaticAnalysis)
include(ChronosDependencies)
include(ChronosInstall)

chronos_validate_sanitizers()

function(chronos_validate_standard_library)
  include(CheckCXXSourceCompiles)
  set(CMAKE_CXX_STANDARD 23)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  check_cxx_source_compiles(
    "#include <expected>
     #ifndef __cpp_lib_expected
     #error std::expected is unavailable
     #endif
     int main() {
       std::expected<int, int> value{42};
       return value.value() == 42 ? 0 : 1;
     }"
    CHRONOS_HAS_CXX23_STD_EXPECTED
  )
  if(NOT CHRONOS_HAS_CXX23_STD_EXPECTED)
    message(
      FATAL_ERROR
        "ChronosDB requires C++23 std::expected, but ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION} with the selected C++ standard library does not provide it. Select a compatible standard library and configure a fresh build tree; on Ubuntu 24.04 with Clang, install libc++-dev and libc++abi-dev and set CXXFLAGS=-stdlib=libc++."
    )
  endif()
endfunction()

chronos_validate_standard_library()
find_package(Threads REQUIRED)
find_package(OpenSSL 3.0...<4.0 REQUIRED COMPONENTS Crypto)

set(CHRONOS_BUILD_TYPE "${CMAKE_BUILD_TYPE}")
if(CHRONOS_BUILD_TYPE STREQUAL "")
  set(CHRONOS_BUILD_TYPE "unknown")
endif()
set(CHRONOS_COMPILER "${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
set(CHRONOS_TARGET_ARCHITECTURE "${CMAKE_SYSTEM_PROCESSOR}")
if(CHRONOS_TARGET_ARCHITECTURE STREQUAL "")
  set(CHRONOS_TARGET_ARCHITECTURE "unknown")
endif()
set(CHRONOS_OPERATING_SYSTEM "${CMAKE_SYSTEM_NAME}")

set(CHRONOS_GENERATED_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY "${CHRONOS_GENERATED_INCLUDE_DIR}/chronos/common")
set(
  CHRONOS_VERSION_HEADER
  "${CHRONOS_GENERATED_INCLUDE_DIR}/chronos/common/version_config.hpp"
)
add_custom_target(
  chronos_version_header ALL
  COMMAND
    "${CMAKE_COMMAND}"
    "-DCHRONOS_SOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}"
    "-DCHRONOS_OUTPUT_FILE=${CHRONOS_VERSION_HEADER}"
    "-DCHRONOS_TEMPLATE_FILE=${CMAKE_CURRENT_SOURCE_DIR}/cmake/version.hpp.in"
    "-DCHRONOS_VERSION=${PROJECT_VERSION}"
    "-DCHRONOS_BUILD_TYPE=${CHRONOS_BUILD_TYPE}"
    "-DCHRONOS_COMPILER=${CHRONOS_COMPILER}"
    "-DCHRONOS_TARGET_ARCHITECTURE=${CHRONOS_TARGET_ARCHITECTURE}"
    "-DCHRONOS_OPERATING_SYSTEM=${CHRONOS_OPERATING_SYSTEM}"
    -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake/GenerateVersionHeader.cmake"
  BYPRODUCTS "${CHRONOS_VERSION_HEADER}"
  COMMENT "Refreshing ChronosDB version metadata"
  VERBATIM
)

add_library(
  chronos_common
  src/common/byte_reader.cpp
  src/common/byte_writer.cpp
  src/common/crc32c.cpp
  src/common/status.cpp
  src/common/version.cpp
)
add_library(chronos::common ALIAS chronos_common)
add_dependencies(chronos_common chronos_version_header)
set_target_properties(chronos_common PROPERTIES EXPORT_NAME common)
set_target_properties(chronos_common PROPERTIES CXX_EXTENSIONS OFF)
target_compile_features(chronos_common PUBLIC cxx_std_23)
target_include_directories(
  chronos_common
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<BUILD_INTERFACE:${CHRONOS_GENERATED_INCLUDE_DIR}>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
)
chronos_set_project_warnings(chronos_common)
chronos_enable_sanitizers(chronos_common)
chronos_enable_static_analysis(chronos_common)

add_library(
  chronos_schema
  src/schema/column_definition.cpp
  src/schema/identity.cpp
  src/schema/logical_type.cpp
  src/schema/schema_lineage.cpp
  src/schema/table_schema.cpp
  src/schema/utf8.cpp
)
add_library(chronos::schema ALIAS chronos_schema)
set_target_properties(chronos_schema PROPERTIES EXPORT_NAME schema)
set_target_properties(chronos_schema PROPERTIES CXX_EXTENSIONS OFF)
target_compile_features(chronos_schema PUBLIC cxx_std_23)
target_include_directories(
  chronos_schema
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
)
target_link_libraries(chronos_schema PUBLIC chronos::common)
chronos_set_project_warnings(chronos_schema)
chronos_enable_sanitizers(chronos_schema)
chronos_enable_static_analysis(chronos_schema)

add_library(
  chronos_columnar
  src/columnar/column_vector.cpp
  src/columnar/columnar_batch.cpp
  src/columnar/columnar_batch_codec.cpp
)
add_library(chronos::columnar ALIAS chronos_columnar)
set_target_properties(chronos_columnar PROPERTIES EXPORT_NAME columnar)
set_target_properties(chronos_columnar PROPERTIES CXX_EXTENSIONS OFF)
target_compile_features(chronos_columnar PUBLIC cxx_std_23)
target_include_directories(
  chronos_columnar
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
)
target_link_libraries(chronos_columnar PUBLIC chronos::schema)
chronos_set_project_warnings(chronos_columnar)
chronos_enable_sanitizers(chronos_columnar)
chronos_enable_static_analysis(chronos_columnar)

if(NOT CMAKE_SYSTEM_NAME MATCHES "^(Linux|Darwin)$")
  message(FATAL_ERROR "chronos_io currently supports only Linux and macOS")
endif()

add_library(chronos_io src/io/posix_io.cpp)
add_library(chronos::io ALIAS chronos_io)
set_target_properties(chronos_io PROPERTIES EXPORT_NAME io)
set_target_properties(chronos_io PROPERTIES CXX_EXTENSIONS OFF)
target_compile_features(chronos_io PUBLIC cxx_std_23)
target_include_directories(
  chronos_io
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src"
)
target_link_libraries(chronos_io PUBLIC chronos::common)
chronos_set_project_warnings(chronos_io)
chronos_enable_sanitizers(chronos_io)
chronos_enable_static_analysis(chronos_io)

add_library(
  chronos_wal
  src/wal/application.cpp
  src/wal/codec.cpp
  src/wal/wal_commit_coordinator.cpp
  src/wal/wal_log_id_generator.cpp
  src/wal/wal_paths.cpp
  src/wal/wal_recovery.cpp
  src/wal/wal_reopen.cpp
  src/wal/wal_scan.cpp
  src/wal/wal_segment.cpp
  src/wal/wal_writer.cpp
)
add_library(chronos::wal ALIAS chronos_wal)
set_target_properties(chronos_wal PROPERTIES EXPORT_NAME wal)
set_target_properties(chronos_wal PROPERTIES CXX_EXTENSIONS OFF)
target_compile_features(chronos_wal PUBLIC cxx_std_23)
target_include_directories(
  chronos_wal
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src"
)
target_link_libraries(chronos_wal PUBLIC chronos::common PRIVATE chronos::io Threads::Threads)
chronos_set_project_warnings(chronos_wal)
chronos_enable_sanitizers(chronos_wal)
chronos_enable_static_analysis(chronos_wal)

add_library(chronos_head src/head/mutable_head.cpp)
add_library(chronos::head ALIAS chronos_head)
set_target_properties(chronos_head PROPERTIES EXPORT_NAME head)
set_target_properties(chronos_head PROPERTIES CXX_EXTENSIONS OFF)
target_compile_features(chronos_head PUBLIC cxx_std_23)
target_include_directories(
  chronos_head
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
)
target_link_libraries(chronos_head PUBLIC chronos::columnar chronos::wal)
chronos_set_project_warnings(chronos_head)
chronos_enable_sanitizers(chronos_head)
chronos_enable_static_analysis(chronos_head)

add_library(
  chronos_ingest
  src/ingest/columnar_append.cpp
  src/ingest/columnar_append_executor.cpp
  src/ingest/columnar_append_recovery.cpp
  src/ingest/deduplication_key.cpp
  src/ingest/retry_directory.cpp
  src/ingest/sha256.cpp
  src/ingest/tablet_state.cpp
)
add_library(chronos::ingest ALIAS chronos_ingest)
set_target_properties(chronos_ingest PROPERTIES EXPORT_NAME ingest)
set_target_properties(chronos_ingest PROPERTIES CXX_EXTENSIONS OFF)
target_compile_features(chronos_ingest PUBLIC cxx_std_23)
target_include_directories(
  chronos_ingest
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
)
target_link_libraries(
  chronos_ingest
  PUBLIC chronos::columnar chronos::head chronos::wal
  PRIVATE OpenSSL::Crypto
)
chronos_set_project_warnings(chronos_ingest)
chronos_enable_sanitizers(chronos_ingest)
chronos_enable_static_analysis(chronos_ingest)

add_executable(chronosctl tools/chronosctl/main.cpp)
set_target_properties(chronosctl PROPERTIES CXX_EXTENSIONS OFF)
target_compile_features(chronosctl PRIVATE cxx_std_23)
target_link_libraries(chronosctl PRIVATE chronos::common)
chronos_set_project_warnings(chronosctl)
chronos_enable_sanitizers(chronosctl)
chronos_enable_static_analysis(chronosctl)

add_executable(chronos-waldump tools/chronos-waldump/main.cpp)
set_target_properties(chronos-waldump PROPERTIES CXX_EXTENSIONS OFF)
target_compile_features(chronos-waldump PRIVATE cxx_std_23)
target_link_libraries(chronos-waldump PRIVATE chronos::wal)
chronos_set_project_warnings(chronos-waldump)
chronos_enable_sanitizers(chronos-waldump)
chronos_enable_static_analysis(chronos-waldump)

add_executable(chronos-walbench tools/chronos-walbench/main.cpp)
set_target_properties(chronos-walbench PROPERTIES CXX_EXTENSIONS OFF)
target_compile_features(chronos-walbench PRIVATE cxx_std_23)
target_link_libraries(chronos-walbench PRIVATE chronos::wal Threads::Threads)
chronos_set_project_warnings(chronos-walbench)
chronos_enable_sanitizers(chronos-walbench)
chronos_enable_static_analysis(chronos-walbench)

if(CHRONOS_BUILD_TESTS)
  enable_testing()
  chronos_fetch_googletest()
  include(GoogleTest)

  add_executable(
    chronos_common_tests
    tests/common/byte_io_test.cpp
    tests/common/byte_reader_header_self_contained.cpp
    tests/common/byte_writer_header_self_contained.cpp
    tests/common/bytes_test.cpp
    tests/common/bytes_header_self_contained.cpp
    tests/common/checked_math_test.cpp
    tests/common/checked_math_header_self_contained.cpp
    tests/common/crc32c_test.cpp
    tests/common/crc32c_header_self_contained.cpp
    tests/common/result_header_self_contained.cpp
    tests/common/status_header_self_contained.cpp
    tests/common/status_result_test.cpp
    tests/common/uuid_header_self_contained.cpp
    tests/common/uuid_test.cpp
    tests/common/version_header_self_contained.cpp
    tests/common/version_test.cpp
  )
  set_target_properties(chronos_common_tests PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_common_tests PRIVATE cxx_std_23)
  target_link_libraries(chronos_common_tests PRIVATE chronos::common GTest::gtest_main)
  chronos_set_project_warnings(chronos_common_tests)
  chronos_enable_sanitizers(chronos_common_tests)
  chronos_enable_static_analysis(chronos_common_tests)
  gtest_discover_tests(chronos_common_tests DISCOVERY_TIMEOUT 30)

  add_executable(
    chronos_schema_tests
    tests/schema/column_definition_header_self_contained.cpp
    tests/schema/column_definition_test.cpp
    tests/schema/identity_header_self_contained.cpp
    tests/schema/identity_test.cpp
    tests/schema/logical_type_header_self_contained.cpp
    tests/schema/logical_type_test.cpp
    tests/schema/schema_lineage_header_self_contained.cpp
    tests/schema/schema_lineage_test.cpp
    tests/schema/table_schema_header_self_contained.cpp
    tests/schema/table_schema_test.cpp
    tests/schema/utf8_header_self_contained.cpp
    tests/schema/utf8_test.cpp
  )
  set_target_properties(chronos_schema_tests PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_schema_tests PRIVATE cxx_std_23)
  target_link_libraries(chronos_schema_tests PRIVATE chronos::schema GTest::gtest_main)
  chronos_set_project_warnings(chronos_schema_tests)
  chronos_enable_sanitizers(chronos_schema_tests)
  chronos_enable_static_analysis(chronos_schema_tests)
  gtest_discover_tests(chronos_schema_tests DISCOVERY_TIMEOUT 30)

  add_executable(
    chronos_columnar_tests
    tests/columnar/columnar_batch_codec_corruption_test.cpp
    tests/columnar/columnar_batch_codec_header_self_contained.cpp
    tests/columnar/columnar_batch_codec_property_test.cpp
    tests/columnar/columnar_batch_codec_test.cpp
    tests/columnar/columnar_batch_format_header_self_contained.cpp
    tests/columnar/column_vector_header_self_contained.cpp
    tests/columnar/column_vector_test.cpp
    tests/columnar/column_vector_property_test.cpp
    tests/columnar/columnar_batch_header_self_contained.cpp
    tests/columnar/columnar_batch_test.cpp
  )
  set_target_properties(chronos_columnar_tests PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_columnar_tests PRIVATE cxx_std_23)
  target_include_directories(chronos_columnar_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests")
  target_link_libraries(chronos_columnar_tests PRIVATE chronos::columnar GTest::gtest_main)
  chronos_set_project_warnings(chronos_columnar_tests)
  chronos_enable_sanitizers(chronos_columnar_tests)
  chronos_enable_static_analysis(chronos_columnar_tests)
  gtest_discover_tests(chronos_columnar_tests DISCOVERY_TIMEOUT 30)

  add_executable(
    chronos_io_tests
    tests/io/posix_io_header_self_contained.cpp
    tests/io/posix_io_test.cpp
  )
  set_target_properties(chronos_io_tests PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_io_tests PRIVATE cxx_std_23)
  target_include_directories(chronos_io_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src")
  target_link_libraries(chronos_io_tests PRIVATE chronos::io GTest::gtest_main)
  chronos_set_project_warnings(chronos_io_tests)
  chronos_enable_sanitizers(chronos_io_tests)
  chronos_enable_static_analysis(chronos_io_tests)
  gtest_discover_tests(chronos_io_tests DISCOVERY_TIMEOUT 30)

  add_executable(
    chronos_wal_tests
    tests/wal/application_header_self_contained.cpp
    tests/wal/application_test.cpp
    tests/wal/codec_header_self_contained.cpp
    tests/wal/codec_test.cpp
    tests/wal/types_header_self_contained.cpp
    tests/wal/wal_commit_coordinator_header_self_contained.cpp
    tests/wal/wal_commit_coordinator_test.cpp
    tests/wal/wal_corruption_matrix_test.cpp
    tests/wal/wal_corruption_test.cpp
    tests/wal/wal_crash_group_commit_test.cpp
    tests/wal/wal_crash_harness_test.cpp
    tests/wal/wal_crash_matrix_test.cpp
    tests/wal/wal_crash_reopen_test.cpp
    tests/wal/wal_discovery_test.cpp
    tests/wal/wal_append_result_header_self_contained.cpp
    tests/wal/wal_log_id_generator_header_self_contained.cpp
    tests/wal/wal_paths_header_self_contained.cpp
    tests/wal/wal_process_lock_test.cpp
    tests/wal/wal_recovery_header_self_contained.cpp
    tests/wal/wal_recovery_report_header_self_contained.cpp
    tests/wal/wal_recovery_idempotence_test.cpp
    tests/wal/wal_recovery_test.cpp
    tests/wal/wal_reopen_test.cpp
    tests/wal/wal_replay_sink_header_self_contained.cpp
    tests/wal/wal_replay_test.cpp
    tests/wal/wal_rotation_test.cpp
    tests/wal/wal_scan_header_self_contained.cpp
    tests/wal/wal_scan_test.cpp
    tests/wal/wal_segment_header_self_contained.cpp
    tests/wal/wal_segment_installation_test.cpp
    tests/wal/wal_writer_directory_test.cpp
    tests/wal/wal_writer_failure_test.cpp
    tests/wal/wal_writer_config_header_self_contained.cpp
    tests/wal/wal_writer_header_self_contained.cpp
    tests/wal/wal_writer_test.cpp
    tests/wal/wal_tail_repair_test.cpp
    tests/wal/waldump_test.cpp
  )

  add_executable(chronos_wal_crash_child tests/wal/wal_crash_child.cpp)
  set_target_properties(chronos_wal_crash_child PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_wal_crash_child PRIVATE cxx_std_23)
  target_include_directories(
    chronos_wal_crash_child PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src"
                                    "${CMAKE_CURRENT_SOURCE_DIR}/tests"
  )
  target_link_libraries(chronos_wal_crash_child PRIVATE chronos::wal Threads::Threads)
  chronos_set_project_warnings(chronos_wal_crash_child)
  chronos_enable_sanitizers(chronos_wal_crash_child)
  chronos_enable_static_analysis(chronos_wal_crash_child)

  set_target_properties(chronos_wal_tests PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_wal_tests PRIVATE cxx_std_23)
  target_include_directories(chronos_wal_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src"
                                                       "${CMAKE_CURRENT_SOURCE_DIR}/tests")
  target_link_libraries(chronos_wal_tests PRIVATE chronos::wal GTest::gtest_main)
  target_compile_definitions(
    chronos_wal_tests
    PRIVATE CHRONOS_WALDUMP_PATH="$<TARGET_FILE:chronos-waldump>"
            CHRONOS_WAL_CRASH_CHILD_PATH="$<TARGET_FILE:chronos_wal_crash_child>"
  )
  add_dependencies(chronos_wal_tests chronos-waldump chronos_wal_crash_child)
  chronos_set_project_warnings(chronos_wal_tests)
  chronos_enable_sanitizers(chronos_wal_tests)
  chronos_enable_static_analysis(chronos_wal_tests)
  gtest_discover_tests(chronos_wal_tests DISCOVERY_TIMEOUT 30 NO_PRETTY_VALUES)

  add_executable(
    chronos_ingest_tests
    tests/ingest/columnar_append_corruption_test.cpp
    tests/ingest/columnar_append_executor_header_self_contained.cpp
    tests/ingest/columnar_append_executor_test.cpp
    tests/ingest/columnar_append_format_header_self_contained.cpp
    tests/ingest/columnar_append_header_self_contained.cpp
    tests/ingest/columnar_append_recovery_header_self_contained.cpp
    tests/ingest/columnar_append_recovery_test.cpp
    tests/ingest/columnar_append_property_test.cpp
    tests/ingest/columnar_append_test.cpp
    tests/ingest/deduplication_key_test.cpp
    tests/ingest/identity_header_self_contained.cpp
    tests/ingest/identity_test.cpp
    tests/ingest/retry_directory_header_self_contained.cpp
    tests/ingest/retry_directory_test.cpp
    tests/ingest/sha256_header_self_contained.cpp
    tests/ingest/sha256_test.cpp
    tests/ingest/tablet_state_header_self_contained.cpp
    tests/ingest/tablet_state_test.cpp
  )
  set_target_properties(chronos_ingest_tests PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_ingest_tests PRIVATE cxx_std_23)
  target_include_directories(
    chronos_ingest_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src"
                                 "${CMAKE_CURRENT_SOURCE_DIR}/tests"
  )
  target_link_libraries(chronos_ingest_tests PRIVATE chronos::ingest GTest::gtest_main)
  chronos_set_project_warnings(chronos_ingest_tests)
  chronos_enable_sanitizers(chronos_ingest_tests)
  chronos_enable_static_analysis(chronos_ingest_tests)
  gtest_discover_tests(chronos_ingest_tests DISCOVERY_TIMEOUT 30)

  add_executable(
    chronos_ingest_allocation_failure_tests
    tests/ingest/allocation_failure_test.cpp
    tests/support/failing_allocator.cpp
  )
  set_target_properties(chronos_ingest_allocation_failure_tests PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_ingest_allocation_failure_tests PRIVATE cxx_std_23)
  target_include_directories(
    chronos_ingest_allocation_failure_tests
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests"
  )
  target_link_libraries(
    chronos_ingest_allocation_failure_tests PRIVATE chronos::ingest GTest::gtest_main
  )
  chronos_set_project_warnings(chronos_ingest_allocation_failure_tests)
  chronos_enable_sanitizers(chronos_ingest_allocation_failure_tests)
  chronos_enable_static_analysis(chronos_ingest_allocation_failure_tests)
  gtest_discover_tests(chronos_ingest_allocation_failure_tests DISCOVERY_TIMEOUT 30)

  add_executable(
    chronos_head_tests
    tests/head/mutable_head_header_self_contained.cpp
    tests/head/mutable_head_test.cpp
  )
  set_target_properties(chronos_head_tests PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_head_tests PRIVATE cxx_std_23)
  target_include_directories(
    chronos_head_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src"
                               "${CMAKE_CURRENT_SOURCE_DIR}/tests"
  )
  target_link_libraries(chronos_head_tests PRIVATE chronos::head GTest::gtest_main)
  chronos_set_project_warnings(chronos_head_tests)
  chronos_enable_sanitizers(chronos_head_tests)
  chronos_enable_static_analysis(chronos_head_tests)
  gtest_discover_tests(chronos_head_tests DISCOVERY_TIMEOUT 30)

  add_test(
    NAME InstallLayoutTest.IncludesOperatorAndBenchmarkTools
    COMMAND
      "${CMAKE_COMMAND}"
      "-DCHRONOS_TEST_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}"
      "-DCHRONOS_TEST_INSTALL_BINDIR=${CMAKE_INSTALL_BINDIR}"
      "-DCHRONOS_TEST_INSTALL_INCLUDEDIR=${CMAKE_INSTALL_INCLUDEDIR}"
      "-DCHRONOS_TEST_INSTALL_LIBDIR=${CMAKE_INSTALL_LIBDIR}"
      "-DCHRONOS_TEST_EXECUTABLE_SUFFIX=${CMAKE_EXECUTABLE_SUFFIX}"
      -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/cmake/install_layout_test.cmake"
  )

  add_test(
    NAME WalBenchmarkSmokeTest.ModesAndArtifacts
    COMMAND
      "${CMAKE_COMMAND}"
      "-DCHRONOS_WALBENCH=$<TARGET_FILE:chronos-walbench>"
      "-DCHRONOS_TEST_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}"
      -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/cmake/walbench_smoke_test.cmake"
  )

  add_test(
    NAME ExternalConsumerTest.InstalledIngestTarget
    COMMAND
      "${CMAKE_COMMAND}"
      "-DCHRONOS_TEST_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}"
      "-DCHRONOS_TEST_INSTALL_LIBDIR=${CMAKE_INSTALL_LIBDIR}"
      "-DCHRONOS_TEST_GENERATOR=${CMAKE_GENERATOR}"
      "-DCHRONOS_TEST_EXECUTABLE_SUFFIX=${CMAKE_EXECUTABLE_SUFFIX}"
      "-DCHRONOS_TEST_ENABLE_ASAN=${CHRONOS_ENABLE_ASAN}"
      "-DCHRONOS_TEST_ENABLE_UBSAN=${CHRONOS_ENABLE_UBSAN}"
      "-DCHRONOS_TEST_ENABLE_TSAN=${CHRONOS_ENABLE_TSAN}"
      -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/cmake/external_consumer_test.cmake"
  )

  if(UNIX)
    find_program(CHRONOS_BASH_EXECUTABLE bash REQUIRED)
    add_test(
      NAME BuildMetadataTest.RefreshesGitStateWithoutReconfigure
      COMMAND
        "${CHRONOS_BASH_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/tests/build/version_metadata_test.sh"
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${CMAKE_COMMAND}"
    )
  endif()

  if(CHRONOS_ENABLE_UBSAN AND CMAKE_EXPORT_COMPILE_COMMANDS)
    add_test(
      NAME BuildConfigurationTest.UbsanIsNonRecovering
      COMMAND
        "${CMAKE_COMMAND}"
        "-DCOMPILE_COMMANDS=${CMAKE_BINARY_DIR}/compile_commands.json"
        "-DSOURCE_ROOT=${CMAKE_SOURCE_DIR}"
        -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/build/check_ubsan_nonrecovering.cmake"
    )
  endif()
endif()

if(CHRONOS_BUILD_BENCHMARKS)
  chronos_fetch_google_benchmark()
  add_executable(
    chronos_common_benchmarks
    benchmarks/common/binary_foundations_benchmark.cpp
    benchmarks/common/version_benchmark.cpp
  )
  set_target_properties(chronos_common_benchmarks PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_common_benchmarks PRIVATE cxx_std_23)
  target_link_libraries(chronos_common_benchmarks PRIVATE chronos::common benchmark::benchmark_main)
  chronos_set_project_warnings(chronos_common_benchmarks)
  chronos_enable_sanitizers(chronos_common_benchmarks)
  chronos_enable_static_analysis(chronos_common_benchmarks)

  add_executable(
    chronos_columnar_benchmarks
    benchmarks/columnar/columnar_batch_codec_benchmark.cpp
  )
  set_target_properties(chronos_columnar_benchmarks PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_columnar_benchmarks PRIVATE cxx_std_23)
  target_link_libraries(
    chronos_columnar_benchmarks PRIVATE chronos::columnar benchmark::benchmark_main
  )
  chronos_set_project_warnings(chronos_columnar_benchmarks)
  chronos_enable_sanitizers(chronos_columnar_benchmarks)
  chronos_enable_static_analysis(chronos_columnar_benchmarks)

  add_executable(
    chronos_ingest_benchmarks
    benchmarks/ingest/columnar_append_benchmark.cpp
    benchmarks/ingest/columnar_append_executor_benchmark.cpp
    benchmarks/ingest/columnar_append_recovery_benchmark.cpp
    benchmarks/ingest/tablet_state_benchmark.cpp
  )
  set_target_properties(chronos_ingest_benchmarks PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_ingest_benchmarks PRIVATE cxx_std_23)
  target_link_libraries(
    chronos_ingest_benchmarks PRIVATE chronos::ingest benchmark::benchmark_main
  )
  chronos_set_project_warnings(chronos_ingest_benchmarks)
  chronos_enable_sanitizers(chronos_ingest_benchmarks)
  chronos_enable_static_analysis(chronos_ingest_benchmarks)

  add_executable(
    chronos_head_benchmarks
    benchmarks/head/mutable_head_benchmark.cpp
  )
  set_target_properties(chronos_head_benchmarks PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_head_benchmarks PRIVATE cxx_std_23)
  target_link_libraries(
    chronos_head_benchmarks PRIVATE chronos::head benchmark::benchmark_main
  )
  chronos_set_project_warnings(chronos_head_benchmarks)
  chronos_enable_sanitizers(chronos_head_benchmarks)
  chronos_enable_static_analysis(chronos_head_benchmarks)
endif()

if(CHRONOS_BUILD_FUZZERS)
  if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang|AppleClang")
    message(FATAL_ERROR "CHRONOS_BUILD_FUZZERS requires Clang or AppleClang with libFuzzer")
  endif()
  if(CHRONOS_ENABLE_TSAN)
    message(FATAL_ERROR "libFuzzer targets cannot be combined with ThreadSanitizer")
  endif()

  include(CheckCXXSourceCompiles)
  set(CHRONOS_SAVED_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS}")
  set(CHRONOS_SAVED_REQUIRED_LINK_OPTIONS "${CMAKE_REQUIRED_LINK_OPTIONS}")
  set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} -fsanitize=fuzzer")
  list(APPEND CMAKE_REQUIRED_LINK_OPTIONS -fsanitize=fuzzer)
  check_cxx_source_compiles(
    "#include <cstddef>
     #include <cstdint>
     extern \"C\" int LLVMFuzzerTestOneInput(const std::uint8_t*, std::size_t) { return 0; }"
    CHRONOS_HAS_LIBFUZZER_RUNTIME
  )
  set(CMAKE_REQUIRED_FLAGS "${CHRONOS_SAVED_REQUIRED_FLAGS}")
  set(CMAKE_REQUIRED_LINK_OPTIONS "${CHRONOS_SAVED_REQUIRED_LINK_OPTIONS}")
  if(NOT CHRONOS_HAS_LIBFUZZER_RUNTIME)
    message(
      FATAL_ERROR
        "CHRONOS_BUILD_FUZZERS=ON, but ${CMAKE_CXX_COMPILER} cannot compile and link a libFuzzer target; install a Clang distribution with its libFuzzer runtime and configure a fresh build"
    )
  endif()

  add_executable(chronos_byte_reader_fuzz tests/fuzz/byte_reader_fuzz.cpp)
  set_target_properties(chronos_byte_reader_fuzz PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_byte_reader_fuzz PRIVATE cxx_std_23)
  target_link_libraries(chronos_byte_reader_fuzz PRIVATE chronos::common)
  target_compile_options(chronos_byte_reader_fuzz PRIVATE -fsanitize=fuzzer)
  target_link_options(chronos_byte_reader_fuzz PRIVATE -fsanitize=fuzzer)
  chronos_set_project_warnings(chronos_byte_reader_fuzz)
  chronos_enable_sanitizers(chronos_byte_reader_fuzz)
  chronos_enable_static_analysis(chronos_byte_reader_fuzz)

  add_executable(chronos_wal_codec_fuzz tests/fuzz/wal_codec_fuzz.cpp)
  set_target_properties(chronos_wal_codec_fuzz PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_wal_codec_fuzz PRIVATE cxx_std_23)
  target_link_libraries(chronos_wal_codec_fuzz PRIVATE chronos::wal)
  target_compile_options(chronos_wal_codec_fuzz PRIVATE -fsanitize=fuzzer)
  target_link_options(chronos_wal_codec_fuzz PRIVATE -fsanitize=fuzzer)
  chronos_set_project_warnings(chronos_wal_codec_fuzz)
  chronos_enable_sanitizers(chronos_wal_codec_fuzz)
  chronos_enable_static_analysis(chronos_wal_codec_fuzz)

  add_executable(chronos_columnar_batch_codec_fuzz tests/fuzz/columnar_batch_codec_fuzz.cpp)
  set_target_properties(chronos_columnar_batch_codec_fuzz PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_columnar_batch_codec_fuzz PRIVATE cxx_std_23)
  target_link_libraries(chronos_columnar_batch_codec_fuzz PRIVATE chronos::columnar)
  target_compile_options(chronos_columnar_batch_codec_fuzz PRIVATE -fsanitize=fuzzer)
  target_link_options(chronos_columnar_batch_codec_fuzz PRIVATE -fsanitize=fuzzer)
  chronos_set_project_warnings(chronos_columnar_batch_codec_fuzz)
  chronos_enable_sanitizers(chronos_columnar_batch_codec_fuzz)
  chronos_enable_static_analysis(chronos_columnar_batch_codec_fuzz)

  add_executable(chronos_columnar_append_fuzz tests/fuzz/columnar_append_fuzz.cpp)
  set_target_properties(chronos_columnar_append_fuzz PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_columnar_append_fuzz PRIVATE cxx_std_23)
  target_link_libraries(chronos_columnar_append_fuzz PRIVATE chronos::ingest)
  target_compile_options(chronos_columnar_append_fuzz PRIVATE -fsanitize=fuzzer)
  target_link_options(chronos_columnar_append_fuzz PRIVATE -fsanitize=fuzzer)
  chronos_set_project_warnings(chronos_columnar_append_fuzz)
  chronos_enable_sanitizers(chronos_columnar_append_fuzz)
  chronos_enable_static_analysis(chronos_columnar_append_fuzz)
endif()

chronos_configure_installation()
