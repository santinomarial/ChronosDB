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

set(CHRONOS_GIT_COMMIT "unknown")
set(CHRONOS_GIT_METADATA_AVAILABLE 0)
set(CHRONOS_GIT_DIRTY 0)

find_package(Git QUIET)
if(Git_FOUND)
  execute_process(
    COMMAND "${GIT_EXECUTABLE}" rev-parse --short=12 HEAD
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
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
      WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
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

function(chronos_escape_cpp_string input output)
  set(value "${input}")
  string(REPLACE "\\" "\\\\" value "${value}")
  string(REPLACE "\"" "\\\"" value "${value}")
  string(REPLACE "\n" "\\n" value "${value}")
  string(REPLACE "\r" "\\r" value "${value}")
  set("${output}" "${value}" PARENT_SCOPE)
endfunction()

chronos_escape_cpp_string("${PROJECT_VERSION}" CHRONOS_VERSION_ESCAPED)
chronos_escape_cpp_string("${CHRONOS_GIT_COMMIT}" CHRONOS_GIT_COMMIT_ESCAPED)
chronos_escape_cpp_string("${CHRONOS_BUILD_TYPE}" CHRONOS_BUILD_TYPE_ESCAPED)
chronos_escape_cpp_string("${CHRONOS_COMPILER}" CHRONOS_COMPILER_ESCAPED)
chronos_escape_cpp_string("${CHRONOS_TARGET_ARCHITECTURE}" CHRONOS_TARGET_ARCHITECTURE_ESCAPED)
chronos_escape_cpp_string("${CHRONOS_OPERATING_SYSTEM}" CHRONOS_OPERATING_SYSTEM_ESCAPED)

set(CHRONOS_GENERATED_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY "${CHRONOS_GENERATED_INCLUDE_DIR}/chronos/common")
configure_file(
  cmake/version.hpp.in
  "${CHRONOS_GENERATED_INCLUDE_DIR}/chronos/common/version_config.hpp"
  @ONLY
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

add_executable(chronosctl tools/chronosctl/main.cpp)
set_target_properties(chronosctl PROPERTIES CXX_EXTENSIONS OFF)
target_compile_features(chronosctl PRIVATE cxx_std_23)
target_link_libraries(chronosctl PRIVATE chronos::common)
chronos_set_project_warnings(chronosctl)
chronos_enable_sanitizers(chronosctl)
chronos_enable_static_analysis(chronosctl)

if(CHRONOS_BUILD_TESTS)
  enable_testing()
  chronos_fetch_googletest()
  include(GoogleTest)

  add_executable(
    chronos_common_tests
    tests/common/byte_io_test.cpp
    tests/common/byte_writer_header_self_contained.cpp
    tests/common/bytes_test.cpp
    tests/common/checked_math_test.cpp
    tests/common/crc32c_test.cpp
    tests/common/status_result_test.cpp
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
endif()

if(CHRONOS_BUILD_BENCHMARKS)
  chronos_fetch_google_benchmark()
  add_executable(chronos_common_benchmarks benchmarks/common/version_benchmark.cpp)
  set_target_properties(chronos_common_benchmarks PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_common_benchmarks PRIVATE cxx_std_23)
  target_link_libraries(chronos_common_benchmarks PRIVATE chronos::common benchmark::benchmark_main)
  chronos_set_project_warnings(chronos_common_benchmarks)
  chronos_enable_sanitizers(chronos_common_benchmarks)
  chronos_enable_static_analysis(chronos_common_benchmarks)
endif()

if(CHRONOS_BUILD_FUZZERS)
  if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang|AppleClang")
    message(FATAL_ERROR "CHRONOS_BUILD_FUZZERS requires Clang or AppleClang with libFuzzer")
  endif()
  if(CHRONOS_ENABLE_TSAN)
    message(FATAL_ERROR "libFuzzer targets cannot be combined with ThreadSanitizer")
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
endif()

chronos_configure_installation()
