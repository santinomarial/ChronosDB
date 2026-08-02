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
  src/wal/codec.cpp
  src/wal/wal_log_id_generator.cpp
  src/wal/wal_paths.cpp
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
target_link_libraries(chronos_wal PUBLIC chronos::common PRIVATE chronos::io)
chronos_set_project_warnings(chronos_wal)
chronos_enable_sanitizers(chronos_wal)
chronos_enable_static_analysis(chronos_wal)

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
    tests/wal/codec_header_self_contained.cpp
    tests/wal/codec_test.cpp
    tests/wal/types_header_self_contained.cpp
    tests/wal/wal_append_result_header_self_contained.cpp
    tests/wal/wal_log_id_generator_header_self_contained.cpp
    tests/wal/wal_paths_header_self_contained.cpp
    tests/wal/wal_rotation_test.cpp
    tests/wal/wal_segment_header_self_contained.cpp
    tests/wal/wal_segment_installation_test.cpp
    tests/wal/wal_writer_directory_test.cpp
    tests/wal/wal_writer_failure_test.cpp
    tests/wal/wal_writer_config_header_self_contained.cpp
    tests/wal/wal_writer_header_self_contained.cpp
    tests/wal/wal_writer_test.cpp
  )
  set_target_properties(chronos_wal_tests PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_wal_tests PRIVATE cxx_std_23)
  target_include_directories(chronos_wal_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src"
                                                       "${CMAKE_CURRENT_SOURCE_DIR}/tests")
  target_link_libraries(chronos_wal_tests PRIVATE chronos::wal GTest::gtest_main)
  chronos_set_project_warnings(chronos_wal_tests)
  chronos_enable_sanitizers(chronos_wal_tests)
  chronos_enable_static_analysis(chronos_wal_tests)
  gtest_discover_tests(chronos_wal_tests DISCOVERY_TIMEOUT 30)

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
endif()

chronos_configure_installation()
