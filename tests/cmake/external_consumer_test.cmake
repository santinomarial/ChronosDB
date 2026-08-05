if(NOT DEFINED CHRONOS_TEST_BINARY_DIR OR NOT DEFINED CHRONOS_TEST_INSTALL_LIBDIR)
  message(FATAL_ERROR "external consumer test requires the build and install library directories")
endif()

set(install_prefix "${CHRONOS_TEST_BINARY_DIR}/external-consumer-install")
set(consumer_source "${CHRONOS_TEST_BINARY_DIR}/external-consumer-source")
set(consumer_build "${CHRONOS_TEST_BINARY_DIR}/external-consumer-build")
file(REMOVE_RECURSE "${install_prefix}" "${consumer_source}" "${consumer_build}")

execute_process(
  COMMAND "${CMAKE_COMMAND}" --install "${CHRONOS_TEST_BINARY_DIR}" --prefix "${install_prefix}"
  RESULT_VARIABLE install_result
)
if(NOT install_result EQUAL 0)
  message(FATAL_ERROR "external consumer staging install failed with status ${install_result}")
endif()

file(MAKE_DIRECTORY "${consumer_source}")
file(WRITE "${consumer_source}/CMakeLists.txt" [=[
cmake_minimum_required(VERSION 3.25)
project(ChronosColumnarConsumer LANGUAGES CXX)
find_package(ChronosDB 0.1 CONFIG REQUIRED)
add_executable(consumer main.cpp)
target_link_libraries(consumer PRIVATE chronos::columnar)
target_compile_features(consumer PRIVATE cxx_std_23)
]=])
file(WRITE "${consumer_source}/main.cpp" [=[
#include <chronos/columnar/columnar_batch.hpp>
#include <chronos/columnar/columnar_batch_codec.hpp>
#include <chronos/columnar/columnar_batch_format.hpp>
#include <chronos/columnar/column_vector.hpp>

#include <array>

int main() {
  static_assert(chronos::columnar::bitmap_size(9U) == 2U);
  static_assert(chronos::columnar::format::kBatchHeaderLength == 96U);
  chronos::columnar::ColumnarBatchLimits limits;
  std::array<std::byte, 0> empty{};
  const auto decoded = chronos::columnar::decode_columnar_batch_v1_exact(empty);
  return limits.max_columns == 4096U && !decoded.has_value() &&
                 decoded.error().kind() ==
                     chronos::columnar::ColumnarBatchDecodeErrorKind::kIncomplete
             ? 0
             : 1;
}
]=])

set(configure_command
    "${CMAKE_COMMAND}" -S "${consumer_source}" -B "${consumer_build}"
    "-DChronosDB_DIR=${install_prefix}/${CHRONOS_TEST_INSTALL_LIBDIR}/cmake/ChronosDB")
if(DEFINED CHRONOS_TEST_GENERATOR AND NOT CHRONOS_TEST_GENERATOR STREQUAL "")
  list(APPEND configure_command -G "${CHRONOS_TEST_GENERATOR}")
endif()
execute_process(COMMAND ${configure_command} RESULT_VARIABLE configure_result)
if(NOT configure_result EQUAL 0)
  message(FATAL_ERROR "installed external consumer configure failed with status ${configure_result}")
endif()
execute_process(
  COMMAND "${CMAKE_COMMAND}" --build "${consumer_build}"
  RESULT_VARIABLE build_result
)
if(NOT build_result EQUAL 0)
  message(FATAL_ERROR "installed external consumer build failed with status ${build_result}")
endif()
execute_process(
  COMMAND "${consumer_build}/consumer${CHRONOS_TEST_EXECUTABLE_SUFFIX}"
  RESULT_VARIABLE run_result
)
if(NOT run_result EQUAL 0)
  message(FATAL_ERROR "installed external consumer execution failed with status ${run_result}")
endif()
