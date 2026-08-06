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
project(ChronosIngestConsumer LANGUAGES CXX)
find_package(ChronosDB 0.1 CONFIG REQUIRED)
add_executable(consumer main.cpp)
target_link_libraries(consumer PRIVATE chronos::cseg chronos::head chronos::ingest)
target_compile_features(consumer PRIVATE cxx_std_23)
set(consumer_sanitizers "")
if(CHRONOS_TEST_ENABLE_ASAN)
  list(APPEND consumer_sanitizers address)
endif()
if(CHRONOS_TEST_ENABLE_UBSAN)
  list(APPEND consumer_sanitizers undefined)
endif()
if(CHRONOS_TEST_ENABLE_TSAN)
  list(APPEND consumer_sanitizers thread)
endif()
if(consumer_sanitizers)
  list(JOIN consumer_sanitizers "," consumer_sanitizer_flags)
  target_compile_options(consumer PRIVATE "-fsanitize=${consumer_sanitizer_flags}")
  target_link_options(consumer PRIVATE "-fsanitize=${consumer_sanitizer_flags}")
endif()
]=])
file(WRITE "${consumer_source}/main.cpp" [=[
#include <chronos/columnar/columnar_batch.hpp>
#include <chronos/columnar/columnar_batch_codec.hpp>
#include <chronos/columnar/columnar_batch_format.hpp>
#include <chronos/columnar/column_vector.hpp>
#include <chronos/cseg/format.hpp>
#include <chronos/cseg/layout.hpp>
#include <chronos/cseg/types.hpp>
#include <chronos/head/mutable_head.hpp>
#include <chronos/ingest/columnar_append.hpp>
#include <chronos/ingest/columnar_append_executor.hpp>
#include <chronos/ingest/columnar_append_format.hpp>
#include <chronos/ingest/columnar_append_recovery.hpp>
#include <chronos/ingest/identity.hpp>
#include <chronos/ingest/retry_directory.hpp>
#include <chronos/ingest/sha256.hpp>
#include <chronos/ingest/tablet_state.hpp>
#include <chronos/wal/application.hpp>

#include <array>
#include <cstdint>
#include <memory>

int main() {
  using ExecuteFunction = chronos::common::Result<chronos::ingest::ColumnarAppendExecutionResult> (*)(
      const chronos::ingest::ColumnarAppendExecutionInput&, chronos::ingest::RetryDirectory&,
      chronos::ingest::TabletState&, chronos::wal::WalCommitCoordinator&);
  const ExecuteFunction execute = &chronos::ingest::execute_columnar_append;
  using RecoverFunction =
      chronos::common::Result<chronos::ingest::RecoveredColumnarAppendState> (*)(
          const chronos::wal::WalWriterConfig&, const chronos::wal::WalRecoveryOptions&,
          chronos::ingest::ColumnarAppendRecoveryConfig);
  const RecoverFunction recover = &chronos::ingest::recover_columnar_append_wal;
  using RegisterSchemaFunction = chronos::common::Status (chronos::ingest::TabletState::*)(
      std::shared_ptr<const chronos::schema::TableSchema>, chronos::head::MutableHeadCapacity);
  const RegisterSchemaFunction register_schema = &chronos::ingest::TabletState::register_schema;
  static_assert(chronos::columnar::bitmap_size(9U) == 2U);
  static_assert(chronos::columnar::format::kBatchHeaderLength == 96U);
  static_assert(chronos::cseg::format::kFileHeaderLength == 256U);
  const std::array<std::uint64_t, 5> page_lengths{8U, 8U, 8U, 8U, 8U};
  const auto cseg_layout = chronos::cseg::plan_cseg_v1_layout(
      {.user_column_count = 1U, .granule_count = 1U}, page_lengths);
  chronos::columnar::ColumnarBatchLimits limits;
  std::array<std::byte, 0> empty{};
  const auto decoded = chronos::columnar::decode_columnar_batch_v1_exact(empty);
  const auto digest = chronos::ingest::sha256(chronos::common::ByteView{});
  const auto retry_directory = chronos::ingest::RetryDirectory::create(
      chronos::ingest::RetryDirectoryConfig{.maximum_entries = 8U});
  const chronos::head::MutableHeadCapacity head_capacity{.row_capacity = 4U,
                                                        .variable_value_bytes = {}};
  const chronos::ingest::TabletStateConfig tablet_config{
      .head_capacity = head_capacity,
      .maximum_schema_versions = 2U,
      .maximum_sealed_generations = 2U,
      .maximum_retry_entries = 8U};
  static_assert(chronos::ingest::columnar_append_v1::kCommandHeaderLength == 160U);
  return execute != nullptr && recover != nullptr && register_schema != nullptr &&
                 limits.max_columns == 4096U &&
                 head_capacity.row_capacity == 4U &&
                 tablet_config.maximum_schema_versions == 2U &&
                 tablet_config.maximum_sealed_generations == 2U && digest.has_value() &&
                 retry_directory.has_value() &&
                 retry_directory->metrics().maximum_entries == 8U && !decoded.has_value() &&
                 cseg_layout.has_value() && cseg_layout->total_length == 1'248U &&
                 decoded.error().kind() ==
                     chronos::columnar::ColumnarBatchDecodeErrorKind::kIncomplete
             ? 0
             : 1;
}
]=])

set(configure_command
    "${CMAKE_COMMAND}" -S "${consumer_source}" -B "${consumer_build}"
    "-DChronosDB_DIR=${install_prefix}/${CHRONOS_TEST_INSTALL_LIBDIR}/cmake/ChronosDB"
    "-DCHRONOS_TEST_ENABLE_ASAN=${CHRONOS_TEST_ENABLE_ASAN}"
    "-DCHRONOS_TEST_ENABLE_UBSAN=${CHRONOS_TEST_ENABLE_UBSAN}"
    "-DCHRONOS_TEST_ENABLE_TSAN=${CHRONOS_TEST_ENABLE_TSAN}")
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
