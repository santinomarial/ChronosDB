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
find_package(CURL 7.75 REQUIRED)
set(CHRONOS_RESOLVED_CURL_VERSION "${CURL_VERSION}")
if(CHRONOS_RESOLVED_CURL_VERSION STREQUAL "")
  set(CHRONOS_RESOLVED_CURL_VERSION "${CURL_VERSION_STRING}")
endif()
if(CHRONOS_RESOLVED_CURL_VERSION VERSION_GREATER_EQUAL 9.0)
  message(FATAL_ERROR "ChronosDB has not reviewed libcurl ${CHRONOS_RESOLVED_CURL_VERSION}")
endif()
find_package(OpenSSL 3.0...<4.0 REQUIRED COMPONENTS Crypto SSL)
find_package(zstd 1.5.5...<2.0 CONFIG REQUIRED)
if(NOT TARGET zstd::libzstd)
  if(TARGET zstd::libzstd_shared)
    add_library(zstd::libzstd ALIAS zstd::libzstd_shared)
  elseif(TARGET zstd::libzstd_static)
    add_library(zstd::libzstd ALIAS zstd::libzstd_static)
  else()
    message(FATAL_ERROR "The resolved zstd package exports no supported library target")
  endif()
endif()
if(CHRONOS_ENABLE_ARROW_INTEROP)
  find_package(Arrow CONFIG REQUIRED)
  find_package(Parquet CONFIG REQUIRED)
  if(ARROW_VERSION VERSION_LESS 25.0 OR ARROW_VERSION VERSION_GREATER_EQUAL 26.0)
    message(FATAL_ERROR "ChronosDB requires Apache Arrow 25.x")
  endif()
  if(PARQUET_VERSION VERSION_LESS 25.0 OR PARQUET_VERSION VERSION_GREATER_EQUAL 26.0)
    message(FATAL_ERROR "ChronosDB requires Apache Parquet 25.x")
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
  src/common/log.cpp
  src/common/status.cpp
  src/common/time_source.cpp
  src/common/uuid_generator.cpp
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

if(CHRONOS_ENABLE_ARROW_INTEROP)
  add_library(chronos_interop src/interop/arrow_parquet.cpp)
  add_library(chronos::interop ALIAS chronos_interop)
  set_target_properties(chronos_interop PROPERTIES EXPORT_NAME interop CXX_EXTENSIONS OFF)
  target_compile_features(chronos_interop PUBLIC cxx_std_23)
  target_include_directories(
    chronos_interop
    PUBLIC
      "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
      "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
  )
  target_link_libraries(
    chronos_interop
    PUBLIC chronos::columnar
    PRIVATE Arrow::arrow_shared Parquet::parquet_shared
  )
  chronos_set_project_warnings(chronos_interop)
  chronos_enable_sanitizers(chronos_interop)
  chronos_enable_static_analysis(chronos_interop)
endif()

add_library(
  chronos_cseg
  src/cseg/compression.cpp
  src/cseg/inspection.cpp
  src/cseg/layout.cpp
  src/cseg/metadata_codec.cpp
  src/cseg/part_codec.cpp
  src/cseg/page_codec.cpp
  src/cseg/plain_page.cpp
  src/cseg/projected_reader.cpp
  src/cseg/pruning.cpp
  src/cseg/sort_order_internal.cpp
  src/cseg/system_rows_internal.cpp
  src/cseg/temporal_layout.cpp
  src/cseg/temporal_system_row.cpp
  src/cseg/validator.cpp
)
add_library(chronos::cseg ALIAS chronos_cseg)
set_target_properties(chronos_cseg PROPERTIES EXPORT_NAME cseg)
set_target_properties(chronos_cseg PROPERTIES CXX_EXTENSIONS OFF)
target_compile_features(chronos_cseg PUBLIC cxx_std_23)
target_include_directories(
  chronos_cseg
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
)
target_link_libraries(
  chronos_cseg
  PUBLIC chronos::columnar chronos::common chronos::schema
  PRIVATE zstd::libzstd
)
chronos_set_project_warnings(chronos_cseg)
chronos_enable_sanitizers(chronos_cseg)
chronos_enable_static_analysis(chronos_cseg)

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
  src/ingest/async_raft_tablet_application.cpp
  src/ingest/columnar_append.cpp
  src/ingest/columnar_append_executor.cpp
  src/ingest/columnar_append_recovery.cpp
  src/ingest/committed_columnar_append.cpp
  src/ingest/deduplication_key.cpp
  src/ingest/raft_tablet_snapshot.cpp
  src/ingest/raft_tablet_snapshot_storage.cpp
  src/ingest/raft_tablet_state_machine.cpp
  src/ingest/retry_directory.cpp
  src/ingest/sealed_head_flush_queue.cpp
  src/ingest/sha256.cpp
  src/ingest/tablet_movement_catch_up_checkpoint.cpp
  src/ingest/tablet_movement_raft_snapshot_completion.cpp
  src/ingest/tablet_movement_snapshot_handoff.cpp
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
  PUBLIC chronos::columnar chronos::head chronos::raft chronos::wal
  PRIVATE OpenSSL::Crypto chronos::io
)
chronos_set_project_warnings(chronos_ingest)
chronos_enable_sanitizers(chronos_ingest)
chronos_enable_static_analysis(chronos_ingest)

add_library(
  chronos_manifest
  src/manifest/checkpoint_builder.cpp
  src/manifest/codec.cpp
  src/manifest/compaction.cpp
  src/manifest/compaction_coordinator.cpp
  src/manifest/compaction_equivalence.cpp
  src/manifest/compaction_planner.cpp
  src/manifest/generation_builder.cpp
  src/manifest/layout.cpp
  src/manifest/naming.cpp
  src/manifest/part_validation.cpp
  src/manifest/publication.cpp
  src/manifest/raft_tablet_physical_snapshot.cpp
  src/manifest/sealed_head_flush.cpp
  src/manifest/sealed_head_flush_coordinator.cpp
  src/manifest/startup_recovery.cpp
  src/manifest/storage.cpp
  src/manifest/temporal_codec.cpp
  src/manifest/temporal_layout.cpp
  src/manifest/temporal_part_validation.cpp
  src/manifest/temporal_publication.cpp
  src/manifest/temporal_validation.cpp
  src/manifest/validation.cpp
)
add_library(chronos::manifest ALIAS chronos_manifest)
set_target_properties(chronos_manifest PROPERTIES EXPORT_NAME manifest)
set_target_properties(chronos_manifest PROPERTIES CXX_EXTENSIONS OFF)
target_compile_features(chronos_manifest PUBLIC cxx_std_23)
target_include_directories(
  chronos_manifest
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src"
)
target_link_libraries(
  chronos_manifest
  PUBLIC chronos::cseg chronos::head chronos::ingest chronos::schema chronos::wal
  PRIVATE chronos::io
)
chronos_set_project_warnings(chronos_manifest)
chronos_enable_sanitizers(chronos_manifest)
chronos_enable_static_analysis(chronos_manifest)

add_library(
  chronos_runtime
  src/runtime/database_bootstrap.cpp
  src/runtime/thread_placement.cpp
)
add_library(chronos::runtime ALIAS chronos_runtime)
set_target_properties(chronos_runtime PROPERTIES EXPORT_NAME runtime CXX_EXTENSIONS OFF)
target_compile_features(chronos_runtime PUBLIC cxx_std_23)
target_include_directories(
  chronos_runtime
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
)
target_link_libraries(chronos_runtime PUBLIC chronos::common PRIVATE chronos::io)
chronos_set_project_warnings(chronos_runtime)
chronos_enable_sanitizers(chronos_runtime)
chronos_enable_static_analysis(chronos_runtime)

add_library(
  chronos_query
  src/query/asof_join.cpp
  src/query/aggregate.cpp
  src/query/ast.cpp
  src/query/binder.cpp
  src/query/catalog.cpp
  src/query/columnar_batch_scan.cpp
  src/query/column_output.cpp
  src/query/committed_temporal_command.cpp
  src/query/cseg_scan.cpp
  src/query/database_cseg_scan.cpp
  src/query/decimal_internal.cpp
  src/query/diagnostic.cpp
  src/query/distributed.cpp
  src/query/distributed_grouped_exchange.cpp
  src/query/distributed_vector_aggregate_coordinator.cpp
  src/query/distributed_vector_aggregate_exchange.cpp
  src/query/distributed_vector_aggregate_state.cpp
  src/query/distributed_vector_exchange.cpp
  src/query/distributed_vector_fragment.cpp
  src/query/distributed_vector_fragment_v2.cpp
  src/query/distributed_vector_plan.cpp
  src/query/distributed_vector_result_schema.cpp
  src/query/distributed_fragment.cpp
  src/query/distributed_fragment_binding.cpp
  src/query/distributed_fragment_dispatch.cpp
  src/query/distributed_fragment_worker.cpp
  src/query/evaluator.cpp
  src/query/executor.cpp
  src/query/explain.cpp
  src/query/head_scan.cpp
  src/query/insert_batch.cpp
  src/query/lexer.cpp
  src/query/latest.cpp
  src/query/literal.cpp
  src/query/parser.cpp
  src/query/parallel_scheduler.cpp
  src/query/physical_operator.cpp
  src/query/physical_optimizer.cpp
  src/query/physical_lowering.cpp
  src/query/physical_plan.cpp
  src/query/relational_plan.cpp
  src/query/resource_context.cpp
  src/query/row_version.cpp
  src/query/scalar_snapshot_scan.cpp
  src/query/snapshot.cpp
  src/query/snapshot_pipeline.cpp
  src/query/snapshot_shape.cpp
  src/query/sort.cpp
  src/query/spill_sort.cpp
  src/query/statement_binder.cpp
  src/query/temporal_command.cpp
  src/query/temporal_command_executor.cpp
  src/query/temporal_cseg_snapshot.cpp
  src/query/temporal_recovery.cpp
  src/query/temporal_snapshot.cpp
  src/query/tablet_state_pipeline.cpp
  src/query/timestamp_filter_kernel.cpp
  src/query/value.cpp
  src/query/vector_chunk.cpp
  src/query/vector_expression.cpp
)
add_library(chronos::query ALIAS chronos_query)
set_target_properties(chronos_query PROPERTIES EXPORT_NAME query CXX_EXTENSIONS OFF)
target_compile_features(chronos_query PUBLIC cxx_std_23)
target_include_directories(
  chronos_query
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src"
)
target_link_libraries(
  chronos_query
  PUBLIC chronos::columnar chronos::common chronos::cseg chronos::head chronos::ingest chronos::io
         chronos::manifest chronos::runtime chronos::schema chronos::wal
  PRIVATE Threads::Threads
)
chronos_set_project_warnings(chronos_query)
chronos_enable_sanitizers(chronos_query)
chronos_enable_static_analysis(chronos_query)

add_library(
  chronos_network
  src/network/client_session.cpp
  src/network/connection_buffers.cpp
  src/network/connection_state.cpp
  src/network/epoll_reactor.cpp
  src/network/io_uring_reactor.cpp
  src/network/messages.cpp
  src/network/protocol.cpp
  src/network/reactor.cpp
  src/network/security.cpp
  src/network/spsc_queue.cpp
  src/network/subscription_messages.cpp
  src/network/tcp_socket.cpp
  src/network/tls_socket.cpp
)
add_library(chronos::network ALIAS chronos_network)
set_target_properties(chronos_network PROPERTIES EXPORT_NAME network CXX_EXTENSIONS OFF)
target_compile_features(chronos_network PUBLIC cxx_std_23)
target_include_directories(
  chronos_network
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
)
target_link_libraries(
  chronos_network
  PUBLIC chronos::common chronos::schema
  PRIVATE OpenSSL::Crypto OpenSSL::SSL
)
chronos_set_project_warnings(chronos_network)
chronos_enable_sanitizers(chronos_network)
chronos_enable_static_analysis(chronos_network)
if(CHRONOS_ENABLE_IO_URING)
  if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "CHRONOS_ENABLE_IO_URING requires Linux")
  endif()
  find_path(CHRONOS_LIBURING_INCLUDE_DIR liburing.h REQUIRED)
  find_library(CHRONOS_LIBURING_LIBRARY uring REQUIRED)
  target_include_directories(chronos_network PRIVATE "${CHRONOS_LIBURING_INCLUDE_DIR}")
  target_link_libraries(chronos_network PRIVATE "${CHRONOS_LIBURING_LIBRARY}")
  target_compile_definitions(chronos_network PUBLIC CHRONOS_HAS_LIBURING=1)
endif()

add_library(
  chronos_live
  src/live/bound_materialized_view_checkpoint.cpp
  src/live/committed_batch_evaluator.cpp
  src/live/durable_multi_tablet_subscription.cpp
  src/live/durable_materialized_view.cpp
  src/live/incremental_aggregate.cpp
  src/live/materialized_view.cpp
  src/live/materialized_view_checkpoint.cpp
  src/live/materialized_view_checkpoint_storage.cpp
  src/live/multi_tablet_subscription.cpp
  src/live/multi_tablet_subscription_checkpoint.cpp
  src/live/multi_tablet_subscription_checkpoint_storage.cpp
  src/live/multi_tablet_snapshot_subscription.cpp
  src/live/raft_subscription_reclaimer.cpp
  src/live/resume_token.cpp
  src/live/snapshot_subscription.cpp
  src/live/subscription.cpp
  src/live/subscription_plan.cpp
  src/live/subscription_plan_definition.cpp
  src/live/subscription_plan_storage.cpp
  src/live/subscription_protocol.cpp
  src/live/subscription_retention.cpp
  src/live/subscription_service.cpp
  src/live/wal_subscription_reclaimer.cpp
)
add_library(chronos::live ALIAS chronos_live)
set_target_properties(chronos_live PROPERTIES EXPORT_NAME live CXX_EXTENSIONS OFF)
target_compile_features(chronos_live PUBLIC cxx_std_23)
target_include_directories(
  chronos_live
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
)
target_link_libraries(
  chronos_live
  PUBLIC chronos::common
         chronos::ingest
         chronos::manifest
         chronos::network
         chronos::query
         chronos::raft
         chronos::schema
         chronos::wal
  PRIVATE OpenSSL::Crypto
)
chronos_set_project_warnings(chronos_live)
chronos_enable_sanitizers(chronos_live)
chronos_enable_static_analysis(chronos_live)

add_library(
  chronos_raft
  src/raft/async_durable_runtime.cpp
  src/raft/async_durable_worker_extension_set.cpp
  src/raft/async_metadata_application.cpp
  src/raft/deterministic_simulator.cpp
  src/raft/durable_runtime.cpp
  src/raft/durable_tablet_reconfiguration.cpp
  src/raft/membership.cpp
  src/raft/metadata.cpp
  src/raft/metadata_codec.cpp
  src/raft/metadata_snapshot.cpp
  src/raft/metadata_snapshot_storage.cpp
  src/raft/metadata_runtime.cpp
  src/raft/multi_raft.cpp
  src/raft/multiplexed_log.cpp
  src/raft/node.cpp
  src/raft/persistent_log.cpp
  src/raft/runtime_timer.cpp
  src/raft/runtime_timer_driver.cpp
  src/raft/rebalancing.cpp
  src/raft/schema_definition_codec.cpp
  src/raft/tablet_reconfiguration.cpp
  src/raft/tablet_reconfiguration_action_codec.cpp
  src/raft/tablet_reconfiguration_action_ledger.cpp
  src/raft/tablet_group_binding_codec.cpp
  src/raft/tablet_movement_checkpoint.cpp
  src/raft/tablet_movement_checkpoint_recovery.cpp
  src/raft/tablet_movement_checkpoint_reference.cpp
  src/raft/tablet_movement_checkpoint_storage.cpp
  src/raft/tablet_movement_snapshot_chunk.cpp
  src/raft/tablet_movement_snapshot_chunk_storage.cpp
  src/raft/transport_codec.cpp
)
add_library(chronos::raft ALIAS chronos_raft)
set_target_properties(chronos_raft PROPERTIES EXPORT_NAME raft CXX_EXTENSIONS OFF)
target_compile_features(chronos_raft PUBLIC cxx_std_23)
target_include_directories(
  chronos_raft
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
)
target_link_libraries(
  chronos_raft PUBLIC chronos::common chronos::schema PRIVATE OpenSSL::Crypto chronos::io)
chronos_set_project_warnings(chronos_raft)
chronos_enable_sanitizers(chronos_raft)
chronos_enable_static_analysis(chronos_raft)

add_library(
  chronos_cluster
  src/cluster/distributed_grouped_query_execution.cpp
  src/cluster/distributed_grouped_query_tcp_execution.cpp
  src/cluster/distributed_grouped_query_tcp_client.cpp
  src/cluster/distributed_grouped_query_tcp_server.cpp
  src/cluster/distributed_grouped_query_tls.cpp
  src/cluster/distributed_grouped_query_transport.cpp
  src/cluster/distributed_vector_aggregate_finalization_v2.cpp
  src/cluster/distributed_vector_aggregate_query_execution_v2.cpp
  src/cluster/distributed_vector_aggregate_query_tcp_client_v2.cpp
  src/cluster/distributed_vector_aggregate_query_tcp_execution_v2.cpp
  src/cluster/distributed_vector_aggregate_query_tcp_server_v2.cpp
  src/cluster/distributed_vector_aggregate_query_tls_v2.cpp
  src/cluster/distributed_vector_aggregate_query_transport_v2.cpp
  src/cluster/distributed_vector_row_finalization_v2.cpp
  src/cluster/distributed_vector_result_exchange.cpp
  src/cluster/distributed_vector_query_execution_v2.cpp
  src/cluster/distributed_vector_query_tcp_execution_v2.cpp
  src/cluster/distributed_vector_query_transport.cpp
  src/cluster/distributed_vector_query_transport_v2.cpp
  src/cluster/distributed_vector_query_tls_v2.cpp
  src/cluster/distributed_vector_query_tcp_client_v2.cpp
  src/cluster/distributed_vector_query_tcp_server_v2.cpp
  src/cluster/distributed_query_execution.cpp
  src/cluster/distributed_query_tcp_client.cpp
  src/cluster/distributed_query_tcp_execution.cpp
  src/cluster/distributed_query_tcp_server.cpp
  src/cluster/distributed_query_tls_client.cpp
  src/cluster/distributed_query_tls_server.cpp
  src/cluster/distributed_query_transport.cpp
  src/cluster/raft_observation_transport.cpp
  src/cluster/raft_observation_tls_client.cpp
  src/cluster/raft_observation_tcp_client.cpp
  src/cluster/raft_observation_tcp_acquisition.cpp
  src/cluster/raft_observation_tcp_pair_acquisition.cpp
  src/cluster/raft_observation_tcp_batch_acquisition.cpp
  src/cluster/raft_observation_tls_server.cpp
  src/cluster/raft_observation_tcp_server.cpp
  src/cluster/raft_transport_receiver.cpp
  src/cluster/raft_transport_peer_pool.cpp
  src/cluster/raft_transport_peer_reconnect.cpp
  src/cluster/raft_transport_peer_manager.cpp
  src/cluster/raft_transport_runtime.cpp
  src/cluster/raft_transport_tcp_server.cpp
  src/cluster/raft_transport_tcp_connector.cpp
  src/cluster/raft_transport_tls_client.cpp
  src/cluster/raft_transport_tls_server.cpp
  src/cluster/remote_tablet_reconfiguration.cpp
  src/cluster/tablet_physical_part_install.cpp
  src/cluster/tablet_physical_movement_readiness.cpp
  src/cluster/tablet_physical_part_chunk.cpp
  src/cluster/tablet_physical_part_chunk_storage.cpp
  src/cluster/tablet_physical_receipt_reclamation.cpp
  src/cluster/tablet_physical_snapshot_ownership.cpp
)
add_library(chronos::cluster ALIAS chronos_cluster)
set_target_properties(chronos_cluster PROPERTIES EXPORT_NAME cluster CXX_EXTENSIONS OFF)
target_compile_features(chronos_cluster PUBLIC cxx_std_23)
target_include_directories(
  chronos_cluster
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
)
target_link_libraries(
  chronos_cluster
  PUBLIC chronos::cseg chronos::ingest chronos::manifest chronos::network chronos::query chronos::raft
  PRIVATE chronos::io
)
chronos_set_project_warnings(chronos_cluster)
chronos_enable_sanitizers(chronos_cluster)
chronos_enable_static_analysis(chronos_cluster)

add_library(
  chronos_tiering
  src/tiering/cold_manifest.cpp
  src/tiering/cold_manifest_storage.cpp
  src/tiering/object_store.cpp
  src/tiering/tiered_distributed_fragment_worker.cpp
  src/tiering/tiered_pair_commit.cpp
  src/tiering/tiered_part_loader.cpp
  src/tiering/tiered_publication.cpp
  src/tiering/tiered_reclamation.cpp
  src/tiering/tiered_parts.cpp
)
add_library(chronos::tiering ALIAS chronos_tiering)
set_target_properties(chronos_tiering PROPERTIES EXPORT_NAME tiering CXX_EXTENSIONS OFF)
target_compile_features(chronos_tiering PUBLIC cxx_std_23)
target_include_directories(
  chronos_tiering
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src"
)
target_link_libraries(
  chronos_tiering
  PUBLIC chronos::common
         chronos::cseg
         chronos::ingest
         chronos::manifest
         chronos::query
         chronos::schema
  PRIVATE chronos::io CURL::libcurl
)
chronos_set_project_warnings(chronos_tiering)
chronos_enable_sanitizers(chronos_tiering)
chronos_enable_static_analysis(chronos_tiering)

add_library(
  chronos_service
  src/service/replicated_distributed_grouped_query_receiver.cpp
  src/service/replicated_distributed_grouped_query_tcp_server.cpp
  src/service/native_protocol_service.cpp
  src/service/replicated_distributed_query.cpp
  src/service/replicated_distributed_query_tcp_server.cpp
  src/service/replicated_distributed_query_worker.cpp
  src/service/replicated_distributed_vector_aggregate_query_tcp_server_v2.cpp
  src/service/replicated_distributed_vector_query_tcp_server_v2.cpp
  src/service/replicated_ingest_coordinator.cpp
  src/service/replicated_ingest_database.cpp
  src/service/replicated_ingest_operation.cpp
  src/service/replicated_group_config.cpp
  src/service/replicated_peer_authority.cpp
  src/service/replicated_peer_config.cpp
  src/service/replicated_read_barrier.cpp
  src/service/replicated_raft_transport_runtime.cpp
  src/service/replicated_ingest_runtime.cpp
  src/service/replicated_ingest_service.cpp
  src/service/single_node_committed_append_router.cpp
  src/service/single_node_database.cpp
  src/service/single_node_live_append_fanout.cpp
  src/service/single_node_subscription_runtime.cpp
)
add_library(chronos::service ALIAS chronos_service)
set_target_properties(chronos_service PROPERTIES EXPORT_NAME service CXX_EXTENSIONS OFF)
target_compile_features(chronos_service PUBLIC cxx_std_23)
target_include_directories(
  chronos_service
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
)
target_link_libraries(
  chronos_service
  PUBLIC chronos::ingest
         chronos::cluster
         chronos::live
         chronos::network
         chronos::query
         chronos::raft
         chronos::runtime
         chronos::schema
         chronos::wal
  PRIVATE chronos::io
)
chronos_set_project_warnings(chronos_service)
chronos_enable_sanitizers(chronos_service)
chronos_enable_static_analysis(chronos_service)

add_executable(chronosctl tools/chronosctl/main.cpp)
set_target_properties(chronosctl PROPERTIES CXX_EXTENSIONS OFF)
target_compile_features(chronosctl PRIVATE cxx_std_23)
target_link_libraries(chronosctl PRIVATE chronos::common)
chronos_set_project_warnings(chronosctl)
chronos_enable_sanitizers(chronosctl)
chronos_enable_static_analysis(chronosctl)

add_executable(chronosd tools/chronosd/main.cpp)
set_target_properties(chronosd PROPERTIES CXX_EXTENSIONS OFF)
target_compile_features(chronosd PRIVATE cxx_std_23)
target_link_libraries(chronosd PRIVATE chronos::service OpenSSL::Crypto Threads::Threads)
chronos_set_project_warnings(chronosd)
chronos_enable_sanitizers(chronosd)
chronos_enable_static_analysis(chronosd)

add_executable(chronos-waldump tools/chronos-waldump/main.cpp)
set_target_properties(chronos-waldump PROPERTIES CXX_EXTENSIONS OFF)
target_compile_features(chronos-waldump PRIVATE cxx_std_23)
target_link_libraries(chronos-waldump PRIVATE chronos::wal)
chronos_set_project_warnings(chronos-waldump)
chronos_enable_sanitizers(chronos-waldump)
chronos_enable_static_analysis(chronos-waldump)

add_executable(chronos-csegdump tools/chronos-csegdump/main.cpp)
set_target_properties(chronos-csegdump PROPERTIES CXX_EXTENSIONS OFF)
target_compile_features(chronos-csegdump PRIVATE cxx_std_23)
target_link_libraries(chronos-csegdump PRIVATE chronos::cseg)
chronos_set_project_warnings(chronos-csegdump)
chronos_enable_sanitizers(chronos-csegdump)
chronos_enable_static_analysis(chronos-csegdump)

add_executable(chronos-walbench tools/chronos-walbench/main.cpp)
set_target_properties(chronos-walbench PROPERTIES CXX_EXTENSIONS OFF)
target_compile_features(chronos-walbench PRIVATE cxx_std_23)
target_link_libraries(chronos-walbench PRIVATE chronos::wal Threads::Threads)
chronos_set_project_warnings(chronos-walbench)
chronos_enable_sanitizers(chronos-walbench)
chronos_enable_static_analysis(chronos-walbench)

add_executable(chronos-flushbench tools/chronos-flushbench/main.cpp)
set_target_properties(chronos-flushbench PROPERTIES CXX_EXTENSIONS OFF)
target_compile_features(chronos-flushbench PRIVATE cxx_std_23)
target_link_libraries(chronos-flushbench PRIVATE chronos::manifest Threads::Threads)
chronos_set_project_warnings(chronos-flushbench)
chronos_enable_sanitizers(chronos-flushbench)
chronos_enable_static_analysis(chronos-flushbench)

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
    tests/common/log_header_self_contained.cpp
    tests/common/log_test.cpp
    tests/common/result_header_self_contained.cpp
    tests/common/status_header_self_contained.cpp
    tests/common/status_result_test.cpp
    tests/common/time_source_header_self_contained.cpp
    tests/common/time_source_test.cpp
    tests/common/uuid_header_self_contained.cpp
    tests/common/uuid_test.cpp
    tests/common/uuid_generator_header_self_contained.cpp
    tests/common/uuid_generator_test.cpp
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
    chronos_network_tests
    tests/network/client_session_header_self_contained.cpp
    tests/network/client_session_test.cpp
    tests/network/connection_buffers_header_self_contained.cpp
    tests/network/connection_buffers_test.cpp
    tests/network/connection_state_header_self_contained.cpp
    tests/network/connection_state_test.cpp
    tests/network/epoll_reactor_header_self_contained.cpp
    tests/network/epoll_reactor_test.cpp
    tests/network/io_uring_reactor_header_self_contained.cpp
    tests/network/io_uring_reactor_test.cpp
    tests/network/messages_header_self_contained.cpp
    tests/network/messages_test.cpp
    tests/network/packet_fixture_test.cpp
    tests/network/protocol_header_self_contained.cpp
    tests/network/protocol_test.cpp
    tests/network/reactor_header_self_contained.cpp
    tests/network/reactor_test.cpp
    tests/network/security_header_self_contained.cpp
    tests/network/security_test.cpp
    tests/network/spsc_queue_header_self_contained.cpp
    tests/network/spsc_queue_test.cpp
    tests/network/subscription_messages_header_self_contained.cpp
    tests/network/subscription_messages_test.cpp
    tests/network/tcp_socket_header_self_contained.cpp
    tests/network/tcp_socket_test.cpp
    tests/network/tls_socket_header_self_contained.cpp
    tests/network/tls_socket_test.cpp
  )
  set_target_properties(chronos_network_tests PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_network_tests PRIVATE cxx_std_23)
  target_link_libraries(
    chronos_network_tests
    PRIVATE chronos::network GTest::gtest_main OpenSSL::SSL
  )
  target_compile_definitions(
    chronos_network_tests
    PRIVATE CHRONOS_NETWORK_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/network"
  )
  chronos_set_project_warnings(chronos_network_tests)
  chronos_enable_sanitizers(chronos_network_tests)
  chronos_enable_static_analysis(chronos_network_tests)
  gtest_discover_tests(chronos_network_tests DISCOVERY_TIMEOUT 30)

  add_executable(
    chronos_runtime_tests
    tests/runtime/database_bootstrap_header_self_contained.cpp
    tests/runtime/database_bootstrap_test.cpp
    tests/runtime/thread_placement_header_self_contained.cpp
    tests/runtime/thread_placement_test.cpp
  )
  set_target_properties(chronos_runtime_tests PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_runtime_tests PRIVATE cxx_std_23)
  target_link_libraries(chronos_runtime_tests PRIVATE chronos::runtime GTest::gtest_main)
  chronos_set_project_warnings(chronos_runtime_tests)
  chronos_enable_sanitizers(chronos_runtime_tests)
  chronos_enable_static_analysis(chronos_runtime_tests)
  gtest_discover_tests(chronos_runtime_tests DISCOVERY_TIMEOUT 30)

  add_executable(
    chronos_service_tests
    tests/service/native_protocol_service_header_self_contained.cpp
    tests/service/replicated_distributed_grouped_query_tcp_server_header_self_contained.cpp
    tests/service/replicated_distributed_query_header_self_contained.cpp
    tests/service/replicated_distributed_query_test.cpp
    tests/service/replicated_distributed_query_tcp_server_header_self_contained.cpp
    tests/service/replicated_distributed_query_worker_header_self_contained.cpp
    tests/service/replicated_distributed_query_worker_test.cpp
    tests/service/replicated_distributed_vector_aggregate_query_tcp_server_v2_header_self_contained.cpp
    tests/service/replicated_distributed_vector_query_tcp_server_v2_header_self_contained.cpp
    tests/service/replicated_ingest_coordinator_header_self_contained.cpp
    tests/service/replicated_ingest_database_header_self_contained.cpp
    tests/service/replicated_ingest_database_test.cpp
    tests/service/replicated_ingest_operation_header_self_contained.cpp
    tests/service/replicated_ingest_operation_test.cpp
    tests/service/replicated_group_config_header_self_contained.cpp
    tests/service/replicated_group_config_test.cpp
    tests/service/replicated_peer_authority_header_self_contained.cpp
    tests/service/replicated_peer_authority_test.cpp
    tests/service/replicated_peer_config_header_self_contained.cpp
    tests/service/replicated_peer_config_test.cpp
    tests/service/replicated_read_barrier_header_self_contained.cpp
    tests/service/replicated_read_barrier_test.cpp
    tests/service/replicated_raft_transport_runtime_header_self_contained.cpp
    tests/service/replicated_raft_transport_runtime_test.cpp
    tests/service/replicated_ingest_runtime_header_self_contained.cpp
    tests/service/replicated_ingest_runtime_test.cpp
    tests/service/replicated_ingest_service_header_self_contained.cpp
    tests/service/replicated_ingest_service_test.cpp
    tests/service/single_node_committed_append_router_header_self_contained.cpp
    tests/service/single_node_committed_append_router_test.cpp
    tests/service/single_node_live_append_fanout_header_self_contained.cpp
    tests/service/single_node_live_append_fanout_test.cpp
    tests/service/single_node_subscription_runtime_header_self_contained.cpp
    tests/service/single_node_subscription_runtime_test.cpp
    tests/service/single_node_database_header_self_contained.cpp
    tests/service/single_node_database_test.cpp
  )
  set_target_properties(chronos_service_tests PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_service_tests PRIVATE cxx_std_23)
  target_include_directories(chronos_service_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests")
  target_compile_definitions(
    chronos_service_tests
    PRIVATE CHRONOS_NETWORK_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/network"
  )
  target_link_libraries(chronos_service_tests PRIVATE chronos::service GTest::gtest_main)
  chronos_set_project_warnings(chronos_service_tests)
  chronos_enable_sanitizers(chronos_service_tests)
  chronos_enable_static_analysis(chronos_service_tests)
  gtest_discover_tests(chronos_service_tests DISCOVERY_TIMEOUT 30)

  add_executable(
    chronos_service_allocation_failure_tests
    tests/service/replicated_distributed_query_allocation_failure_test.cpp
    tests/support/failing_allocator.cpp
  )
  set_target_properties(
    chronos_service_allocation_failure_tests PROPERTIES CXX_EXTENSIONS OFF
  )
  target_compile_features(chronos_service_allocation_failure_tests PRIVATE cxx_std_23)
  target_include_directories(
    chronos_service_allocation_failure_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests"
  )
  target_compile_definitions(
    chronos_service_allocation_failure_tests
    PRIVATE CHRONOS_NETWORK_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/network"
  )
  target_link_libraries(
    chronos_service_allocation_failure_tests PRIVATE chronos::service GTest::gtest_main
  )
  chronos_set_project_warnings(chronos_service_allocation_failure_tests)
  chronos_enable_sanitizers(chronos_service_allocation_failure_tests)
  chronos_enable_static_analysis(chronos_service_allocation_failure_tests)
  gtest_discover_tests(chronos_service_allocation_failure_tests DISCOVERY_TIMEOUT 30)

  add_executable(
    chronos_network_allocation_failure_tests
    tests/network/protocol_allocation_failure_test.cpp
    tests/support/failing_allocator.cpp
  )
  set_target_properties(chronos_network_allocation_failure_tests PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_network_allocation_failure_tests PRIVATE cxx_std_23)
  target_include_directories(
    chronos_network_allocation_failure_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests"
  )
  target_link_libraries(
    chronos_network_allocation_failure_tests PRIVATE chronos::network GTest::gtest_main
  )
  chronos_set_project_warnings(chronos_network_allocation_failure_tests)
  chronos_enable_sanitizers(chronos_network_allocation_failure_tests)
  chronos_enable_static_analysis(chronos_network_allocation_failure_tests)
  gtest_discover_tests(chronos_network_allocation_failure_tests DISCOVERY_TIMEOUT 30)

  add_executable(
    chronos_live_tests
    tests/live/incremental_aggregate_header_self_contained.cpp
    tests/live/incremental_aggregate_test.cpp
    tests/live/committed_batch_evaluator_header_self_contained.cpp
    tests/live/committed_batch_evaluator_test.cpp
    tests/live/durable_multi_tablet_subscription_header_self_contained.cpp
    tests/live/durable_multi_tablet_subscription_test.cpp
    tests/live/durable_materialized_view_header_self_contained.cpp
    tests/live/durable_materialized_view_test.cpp
    tests/live/materialized_view_header_self_contained.cpp
    tests/live/materialized_view_test.cpp
    tests/live/multi_tablet_subscription_header_self_contained.cpp
    tests/live/multi_tablet_subscription_checkpoint_header_self_contained.cpp
    tests/live/multi_tablet_subscription_checkpoint_storage_header_self_contained.cpp
    tests/live/multi_tablet_subscription_checkpoint_storage_test.cpp
    tests/live/multi_tablet_subscription_checkpoint_test.cpp
    tests/live/multi_tablet_subscription_test.cpp
    tests/live/multi_tablet_snapshot_subscription_header_self_contained.cpp
    tests/live/multi_tablet_snapshot_subscription_test.cpp
    tests/live/raft_subscription_reclaimer_header_self_contained.cpp
    tests/live/raft_subscription_reclaimer_test.cpp
    tests/live/materialized_view_checkpoint_header_self_contained.cpp
    tests/live/materialized_view_checkpoint_test.cpp
    tests/live/materialized_view_checkpoint_storage_header_self_contained.cpp
    tests/live/materialized_view_checkpoint_storage_test.cpp
    tests/live/resume_token_header_self_contained.cpp
    tests/live/resume_token_test.cpp
    tests/live/snapshot_subscription_header_self_contained.cpp
    tests/live/snapshot_subscription_test.cpp
    tests/live/subscription_header_self_contained.cpp
    tests/live/subscription_plan_header_self_contained.cpp
    tests/live/subscription_plan_definition_header_self_contained.cpp
    tests/live/subscription_plan_definition_test.cpp
    tests/live/subscription_plan_storage_header_self_contained.cpp
    tests/live/subscription_plan_storage_test.cpp
    tests/live/subscription_plan_test.cpp
    tests/live/subscription_protocol_header_self_contained.cpp
    tests/live/subscription_protocol_test.cpp
    tests/live/subscription_retention_header_self_contained.cpp
    tests/live/subscription_retention_test.cpp
    tests/live/subscription_service_header_self_contained.cpp
    tests/live/subscription_service_test.cpp
    tests/live/subscription_test.cpp
    tests/live/wal_subscription_reclaimer_header_self_contained.cpp
    tests/live/wal_subscription_reclaimer_test.cpp
  )
  set_target_properties(chronos_live_tests PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_live_tests PRIVATE cxx_std_23)
  target_include_directories(
    chronos_live_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src" "${CMAKE_CURRENT_SOURCE_DIR}/tests"
  )
  target_link_libraries(chronos_live_tests PRIVATE chronos::live GTest::gtest_main zstd::libzstd)
  chronos_set_project_warnings(chronos_live_tests)
  chronos_enable_sanitizers(chronos_live_tests)
  chronos_enable_static_analysis(chronos_live_tests)
  gtest_discover_tests(chronos_live_tests DISCOVERY_TIMEOUT 30)

  add_executable(
    chronos_live_allocation_failure_tests
    tests/live/durable_multi_tablet_subscription_allocation_failure_test.cpp
    tests/live/materialized_view_checkpoint_storage_allocation_failure_test.cpp
    tests/live/multi_tablet_subscription_checkpoint_storage_allocation_failure_test.cpp
    tests/live/multi_tablet_subscription_checkpoint_allocation_failure_test.cpp
    tests/live/snapshot_subscription_allocation_failure_test.cpp
    tests/live/subscription_plan_storage_allocation_failure_test.cpp
    tests/live/subscription_service_allocation_failure_test.cpp
    tests/support/failing_allocator.cpp
  )
  set_target_properties(chronos_live_allocation_failure_tests PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_live_allocation_failure_tests PRIVATE cxx_std_23)
  target_include_directories(
    chronos_live_allocation_failure_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests"
  )
  target_link_libraries(
    chronos_live_allocation_failure_tests PRIVATE chronos::live GTest::gtest_main
  )
  chronos_set_project_warnings(chronos_live_allocation_failure_tests)
  chronos_enable_sanitizers(chronos_live_allocation_failure_tests)
  chronos_enable_static_analysis(chronos_live_allocation_failure_tests)
  gtest_discover_tests(chronos_live_allocation_failure_tests DISCOVERY_TIMEOUT 30)

  add_executable(
    chronos_raft_tests
    tests/raft/async_durable_runtime_header_self_contained.cpp
    tests/raft/async_durable_runtime_test.cpp
    tests/raft/async_durable_worker_extension_set_header_self_contained.cpp
    tests/raft/async_durable_worker_extension_set_test.cpp
    tests/raft/async_metadata_application_header_self_contained.cpp
    tests/raft/async_metadata_application_test.cpp
    tests/raft/deterministic_simulator_header_self_contained.cpp
    tests/raft/deterministic_simulator_test.cpp
    tests/raft/durable_runtime_header_self_contained.cpp
    tests/raft/durable_runtime_test.cpp
    tests/raft/durable_tablet_reconfiguration_header_self_contained.cpp
    tests/raft/durable_tablet_reconfiguration_test.cpp
    tests/raft/membership_header_self_contained.cpp
    tests/raft/membership_test.cpp
    tests/raft/node_header_self_contained.cpp
    tests/raft/node_test.cpp
    tests/raft/rebalancing_header_self_contained.cpp
    tests/raft/rebalancing_test.cpp
    tests/raft/runtime_timer_header_self_contained.cpp
    tests/raft/runtime_timer_test.cpp
    tests/raft/runtime_timer_driver_header_self_contained.cpp
    tests/raft/runtime_timer_driver_test.cpp
    tests/raft/tablet_reconfiguration_header_self_contained.cpp
    tests/raft/tablet_reconfiguration_test.cpp
    tests/raft/tablet_reconfiguration_action_codec_header_self_contained.cpp
    tests/raft/tablet_reconfiguration_action_codec_test.cpp
    tests/raft/tablet_reconfiguration_action_ledger_header_self_contained.cpp
    tests/raft/tablet_reconfiguration_action_ledger_test.cpp
    tests/raft/tablet_group_binding_codec_header_self_contained.cpp
    tests/raft/tablet_group_binding_codec_test.cpp
    tests/raft/tablet_movement_checkpoint_header_self_contained.cpp
    tests/raft/tablet_movement_checkpoint_test.cpp
    tests/raft/tablet_movement_checkpoint_recovery_header_self_contained.cpp
    tests/raft/tablet_movement_checkpoint_recovery_test.cpp
    tests/raft/tablet_movement_checkpoint_reference_header_self_contained.cpp
    tests/raft/tablet_movement_checkpoint_reference_test.cpp
    tests/raft/tablet_movement_checkpoint_storage_header_self_contained.cpp
    tests/raft/tablet_movement_checkpoint_storage_test.cpp
    tests/raft/tablet_movement_snapshot_chunk_header_self_contained.cpp
    tests/raft/tablet_movement_snapshot_chunk_test.cpp
    tests/raft/tablet_movement_snapshot_chunk_storage_header_self_contained.cpp
    tests/raft/tablet_movement_snapshot_chunk_storage_test.cpp
    tests/raft/transport_codec_header_self_contained.cpp
    tests/raft/transport_codec_test.cpp
    tests/raft/multi_raft_header_self_contained.cpp
    tests/raft/multi_raft_test.cpp
    tests/raft/metadata_header_self_contained.cpp
    tests/raft/metadata_test.cpp
    tests/raft/metadata_codec_header_self_contained.cpp
    tests/raft/metadata_codec_test.cpp
    tests/raft/metadata_snapshot_header_self_contained.cpp
    tests/raft/metadata_snapshot_storage_header_self_contained.cpp
    tests/raft/metadata_snapshot_storage_test.cpp
    tests/raft/metadata_snapshot_test.cpp
    tests/raft/metadata_runtime_header_self_contained.cpp
    tests/raft/metadata_runtime_test.cpp
    tests/raft/schema_definition_codec_header_self_contained.cpp
    tests/raft/schema_definition_codec_test.cpp
    tests/raft/multiplexed_log_header_self_contained.cpp
    tests/raft/multiplexed_log_test.cpp
    tests/raft/persistent_log_header_self_contained.cpp
    tests/raft/persistent_log_test.cpp
    tests/raft/types_header_self_contained.cpp
  )
  set_target_properties(chronos_raft_tests PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_raft_tests PRIVATE cxx_std_23)
  target_link_libraries(chronos_raft_tests PRIVATE chronos::raft GTest::gtest_main)
  chronos_set_project_warnings(chronos_raft_tests)
  chronos_enable_sanitizers(chronos_raft_tests)
  chronos_enable_static_analysis(chronos_raft_tests)
  gtest_discover_tests(chronos_raft_tests DISCOVERY_TIMEOUT 30)

  add_executable(
    chronos_cluster_tests
    tests/cluster/distributed_grouped_query_execution_header_self_contained.cpp
    tests/cluster/distributed_grouped_query_tcp_execution_header_self_contained.cpp
    tests/cluster/distributed_grouped_query_tcp_client_header_self_contained.cpp
    tests/cluster/distributed_grouped_query_tcp_client_test.cpp
    tests/cluster/distributed_grouped_query_tcp_server_header_self_contained.cpp
    tests/cluster/distributed_grouped_query_transport_header_self_contained.cpp
    tests/cluster/distributed_grouped_query_transport_test.cpp
    tests/cluster/distributed_vector_aggregate_finalization_v2_header_self_contained.cpp
    tests/cluster/distributed_vector_aggregate_finalization_v2_test.cpp
    tests/cluster/distributed_vector_aggregate_query_execution_v2_header_self_contained.cpp
    tests/cluster/distributed_vector_aggregate_query_tcp_client_v2_header_self_contained.cpp
    tests/cluster/distributed_vector_aggregate_query_tcp_execution_v2_header_self_contained.cpp
    tests/cluster/distributed_vector_aggregate_query_tcp_client_v2_test.cpp
    tests/cluster/distributed_vector_aggregate_query_tcp_server_v2_header_self_contained.cpp
    tests/cluster/distributed_vector_aggregate_query_tcp_server_v2_test.cpp
    tests/cluster/distributed_vector_aggregate_query_transport_v2_header_self_contained.cpp
    tests/cluster/distributed_vector_aggregate_query_transport_v2_test.cpp
    tests/cluster/distributed_vector_aggregate_query_tls_v2_header_self_contained.cpp
    tests/cluster/distributed_vector_aggregate_query_tls_v2_test.cpp
    tests/cluster/distributed_vector_result_exchange_header_self_contained.cpp
    tests/cluster/distributed_vector_result_exchange_test.cpp
    tests/cluster/distributed_vector_row_finalization_v2_header_self_contained.cpp
    tests/cluster/distributed_vector_row_finalization_v2_test.cpp
    tests/cluster/distributed_vector_query_execution_v2_header_self_contained.cpp
    tests/cluster/distributed_vector_query_tcp_execution_v2_header_self_contained.cpp
    tests/cluster/distributed_vector_query_transport_header_self_contained.cpp
    tests/cluster/distributed_vector_query_transport_test.cpp
    tests/cluster/distributed_vector_query_transport_v2_header_self_contained.cpp
    tests/cluster/distributed_vector_query_transport_v2_test.cpp
    tests/cluster/distributed_vector_query_tls_v2_header_self_contained.cpp
    tests/cluster/distributed_vector_query_tls_v2_test.cpp
    tests/cluster/distributed_vector_query_tcp_client_v2_header_self_contained.cpp
    tests/cluster/distributed_vector_query_tcp_client_v2_test.cpp
    tests/cluster/distributed_vector_query_tcp_server_v2_header_self_contained.cpp
    tests/cluster/distributed_grouped_query_tls_header_self_contained.cpp
    tests/cluster/distributed_grouped_query_tls_test.cpp
    tests/cluster/distributed_query_execution_header_self_contained.cpp
    tests/cluster/distributed_query_execution_test.cpp
    tests/cluster/distributed_query_tcp_client_header_self_contained.cpp
    tests/cluster/distributed_query_tcp_execution_header_self_contained.cpp
    tests/cluster/distributed_query_tcp_server_header_self_contained.cpp
    tests/cluster/distributed_query_tcp_server_test.cpp
    tests/cluster/distributed_query_tls_client_header_self_contained.cpp
    tests/cluster/distributed_query_tls_client_test.cpp
    tests/cluster/distributed_query_tls_server_header_self_contained.cpp
    tests/cluster/distributed_query_tls_server_test.cpp
    tests/cluster/distributed_query_transport_header_self_contained.cpp
    tests/cluster/distributed_query_transport_test.cpp
    tests/cluster/raft_observation_transport_header_self_contained.cpp
    tests/cluster/raft_observation_transport_test.cpp
    tests/cluster/raft_observation_tls_client_header_self_contained.cpp
    tests/cluster/raft_observation_tls_client_test.cpp
    tests/cluster/raft_observation_tcp_client_header_self_contained.cpp
    tests/cluster/raft_observation_tcp_client_test.cpp
    tests/cluster/raft_observation_tcp_acquisition_header_self_contained.cpp
    tests/cluster/raft_observation_tcp_acquisition_test.cpp
    tests/cluster/raft_observation_tcp_pair_acquisition_header_self_contained.cpp
    tests/cluster/raft_observation_tcp_pair_acquisition_test.cpp
    tests/cluster/raft_observation_tcp_batch_acquisition_header_self_contained.cpp
    tests/cluster/raft_observation_tcp_batch_acquisition_test.cpp
    tests/cluster/raft_observation_tls_server_header_self_contained.cpp
    tests/cluster/raft_observation_tls_server_test.cpp
    tests/cluster/raft_observation_tcp_server_header_self_contained.cpp
    tests/cluster/raft_observation_tcp_server_test.cpp
    tests/cluster/raft_transport_receiver_header_self_contained.cpp
    tests/cluster/raft_transport_receiver_test.cpp
    tests/cluster/raft_transport_peer_pool_header_self_contained.cpp
    tests/cluster/raft_transport_peer_pool_test.cpp
    tests/cluster/raft_transport_peer_reconnect_header_self_contained.cpp
    tests/cluster/raft_transport_peer_reconnect_test.cpp
    tests/cluster/raft_transport_peer_manager_header_self_contained.cpp
    tests/cluster/raft_transport_peer_manager_test.cpp
    tests/cluster/raft_transport_runtime_header_self_contained.cpp
    tests/cluster/raft_transport_runtime_test.cpp
    tests/cluster/raft_transport_tcp_connector_header_self_contained.cpp
    tests/cluster/raft_transport_tcp_connector_test.cpp
    tests/cluster/raft_transport_tcp_server_header_self_contained.cpp
    tests/cluster/raft_transport_tcp_server_test.cpp
    tests/cluster/raft_transport_tls_client_header_self_contained.cpp
    tests/cluster/raft_transport_tls_client_test.cpp
    tests/cluster/raft_transport_tls_server_header_self_contained.cpp
    tests/cluster/raft_transport_tls_server_test.cpp
    tests/cluster/remote_tablet_reconfiguration_header_self_contained.cpp
    tests/cluster/remote_tablet_reconfiguration_test.cpp
    tests/cluster/tablet_physical_part_install_header_self_contained.cpp
    tests/cluster/tablet_physical_part_install_test.cpp
    tests/cluster/tablet_physical_movement_readiness_header_self_contained.cpp
    tests/cluster/tablet_physical_part_chunk_header_self_contained.cpp
    tests/cluster/tablet_physical_part_chunk_storage_header_self_contained.cpp
    tests/cluster/tablet_physical_part_chunk_storage_test.cpp
    tests/cluster/tablet_physical_receipt_reclamation_header_self_contained.cpp
    tests/cluster/tablet_physical_part_chunk_test.cpp
    tests/cluster/tablet_physical_snapshot_ownership_header_self_contained.cpp
    tests/cluster/tablet_physical_snapshot_ownership_test.cpp
  )
  set_target_properties(chronos_cluster_tests PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_cluster_tests PRIVATE cxx_std_23)
  target_include_directories(chronos_cluster_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests")
  target_compile_definitions(
    chronos_cluster_tests
    PRIVATE CHRONOS_NETWORK_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/network"
  )
  target_link_libraries(chronos_cluster_tests PRIVATE chronos::cluster GTest::gtest_main)
  chronos_set_project_warnings(chronos_cluster_tests)
  chronos_enable_sanitizers(chronos_cluster_tests)
  chronos_enable_static_analysis(chronos_cluster_tests)
  gtest_discover_tests(chronos_cluster_tests DISCOVERY_TIMEOUT 30)

  add_executable(
    chronos_cluster_allocation_failure_tests
    tests/cluster/distributed_vector_aggregate_finalization_v2_allocation_failure_test.cpp
    tests/cluster/distributed_vector_aggregate_query_transport_v2_allocation_failure_test.cpp
    tests/cluster/distributed_query_tcp_client_allocation_failure_test.cpp
    tests/cluster/distributed_vector_row_finalization_v2_allocation_failure_test.cpp
    tests/cluster/distributed_vector_query_transport_v2_allocation_failure_test.cpp
    tests/cluster/distributed_vector_result_exchange_allocation_failure_test.cpp
    tests/support/failing_allocator.cpp
  )
  set_target_properties(
    chronos_cluster_allocation_failure_tests PROPERTIES CXX_EXTENSIONS OFF
  )
  target_compile_features(chronos_cluster_allocation_failure_tests PRIVATE cxx_std_23)
  target_include_directories(
    chronos_cluster_allocation_failure_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests"
  )
  target_compile_definitions(
    chronos_cluster_allocation_failure_tests
    PRIVATE CHRONOS_NETWORK_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/network"
  )
  target_link_libraries(
    chronos_cluster_allocation_failure_tests PRIVATE chronos::cluster GTest::gtest_main
  )
  chronos_set_project_warnings(chronos_cluster_allocation_failure_tests)
  chronos_enable_sanitizers(chronos_cluster_allocation_failure_tests)
  chronos_enable_static_analysis(chronos_cluster_allocation_failure_tests)
  gtest_discover_tests(chronos_cluster_allocation_failure_tests DISCOVERY_TIMEOUT 30)

  add_executable(
    chronos_tiering_tests
    tests/tiering/cold_manifest_header_self_contained.cpp
    tests/tiering/cold_manifest_format_header_self_contained.cpp
    tests/tiering/cold_manifest_storage_header_self_contained.cpp
    tests/tiering/cold_manifest_storage_test.cpp
    tests/tiering/cold_manifest_test.cpp
    tests/tiering/object_store_header_self_contained.cpp
    tests/tiering/object_store_test.cpp
    tests/tiering/tiered_distributed_fragment_worker_header_self_contained.cpp
    tests/tiering/tiered_pair_commit_header_self_contained.cpp
    tests/tiering/tiered_pair_commit_test.cpp
    tests/tiering/tiered_part_loader_header_self_contained.cpp
    tests/tiering/tiered_part_loader_test.cpp
    tests/tiering/tiered_publication_header_self_contained.cpp
    tests/tiering/tiered_publication_test.cpp
    tests/tiering/tiered_reclamation_header_self_contained.cpp
    tests/tiering/tiered_parts_header_self_contained.cpp
    tests/tiering/tiered_parts_test.cpp
  )
  set_target_properties(chronos_tiering_tests PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_tiering_tests PRIVATE cxx_std_23)
  target_include_directories(
    chronos_tiering_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src" "${CMAKE_CURRENT_SOURCE_DIR}/tests"
  )
  target_link_libraries(chronos_tiering_tests PRIVATE chronos::tiering GTest::gtest_main)
  chronos_set_project_warnings(chronos_tiering_tests)
  chronos_enable_sanitizers(chronos_tiering_tests)
  chronos_enable_static_analysis(chronos_tiering_tests)
  gtest_discover_tests(chronos_tiering_tests DISCOVERY_TIMEOUT 30)

  add_executable(chronos_feature_smoke_tests tests/integration/feature_smoke_test.cpp)
  set_target_properties(chronos_feature_smoke_tests PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_feature_smoke_tests PRIVATE cxx_std_23)
  target_include_directories(chronos_feature_smoke_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests")
  target_link_libraries(
    chronos_feature_smoke_tests
    PRIVATE chronos::live chronos::query chronos::raft chronos::tiering GTest::gtest_main
  )
  chronos_set_project_warnings(chronos_feature_smoke_tests)
  chronos_enable_sanitizers(chronos_feature_smoke_tests)
  chronos_enable_static_analysis(chronos_feature_smoke_tests)
  gtest_discover_tests(chronos_feature_smoke_tests DISCOVERY_TIMEOUT 30)

  if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_executable(chronosd_process_tests tests/integration/chronosd_process_test.cpp)
    set_target_properties(chronosd_process_tests PROPERTIES CXX_EXTENSIONS OFF)
    target_compile_features(chronosd_process_tests PRIVATE cxx_std_23)
    target_include_directories(chronosd_process_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests")
    target_link_libraries(chronosd_process_tests PRIVATE chronos::service GTest::gtest_main)
    target_compile_definitions(
      chronosd_process_tests PRIVATE CHRONOSD_PATH="$<TARGET_FILE:chronosd>"
    )
    add_dependencies(chronosd_process_tests chronosd)
    chronos_set_project_warnings(chronosd_process_tests)
    chronos_enable_sanitizers(chronosd_process_tests)
    chronos_enable_static_analysis(chronosd_process_tests)
    gtest_discover_tests(chronosd_process_tests DISCOVERY_TIMEOUT 30)
  endif()

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

  if(CHRONOS_ENABLE_ARROW_INTEROP)
    add_executable(
      chronos_interop_tests
      tests/interop/arrow_parquet_header_self_contained.cpp
      tests/interop/arrow_parquet_test.cpp
    )
    set_target_properties(chronos_interop_tests PROPERTIES CXX_EXTENSIONS OFF)
    target_compile_features(chronos_interop_tests PRIVATE cxx_std_23)
    target_include_directories(chronos_interop_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests")
    target_link_libraries(chronos_interop_tests PRIVATE chronos::interop GTest::gtest_main)
    chronos_set_project_warnings(chronos_interop_tests)
    chronos_enable_sanitizers(chronos_interop_tests)
    chronos_enable_static_analysis(chronos_interop_tests)
    gtest_discover_tests(chronos_interop_tests DISCOVERY_TIMEOUT 30)
  endif()

  add_executable(
    chronos_cseg_tests
    tests/cseg/csegdump_test.cpp
    tests/cseg/compression_header_self_contained.cpp
    tests/cseg/compression_test.cpp
    tests/cseg/format_header_self_contained.cpp
    tests/cseg/format_test.cpp
    tests/cseg/inspection_header_self_contained.cpp
    tests/cseg/inspection_test.cpp
    tests/cseg/layout_header_self_contained.cpp
    tests/cseg/layout_test.cpp
    tests/cseg/metadata_codec_header_self_contained.cpp
    tests/cseg/metadata_codec_test.cpp
    tests/cseg/part_codec_header_self_contained.cpp
    tests/cseg/part_codec_test.cpp
    tests/cseg/page_codec_header_self_contained.cpp
    tests/cseg/page_codec_test.cpp
    tests/cseg/plain_page_header_self_contained.cpp
    tests/cseg/plain_page_test.cpp
    tests/cseg/projected_reader_header_self_contained.cpp
    tests/cseg/projected_reader_test.cpp
    tests/cseg/pruning_header_self_contained.cpp
    tests/cseg/pruning_test.cpp
    tests/cseg/temporal_format_header_self_contained.cpp
    tests/cseg/temporal_format_test.cpp
    tests/cseg/temporal_layout_header_self_contained.cpp
    tests/cseg/temporal_layout_test.cpp
    tests/cseg/temporal_metadata_codec_test.cpp
    tests/cseg/temporal_part_codec_test.cpp
    tests/cseg/temporal_system_row_header_self_contained.cpp
    tests/cseg/temporal_system_row_test.cpp
    tests/cseg/types_header_self_contained.cpp
    tests/cseg/types_test.cpp
    tests/cseg/validator_header_self_contained.cpp
    tests/cseg/validator_test.cpp
  )
  set_target_properties(chronos_cseg_tests PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_cseg_tests PRIVATE cxx_std_23)
  target_link_libraries(
    chronos_cseg_tests PRIVATE chronos::cseg GTest::gtest_main zstd::libzstd
  )
  target_compile_definitions(
    chronos_cseg_tests
    PRIVATE CHRONOS_CSEGDUMP_PATH="$<TARGET_FILE:chronos-csegdump>"
  )
  add_dependencies(chronos_cseg_tests chronos-csegdump)
  chronos_set_project_warnings(chronos_cseg_tests)
  chronos_enable_sanitizers(chronos_cseg_tests)
  chronos_enable_static_analysis(chronos_cseg_tests)
  gtest_discover_tests(chronos_cseg_tests DISCOVERY_TIMEOUT 30)

  add_executable(
    chronos_cseg_allocation_failure_tests
    tests/cseg/projected_reader_allocation_failure_test.cpp
    tests/support/failing_allocator.cpp
  )
  set_target_properties(chronos_cseg_allocation_failure_tests PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_cseg_allocation_failure_tests PRIVATE cxx_std_23)
  target_include_directories(
    chronos_cseg_allocation_failure_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests"
  )
  target_link_libraries(
    chronos_cseg_allocation_failure_tests
    PRIVATE chronos::cseg GTest::gtest_main zstd::libzstd
  )
  chronos_set_project_warnings(chronos_cseg_allocation_failure_tests)
  chronos_enable_sanitizers(chronos_cseg_allocation_failure_tests)
  chronos_enable_static_analysis(chronos_cseg_allocation_failure_tests)
  gtest_discover_tests(chronos_cseg_allocation_failure_tests DISCOVERY_TIMEOUT 30)

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
    tests/wal/wal_reclamation_test.cpp
    tests/wal/wal_reclamation_crash_test.cpp
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
    tests/ingest/async_raft_tablet_application_header_self_contained.cpp
    tests/ingest/async_raft_tablet_application_test.cpp
    tests/ingest/columnar_append_corruption_test.cpp
    tests/ingest/columnar_append_executor_header_self_contained.cpp
    tests/ingest/columnar_append_executor_test.cpp
    tests/ingest/columnar_append_format_header_self_contained.cpp
    tests/ingest/columnar_append_header_self_contained.cpp
    tests/ingest/columnar_append_recovery_header_self_contained.cpp
    tests/ingest/columnar_append_recovery_test.cpp
    tests/ingest/committed_columnar_append_header_self_contained.cpp
    tests/ingest/columnar_append_property_test.cpp
    tests/ingest/columnar_append_test.cpp
    tests/ingest/deduplication_key_test.cpp
    tests/ingest/identity_header_self_contained.cpp
    tests/ingest/identity_test.cpp
    tests/ingest/raft_tablet_state_machine_header_self_contained.cpp
    tests/ingest/raft_tablet_state_machine_test.cpp
    tests/ingest/raft_tablet_snapshot_header_self_contained.cpp
    tests/ingest/raft_tablet_snapshot_test.cpp
    tests/ingest/raft_tablet_snapshot_storage_header_self_contained.cpp
    tests/ingest/raft_tablet_snapshot_storage_test.cpp
    tests/ingest/retry_directory_header_self_contained.cpp
    tests/ingest/retry_directory_test.cpp
    tests/ingest/sealed_head_flush_queue_header_self_contained.cpp
    tests/ingest/sealed_head_flush_queue_test.cpp
    tests/ingest/sha256_header_self_contained.cpp
    tests/ingest/sha256_test.cpp
    tests/ingest/tablet_movement_catch_up_checkpoint_header_self_contained.cpp
    tests/ingest/tablet_movement_catch_up_checkpoint_test.cpp
    tests/ingest/tablet_movement_raft_snapshot_completion_header_self_contained.cpp
    tests/ingest/tablet_movement_raft_snapshot_completion_test.cpp
    tests/ingest/tablet_movement_snapshot_handoff_header_self_contained.cpp
    tests/ingest/tablet_movement_snapshot_handoff_test.cpp
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
    chronos_manifest_tests
    tests/manifest/checkpoint_builder_header_self_contained.cpp
    tests/manifest/checkpoint_builder_test.cpp
    tests/manifest/codec_corruption_test.cpp
    tests/manifest/codec_header_self_contained.cpp
    tests/manifest/codec_property_test.cpp
    tests/manifest/codec_test.cpp
    tests/manifest/compaction_header_self_contained.cpp
    tests/manifest/compaction_test.cpp
    tests/manifest/compaction_coordinator_header_self_contained.cpp
    tests/manifest/compaction_coordinator_test.cpp
    tests/manifest/compaction_crash_matrix_test.cpp
    tests/manifest/compaction_equivalence_header_self_contained.cpp
    tests/manifest/compaction_equivalence_test.cpp
    tests/manifest/compaction_planner_header_self_contained.cpp
    tests/manifest/compaction_planner_test.cpp
    tests/manifest/format_header_self_contained.cpp
    tests/manifest/format_test.cpp
    tests/manifest/generation_builder_header_self_contained.cpp
    tests/manifest/generation_builder_test.cpp
    tests/manifest/manifest_flush_crash_matrix_test.cpp
    tests/manifest/layout_header_self_contained.cpp
    tests/manifest/layout_test.cpp
    tests/manifest/naming_header_self_contained.cpp
    tests/manifest/naming_test.cpp
    tests/manifest/part_validation_header_self_contained.cpp
    tests/manifest/part_validation_test.cpp
    tests/manifest/part_reclamation_crash_matrix_test.cpp
    tests/manifest/publication_header_self_contained.cpp
    tests/manifest/publication_test.cpp
    tests/manifest/raft_tablet_physical_snapshot_header_self_contained.cpp
    tests/manifest/raft_tablet_physical_snapshot_test.cpp
    tests/manifest/retirement_header_self_contained.cpp
    tests/manifest/sealed_head_flush_header_self_contained.cpp
    tests/manifest/sealed_head_flush_test.cpp
    tests/manifest/sealed_head_flush_coordinator_header_self_contained.cpp
    tests/manifest/sealed_head_flush_coordinator_test.cpp
    tests/manifest/startup_recovery_header_self_contained.cpp
    tests/manifest/startup_recovery_test.cpp
    tests/manifest/storage_header_self_contained.cpp
    tests/manifest/storage_test.cpp
    tests/manifest/temporal_codec_header_self_contained.cpp
    tests/manifest/temporal_codec_test.cpp
    tests/manifest/temporal_format_header_self_contained.cpp
    tests/manifest/temporal_format_test.cpp
    tests/manifest/temporal_layout_header_self_contained.cpp
    tests/manifest/temporal_layout_test.cpp
    tests/manifest/temporal_part_validation_header_self_contained.cpp
    tests/manifest/temporal_part_validation_test.cpp
    tests/manifest/temporal_publication_header_self_contained.cpp
    tests/manifest/temporal_publication_test.cpp
    tests/manifest/temporal_validation_header_self_contained.cpp
    tests/manifest/temporal_validation_test.cpp
    tests/manifest/types_header_self_contained.cpp
    tests/manifest/types_test.cpp
    tests/manifest/validation_header_self_contained.cpp
    tests/manifest/validation_test.cpp
  )
  set_target_properties(chronos_manifest_tests PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_manifest_tests PRIVATE cxx_std_23)
  target_include_directories(
    chronos_manifest_tests
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests" "${CMAKE_CURRENT_SOURCE_DIR}/src"
  )
  target_link_libraries(chronos_manifest_tests PRIVATE chronos::manifest GTest::gtest_main)
  add_executable(chronos_manifest_flush_crash_child
                 tests/manifest/manifest_flush_crash_child.cpp)
  set_target_properties(chronos_manifest_flush_crash_child PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_manifest_flush_crash_child PRIVATE cxx_std_23)
  target_include_directories(
    chronos_manifest_flush_crash_child
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src" "${CMAKE_CURRENT_SOURCE_DIR}/tests"
  )
  target_link_libraries(chronos_manifest_flush_crash_child PRIVATE chronos::manifest)
  chronos_set_project_warnings(chronos_manifest_flush_crash_child)
  chronos_enable_sanitizers(chronos_manifest_flush_crash_child)
  chronos_enable_static_analysis(chronos_manifest_flush_crash_child)
  target_compile_definitions(
    chronos_manifest_tests
    PRIVATE CHRONOS_TEST_CRASH_CHILD_PATH="$<TARGET_FILE:chronos_manifest_flush_crash_child>"
  )
  add_dependencies(chronos_manifest_tests chronos_manifest_flush_crash_child)
  chronos_set_project_warnings(chronos_manifest_tests)
  chronos_enable_sanitizers(chronos_manifest_tests)
  chronos_enable_static_analysis(chronos_manifest_tests)
  gtest_discover_tests(chronos_manifest_tests DISCOVERY_TIMEOUT 30)

  add_executable(
    chronos_query_tests
    tests/query/asof_join_header_self_contained.cpp
    tests/query/asof_join_test.cpp
    tests/query/aggregate_header_self_contained.cpp
    tests/query/aggregate_test.cpp
    tests/query/cseg_scan_header_self_contained.cpp
    tests/query/cseg_scan_test.cpp
    tests/query/database_cseg_scan_header_self_contained.cpp
    tests/query/database_cseg_scan_test.cpp
    tests/query/diagnostic_header_self_contained.cpp
    tests/query/distributed_header_self_contained.cpp
    tests/query/distributed_test.cpp
    tests/query/distributed_grouped_exchange_header_self_contained.cpp
    tests/query/distributed_grouped_exchange_test.cpp
    tests/query/distributed_vector_aggregate_coordinator_header_self_contained.cpp
    tests/query/distributed_vector_aggregate_coordinator_test.cpp
    tests/query/distributed_vector_aggregate_exchange_header_self_contained.cpp
    tests/query/distributed_vector_aggregate_exchange_test.cpp
    tests/query/distributed_vector_aggregate_state_header_self_contained.cpp
    tests/query/distributed_vector_aggregate_state_test.cpp
    tests/query/distributed_vector_exchange_header_self_contained.cpp
    tests/query/distributed_vector_exchange_test.cpp
    tests/query/distributed_vector_fragment_header_self_contained.cpp
    tests/query/distributed_vector_fragment_test.cpp
    tests/query/distributed_vector_plan_header_self_contained.cpp
    tests/query/distributed_vector_plan_test.cpp
    tests/query/distributed_vector_result_schema_header_self_contained.cpp
    tests/query/distributed_vector_result_schema_test.cpp
    tests/query/distributed_fragment_header_self_contained.cpp
    tests/query/distributed_fragment_test.cpp
    tests/query/distributed_fragment_binding_header_self_contained.cpp
    tests/query/distributed_fragment_binding_test.cpp
    tests/query/distributed_fragment_dispatch_header_self_contained.cpp
    tests/query/distributed_fragment_dispatch_test.cpp
    tests/query/distributed_fragment_worker_header_self_contained.cpp
    tests/query/distributed_fragment_worker_test.cpp
    tests/query/evaluator_header_self_contained.cpp
    tests/query/evaluator_test.cpp
    tests/query/executor_header_self_contained.cpp
    tests/query/executor_test.cpp
    tests/query/explain_header_self_contained.cpp
    tests/query/explain_test.cpp
    tests/query/head_scan_header_self_contained.cpp
    tests/query/head_scan_test.cpp
    tests/query/ast_header_self_contained.cpp
    tests/query/binder_header_self_contained.cpp
    tests/query/binder_test.cpp
    tests/query/catalog_header_self_contained.cpp
    tests/query/catalog_test.cpp
    tests/query/columnar_batch_scan_header_self_contained.cpp
    tests/query/columnar_batch_scan_test.cpp
    tests/query/column_output_header_self_contained.cpp
    tests/query/column_output_test.cpp
    tests/query/committed_temporal_command_header_self_contained.cpp
    tests/query/committed_temporal_command_test.cpp
    tests/query/lexer_header_self_contained.cpp
    tests/query/lexer_test.cpp
    tests/query/latest_header_self_contained.cpp
    tests/query/latest_test.cpp
    tests/query/literal_header_self_contained.cpp
    tests/query/literal_test.cpp
    tests/query/parser_header_self_contained.cpp
    tests/query/parser_test.cpp
    tests/query/parallel_scheduler_header_self_contained.cpp
    tests/query/parallel_scheduler_test.cpp
    tests/query/physical_operator_header_self_contained.cpp
    tests/query/physical_operator_test.cpp
    tests/query/physical_optimizer_header_self_contained.cpp
    tests/query/physical_optimizer_test.cpp
    tests/query/physical_lowering_header_self_contained.cpp
    tests/query/physical_lowering_test.cpp
    tests/query/physical_plan_header_self_contained.cpp
    tests/query/physical_plan_test.cpp
    tests/query/relational_plan_header_self_contained.cpp
    tests/query/relational_plan_test.cpp
    tests/query/resource_context_header_self_contained.cpp
    tests/query/resource_context_test.cpp
    tests/query/row_version_header_self_contained.cpp
    tests/query/row_version_test.cpp
    tests/query/scalar_snapshot_scan_header_self_contained.cpp
    tests/query/scalar_snapshot_scan_test.cpp
    tests/query/snapshot_header_self_contained.cpp
    tests/query/snapshot_pipeline_header_self_contained.cpp
    tests/query/snapshot_pipeline_test.cpp
    tests/query/snapshot_shape_header_self_contained.cpp
    tests/query/snapshot_test.cpp
    tests/query/sort_header_self_contained.cpp
    tests/query/sort_test.cpp
    tests/query/spill_sort_header_self_contained.cpp
    tests/query/spill_sort_test.cpp
    tests/query/statement_binder_header_self_contained.cpp
    tests/query/statement_binder_test.cpp
    tests/query/tablet_state_pipeline_header_self_contained.cpp
    tests/query/tablet_state_pipeline_test.cpp
    tests/query/temporal_command_header_self_contained.cpp
    tests/query/temporal_command_test.cpp
    tests/query/temporal_command_executor_header_self_contained.cpp
    tests/query/temporal_command_executor_test.cpp
    tests/query/temporal_cseg_snapshot_header_self_contained.cpp
    tests/query/temporal_cseg_snapshot_test.cpp
    tests/query/temporal_recovery_header_self_contained.cpp
    tests/query/temporal_recovery_test.cpp
    tests/query/temporal_snapshot_header_self_contained.cpp
    tests/query/temporal_snapshot_test.cpp
    tests/query/timestamp_range_header_self_contained.cpp
    tests/query/timestamp_filter_kernel_test.cpp
    tests/query/value_header_self_contained.cpp
    tests/query/value_test.cpp
    tests/query/vector_chunk_header_self_contained.cpp
    tests/query/vector_chunk_test.cpp
    tests/query/vector_expression_header_self_contained.cpp
    tests/query/vector_expression_test.cpp
  )
  set_target_properties(chronos_query_tests PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_query_tests PRIVATE cxx_std_23)
  target_include_directories(
    chronos_query_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src" "${CMAKE_CURRENT_SOURCE_DIR}/tests"
  )
  target_link_libraries(
    chronos_query_tests PRIVATE chronos::query GTest::gtest_main Threads::Threads zstd::libzstd
  )
  chronos_set_project_warnings(chronos_query_tests)
  chronos_enable_sanitizers(chronos_query_tests)
  chronos_enable_static_analysis(chronos_query_tests)
  gtest_discover_tests(chronos_query_tests DISCOVERY_TIMEOUT 30)

  add_executable(
    chronos_query_allocation_failure_tests
    tests/query/asof_join_allocation_failure_test.cpp
    tests/query/aggregate_allocation_failure_test.cpp
    tests/query/cseg_scan_allocation_failure_test.cpp
    tests/query/column_output_allocation_failure_test.cpp
    tests/query/database_cseg_scan_allocation_failure_test.cpp
    tests/query/distributed_fragment_worker_allocation_failure_test.cpp
    tests/query/distributed_vector_aggregate_coordinator_allocation_failure_test.cpp
    tests/query/distributed_vector_aggregate_exchange_allocation_failure_test.cpp
    tests/query/distributed_vector_aggregate_state_allocation_failure_test.cpp
    tests/query/distributed_vector_fragment_v2_allocation_failure_test.cpp
    tests/query/head_scan_allocation_failure_test.cpp
    tests/query/latest_allocation_failure_test.cpp
    tests/query/parallel_scheduler_allocation_failure_test.cpp
    tests/query/physical_optimizer_allocation_failure_test.cpp
    tests/query/physical_lowering_allocation_failure_test.cpp
    tests/query/relational_plan_allocation_failure_test.cpp
    tests/query/sort_allocation_failure_test.cpp
    tests/query/spill_sort_allocation_failure_test.cpp
    tests/support/failing_allocator.cpp
  )
  set_target_properties(chronos_query_allocation_failure_tests PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_query_allocation_failure_tests PRIVATE cxx_std_23)
  target_include_directories(
    chronos_query_allocation_failure_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests"
  )
  target_link_libraries(
    chronos_query_allocation_failure_tests
    PRIVATE chronos::query GTest::gtest_main Threads::Threads zstd::libzstd
  )
  chronos_set_project_warnings(chronos_query_allocation_failure_tests)
  chronos_enable_sanitizers(chronos_query_allocation_failure_tests)
  chronos_enable_static_analysis(chronos_query_allocation_failure_tests)
  gtest_discover_tests(chronos_query_allocation_failure_tests DISCOVERY_TIMEOUT 30)

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
    NAME FlushBenchmarkSmokeTest.ArtifactsAndRecovery
    COMMAND
      "${CMAKE_COMMAND}"
      "-DCHRONOS_FLUSHBENCH=$<TARGET_FILE:chronos-flushbench>"
      "-DCHRONOS_TEST_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}"
      -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/cmake/flushbench_smoke_test.cmake"
  )

  add_test(
    NAME ExternalConsumerTest.InstalledPublicTargets
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
    add_test(
      NAME ChronosdCliTest.StructuredAndTextDiagnostics
      COMMAND
        "${CMAKE_COMMAND}"
        "-DCHRONOSD=$<TARGET_FILE:chronosd>"
        -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/cmake/chronosd_cli_test.cmake"
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
    chronos_cseg_benchmarks
    benchmarks/cseg/metadata_codec_benchmark.cpp
    benchmarks/cseg/part_codec_benchmark.cpp
    benchmarks/cseg/page_codec_benchmark.cpp
    benchmarks/cseg/plain_page_benchmark.cpp
    benchmarks/support/counting_allocator.cpp
  )
  set_target_properties(chronos_cseg_benchmarks PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_cseg_benchmarks PRIVATE cxx_std_23)
  target_include_directories(
    chronos_cseg_benchmarks PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/benchmarks"
  )
  target_link_libraries(chronos_cseg_benchmarks PRIVATE chronos::cseg benchmark::benchmark_main)
  chronos_set_project_warnings(chronos_cseg_benchmarks)
  chronos_enable_sanitizers(chronos_cseg_benchmarks)
  chronos_enable_static_analysis(chronos_cseg_benchmarks)

  add_executable(
    chronos_ingest_benchmarks
    benchmarks/ingest/columnar_append_benchmark.cpp
    benchmarks/ingest/columnar_append_executor_benchmark.cpp
    benchmarks/ingest/columnar_append_recovery_benchmark.cpp
    benchmarks/ingest/retry_directory_benchmark.cpp
    benchmarks/ingest/tablet_state_benchmark.cpp
    benchmarks/support/counting_allocator.cpp
  )
  target_include_directories(
    chronos_ingest_benchmarks PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/benchmarks"
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
    benchmarks/support/counting_allocator.cpp
  )
  target_include_directories(
    chronos_head_benchmarks PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/benchmarks"
  )
  set_target_properties(chronos_head_benchmarks PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_head_benchmarks PRIVATE cxx_std_23)
  target_link_libraries(
    chronos_head_benchmarks PRIVATE chronos::head benchmark::benchmark_main
  )
  chronos_set_project_warnings(chronos_head_benchmarks)
  chronos_enable_sanitizers(chronos_head_benchmarks)
  chronos_enable_static_analysis(chronos_head_benchmarks)

  add_executable(
    chronos_manifest_benchmarks
    benchmarks/manifest/compaction_equivalence_benchmark.cpp
    benchmarks/manifest/compaction_planner_benchmark.cpp
    benchmarks/manifest/codec_benchmark.cpp
    benchmarks/manifest/publication_benchmark.cpp
    benchmarks/manifest/sealed_head_flush_benchmark.cpp
  )
  target_include_directories(
    chronos_manifest_benchmarks PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests"
  )
  set_target_properties(chronos_manifest_benchmarks PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_manifest_benchmarks PRIVATE cxx_std_23)
  target_link_libraries(
    chronos_manifest_benchmarks PRIVATE chronos::manifest benchmark::benchmark_main
  )
  chronos_set_project_warnings(chronos_manifest_benchmarks)
  chronos_enable_sanitizers(chronos_manifest_benchmarks)
  chronos_enable_static_analysis(chronos_manifest_benchmarks)

  add_executable(
    chronos_query_benchmarks
    benchmarks/query/aggregate_benchmark.cpp
    benchmarks/query/asof_join_benchmark.cpp
    benchmarks/query/column_output_benchmark.cpp
    benchmarks/query/cseg_scan_benchmark.cpp
    benchmarks/query/head_scan_benchmark.cpp
    benchmarks/query/lexer_benchmark.cpp
    benchmarks/query/latest_benchmark.cpp
    benchmarks/query/parallel_scheduler_benchmark.cpp
    benchmarks/query/physical_optimizer_benchmark.cpp
    benchmarks/query/physical_plan_benchmark.cpp
    benchmarks/query/relational_plan_benchmark.cpp
    benchmarks/query/resource_context_benchmark.cpp
    benchmarks/query/sort_benchmark.cpp
    benchmarks/query/spill_sort_benchmark.cpp
    benchmarks/query/vector_chunk_benchmark.cpp
    benchmarks/support/counting_allocator.cpp
  )
  set_target_properties(chronos_query_benchmarks PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_query_benchmarks PRIVATE cxx_std_23)
  target_include_directories(
    chronos_query_benchmarks
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/benchmarks" "${CMAKE_CURRENT_SOURCE_DIR}/tests"
  )
  target_link_libraries(
    chronos_query_benchmarks
    PRIVATE chronos::query benchmark::benchmark_main Threads::Threads zstd::libzstd
  )
  chronos_set_project_warnings(chronos_query_benchmarks)
  chronos_enable_sanitizers(chronos_query_benchmarks)
  chronos_enable_static_analysis(chronos_query_benchmarks)

  add_executable(
    chronos_live_benchmarks
    benchmarks/live/subscription_benchmark.cpp
  )
  set_target_properties(chronos_live_benchmarks PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_live_benchmarks PRIVATE cxx_std_23)
  target_link_libraries(
    chronos_live_benchmarks PRIVATE chronos::live benchmark::benchmark_main
  )
  chronos_set_project_warnings(chronos_live_benchmarks)
  chronos_enable_sanitizers(chronos_live_benchmarks)
  chronos_enable_static_analysis(chronos_live_benchmarks)

  add_executable(
    chronos_network_benchmarks
    benchmarks/network/network_benchmark.cpp
    benchmarks/support/counting_allocator.cpp
  )
  set_target_properties(chronos_network_benchmarks PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_network_benchmarks PRIVATE cxx_std_23)
  target_include_directories(
    chronos_network_benchmarks PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/benchmarks"
  )
  target_link_libraries(
    chronos_network_benchmarks
    PRIVATE chronos::network benchmark::benchmark_main Threads::Threads
  )
  chronos_set_project_warnings(chronos_network_benchmarks)
  chronos_enable_sanitizers(chronos_network_benchmarks)
  chronos_enable_static_analysis(chronos_network_benchmarks)
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

  add_executable(chronos_network_protocol_fuzz tests/fuzz/network_protocol_fuzz.cpp)
  set_target_properties(chronos_network_protocol_fuzz PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_network_protocol_fuzz PRIVATE cxx_std_23)
  target_link_libraries(chronos_network_protocol_fuzz PRIVATE chronos::network)
  target_compile_options(chronos_network_protocol_fuzz PRIVATE -fsanitize=fuzzer)
  target_link_options(chronos_network_protocol_fuzz PRIVATE -fsanitize=fuzzer)
  chronos_set_project_warnings(chronos_network_protocol_fuzz)
  chronos_enable_sanitizers(chronos_network_protocol_fuzz)
  chronos_enable_static_analysis(chronos_network_protocol_fuzz)

  add_executable(chronos_resume_token_fuzz tests/fuzz/resume_token_fuzz.cpp)
  set_target_properties(chronos_resume_token_fuzz PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_resume_token_fuzz PRIVATE cxx_std_23)
  target_link_libraries(chronos_resume_token_fuzz PRIVATE chronos::live OpenSSL::Crypto)
  target_compile_options(chronos_resume_token_fuzz PRIVATE -fsanitize=fuzzer)
  target_link_options(chronos_resume_token_fuzz PRIVATE -fsanitize=fuzzer)
  chronos_set_project_warnings(chronos_resume_token_fuzz)
  chronos_enable_sanitizers(chronos_resume_token_fuzz)
  chronos_enable_static_analysis(chronos_resume_token_fuzz)

  add_executable(
    chronos_multi_tablet_subscription_checkpoint_fuzz
    tests/fuzz/multi_tablet_subscription_checkpoint_fuzz.cpp
  )
  set_target_properties(
    chronos_multi_tablet_subscription_checkpoint_fuzz PROPERTIES CXX_EXTENSIONS OFF
  )
  target_compile_features(chronos_multi_tablet_subscription_checkpoint_fuzz PRIVATE cxx_std_23)
  target_link_libraries(chronos_multi_tablet_subscription_checkpoint_fuzz PRIVATE chronos::live)
  target_compile_options(
    chronos_multi_tablet_subscription_checkpoint_fuzz PRIVATE -fsanitize=fuzzer
  )
  target_link_options(
    chronos_multi_tablet_subscription_checkpoint_fuzz PRIVATE -fsanitize=fuzzer
  )
  chronos_set_project_warnings(chronos_multi_tablet_subscription_checkpoint_fuzz)
  chronos_enable_sanitizers(chronos_multi_tablet_subscription_checkpoint_fuzz)
  chronos_enable_static_analysis(chronos_multi_tablet_subscription_checkpoint_fuzz)

  add_executable(
    chronos_distributed_vector_result_exchange_fuzz
    tests/fuzz/distributed_vector_result_exchange_fuzz.cpp
  )
  set_target_properties(
    chronos_distributed_vector_result_exchange_fuzz PROPERTIES CXX_EXTENSIONS OFF
  )
  target_compile_features(chronos_distributed_vector_result_exchange_fuzz PRIVATE cxx_std_23)
  target_link_libraries(chronos_distributed_vector_result_exchange_fuzz PRIVATE chronos::cluster)
  target_compile_options(
    chronos_distributed_vector_result_exchange_fuzz PRIVATE -fsanitize=fuzzer
  )
  target_link_options(
    chronos_distributed_vector_result_exchange_fuzz PRIVATE -fsanitize=fuzzer
  )
  chronos_set_project_warnings(chronos_distributed_vector_result_exchange_fuzz)
  chronos_enable_sanitizers(chronos_distributed_vector_result_exchange_fuzz)
  chronos_enable_static_analysis(chronos_distributed_vector_result_exchange_fuzz)

  add_executable(
    chronos_distributed_vector_fragment_v2_fuzz
    tests/fuzz/distributed_vector_fragment_v2_fuzz.cpp
  )
  set_target_properties(
    chronos_distributed_vector_fragment_v2_fuzz PROPERTIES CXX_EXTENSIONS OFF
  )
  target_compile_features(chronos_distributed_vector_fragment_v2_fuzz PRIVATE cxx_std_23)
  target_link_libraries(chronos_distributed_vector_fragment_v2_fuzz PRIVATE chronos::query)
  target_compile_options(
    chronos_distributed_vector_fragment_v2_fuzz PRIVATE -fsanitize=fuzzer
  )
  target_link_options(chronos_distributed_vector_fragment_v2_fuzz PRIVATE -fsanitize=fuzzer)
  chronos_set_project_warnings(chronos_distributed_vector_fragment_v2_fuzz)
  chronos_enable_sanitizers(chronos_distributed_vector_fragment_v2_fuzz)
  chronos_enable_static_analysis(chronos_distributed_vector_fragment_v2_fuzz)

  add_executable(
    chronos_distributed_vector_aggregate_state_fuzz
    tests/fuzz/distributed_vector_aggregate_state_fuzz.cpp
  )
  set_target_properties(
    chronos_distributed_vector_aggregate_state_fuzz PROPERTIES CXX_EXTENSIONS OFF
  )
  target_compile_features(chronos_distributed_vector_aggregate_state_fuzz PRIVATE cxx_std_23)
  target_link_libraries(chronos_distributed_vector_aggregate_state_fuzz PRIVATE chronos::query)
  target_compile_options(
    chronos_distributed_vector_aggregate_state_fuzz PRIVATE -fsanitize=fuzzer
  )
  target_link_options(
    chronos_distributed_vector_aggregate_state_fuzz PRIVATE -fsanitize=fuzzer
  )
  chronos_set_project_warnings(chronos_distributed_vector_aggregate_state_fuzz)
  chronos_enable_sanitizers(chronos_distributed_vector_aggregate_state_fuzz)
  chronos_enable_static_analysis(chronos_distributed_vector_aggregate_state_fuzz)

  add_executable(
    chronos_distributed_vector_aggregate_exchange_fuzz
    tests/fuzz/distributed_vector_aggregate_exchange_fuzz.cpp
  )
  set_target_properties(
    chronos_distributed_vector_aggregate_exchange_fuzz PROPERTIES CXX_EXTENSIONS OFF
  )
  target_compile_features(chronos_distributed_vector_aggregate_exchange_fuzz PRIVATE cxx_std_23)
  target_link_libraries(chronos_distributed_vector_aggregate_exchange_fuzz PRIVATE chronos::query)
  target_compile_options(
    chronos_distributed_vector_aggregate_exchange_fuzz PRIVATE -fsanitize=fuzzer
  )
  target_link_options(
    chronos_distributed_vector_aggregate_exchange_fuzz PRIVATE -fsanitize=fuzzer
  )
  chronos_set_project_warnings(chronos_distributed_vector_aggregate_exchange_fuzz)
  chronos_enable_sanitizers(chronos_distributed_vector_aggregate_exchange_fuzz)
  chronos_enable_static_analysis(chronos_distributed_vector_aggregate_exchange_fuzz)

  add_executable(
    chronos_distributed_vector_query_transport_v2_fuzz
    tests/fuzz/distributed_vector_query_transport_v2_fuzz.cpp
  )
  set_target_properties(
    chronos_distributed_vector_query_transport_v2_fuzz PROPERTIES CXX_EXTENSIONS OFF
  )
  target_compile_features(chronos_distributed_vector_query_transport_v2_fuzz PRIVATE cxx_std_23)
  target_link_libraries(chronos_distributed_vector_query_transport_v2_fuzz PRIVATE chronos::cluster)
  target_compile_options(
    chronos_distributed_vector_query_transport_v2_fuzz PRIVATE -fsanitize=fuzzer
  )
  target_link_options(
    chronos_distributed_vector_query_transport_v2_fuzz PRIVATE -fsanitize=fuzzer
  )
  chronos_set_project_warnings(chronos_distributed_vector_query_transport_v2_fuzz)
  chronos_enable_sanitizers(chronos_distributed_vector_query_transport_v2_fuzz)
  chronos_enable_static_analysis(chronos_distributed_vector_query_transport_v2_fuzz)

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

  add_executable(chronos_cseg_metadata_codec_fuzz tests/fuzz/cseg_metadata_codec_fuzz.cpp)
  set_target_properties(chronos_cseg_metadata_codec_fuzz PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_cseg_metadata_codec_fuzz PRIVATE cxx_std_23)
  target_link_libraries(chronos_cseg_metadata_codec_fuzz PRIVATE chronos::cseg)
  target_compile_options(chronos_cseg_metadata_codec_fuzz PRIVATE -fsanitize=fuzzer)
  target_link_options(chronos_cseg_metadata_codec_fuzz PRIVATE -fsanitize=fuzzer)
  chronos_set_project_warnings(chronos_cseg_metadata_codec_fuzz)
  chronos_enable_sanitizers(chronos_cseg_metadata_codec_fuzz)
  chronos_enable_static_analysis(chronos_cseg_metadata_codec_fuzz)

  add_executable(chronos_cseg_plain_page_fuzz tests/fuzz/cseg_plain_page_fuzz.cpp)
  set_target_properties(chronos_cseg_plain_page_fuzz PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_cseg_plain_page_fuzz PRIVATE cxx_std_23)
  target_link_libraries(chronos_cseg_plain_page_fuzz PRIVATE chronos::cseg)
  target_compile_options(chronos_cseg_plain_page_fuzz PRIVATE -fsanitize=fuzzer)
  target_link_options(chronos_cseg_plain_page_fuzz PRIVATE -fsanitize=fuzzer)
  chronos_set_project_warnings(chronos_cseg_plain_page_fuzz)
  chronos_enable_sanitizers(chronos_cseg_plain_page_fuzz)
  chronos_enable_static_analysis(chronos_cseg_plain_page_fuzz)

  add_executable(chronos_cseg_page_codec_fuzz tests/fuzz/cseg_page_codec_fuzz.cpp)
  set_target_properties(chronos_cseg_page_codec_fuzz PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_cseg_page_codec_fuzz PRIVATE cxx_std_23)
  target_link_libraries(chronos_cseg_page_codec_fuzz PRIVATE chronos::cseg)
  target_compile_options(chronos_cseg_page_codec_fuzz PRIVATE -fsanitize=fuzzer)
  target_link_options(chronos_cseg_page_codec_fuzz PRIVATE -fsanitize=fuzzer)
  chronos_set_project_warnings(chronos_cseg_page_codec_fuzz)
  chronos_enable_sanitizers(chronos_cseg_page_codec_fuzz)
  chronos_enable_static_analysis(chronos_cseg_page_codec_fuzz)

  add_executable(chronos_cseg_part_codec_fuzz tests/fuzz/cseg_part_codec_fuzz.cpp)
  set_target_properties(chronos_cseg_part_codec_fuzz PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_cseg_part_codec_fuzz PRIVATE cxx_std_23)
  target_link_libraries(chronos_cseg_part_codec_fuzz PRIVATE chronos::cseg)
  target_compile_options(chronos_cseg_part_codec_fuzz PRIVATE -fsanitize=fuzzer)
  target_link_options(chronos_cseg_part_codec_fuzz PRIVATE -fsanitize=fuzzer)
  chronos_set_project_warnings(chronos_cseg_part_codec_fuzz)
  chronos_enable_sanitizers(chronos_cseg_part_codec_fuzz)
  chronos_enable_static_analysis(chronos_cseg_part_codec_fuzz)

  add_executable(chronos_manifest_codec_fuzz tests/fuzz/manifest_codec_fuzz.cpp)
  set_target_properties(chronos_manifest_codec_fuzz PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_manifest_codec_fuzz PRIVATE cxx_std_23)
  target_include_directories(
    chronos_manifest_codec_fuzz PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests"
  )
  target_link_libraries(chronos_manifest_codec_fuzz PRIVATE chronos::manifest)
  target_compile_options(chronos_manifest_codec_fuzz PRIVATE -fsanitize=fuzzer)
  target_link_options(chronos_manifest_codec_fuzz PRIVATE -fsanitize=fuzzer)
  chronos_set_project_warnings(chronos_manifest_codec_fuzz)
  chronos_enable_sanitizers(chronos_manifest_codec_fuzz)
  chronos_enable_static_analysis(chronos_manifest_codec_fuzz)

  add_executable(chronos_sql_lexer_fuzz tests/fuzz/sql_lexer_fuzz.cpp)
  set_target_properties(chronos_sql_lexer_fuzz PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_sql_lexer_fuzz PRIVATE cxx_std_23)
  target_link_libraries(chronos_sql_lexer_fuzz PRIVATE chronos::query)
  target_compile_options(chronos_sql_lexer_fuzz PRIVATE -fsanitize=fuzzer)
  target_link_options(chronos_sql_lexer_fuzz PRIVATE -fsanitize=fuzzer)
  chronos_set_project_warnings(chronos_sql_lexer_fuzz)
  chronos_enable_sanitizers(chronos_sql_lexer_fuzz)
  chronos_enable_static_analysis(chronos_sql_lexer_fuzz)

  add_executable(chronos_sql_parser_fuzz tests/fuzz/sql_parser_fuzz.cpp)
  set_target_properties(chronos_sql_parser_fuzz PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_sql_parser_fuzz PRIVATE cxx_std_23)
  target_link_libraries(chronos_sql_parser_fuzz PRIVATE chronos::query)
  target_compile_options(chronos_sql_parser_fuzz PRIVATE -fsanitize=fuzzer)
  target_link_options(chronos_sql_parser_fuzz PRIVATE -fsanitize=fuzzer)
  chronos_set_project_warnings(chronos_sql_parser_fuzz)
  chronos_enable_sanitizers(chronos_sql_parser_fuzz)
  chronos_enable_static_analysis(chronos_sql_parser_fuzz)

  add_executable(chronos_sql_binder_fuzz tests/fuzz/sql_binder_fuzz.cpp)
  set_target_properties(chronos_sql_binder_fuzz PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_sql_binder_fuzz PRIVATE cxx_std_23)
  target_link_libraries(chronos_sql_binder_fuzz PRIVATE chronos::query)
  target_compile_options(chronos_sql_binder_fuzz PRIVATE -fsanitize=fuzzer)
  target_link_options(chronos_sql_binder_fuzz PRIVATE -fsanitize=fuzzer)
  chronos_set_project_warnings(chronos_sql_binder_fuzz)
  chronos_enable_sanitizers(chronos_sql_binder_fuzz)
  chronos_enable_static_analysis(chronos_sql_binder_fuzz)

  add_executable(chronos_vector_chunk_fuzz tests/fuzz/vector_chunk_fuzz.cpp)
  set_target_properties(chronos_vector_chunk_fuzz PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_vector_chunk_fuzz PRIVATE cxx_std_23)
  target_link_libraries(chronos_vector_chunk_fuzz PRIVATE chronos::query)
  target_compile_options(chronos_vector_chunk_fuzz PRIVATE -fsanitize=fuzzer)
  target_link_options(chronos_vector_chunk_fuzz PRIVATE -fsanitize=fuzzer)
  chronos_set_project_warnings(chronos_vector_chunk_fuzz)
  chronos_enable_sanitizers(chronos_vector_chunk_fuzz)
  chronos_enable_static_analysis(chronos_vector_chunk_fuzz)

  add_executable(chronos_grouped_aggregate_fuzz tests/fuzz/grouped_aggregate_fuzz.cpp)
  set_target_properties(chronos_grouped_aggregate_fuzz PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_grouped_aggregate_fuzz PRIVATE cxx_std_23)
  target_link_libraries(chronos_grouped_aggregate_fuzz PRIVATE chronos::query)
  target_compile_options(chronos_grouped_aggregate_fuzz PRIVATE -fsanitize=fuzzer)
  target_link_options(chronos_grouped_aggregate_fuzz PRIVATE -fsanitize=fuzzer)
  chronos_set_project_warnings(chronos_grouped_aggregate_fuzz)
  chronos_enable_sanitizers(chronos_grouped_aggregate_fuzz)
  chronos_enable_static_analysis(chronos_grouped_aggregate_fuzz)

  add_executable(chronos_asof_join_fuzz tests/fuzz/asof_join_fuzz.cpp)
  set_target_properties(chronos_asof_join_fuzz PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_asof_join_fuzz PRIVATE cxx_std_23)
  target_link_libraries(chronos_asof_join_fuzz PRIVATE chronos::query)
  target_compile_options(chronos_asof_join_fuzz PRIVATE -fsanitize=fuzzer)
  target_link_options(chronos_asof_join_fuzz PRIVATE -fsanitize=fuzzer)
  chronos_set_project_warnings(chronos_asof_join_fuzz)
  chronos_enable_sanitizers(chronos_asof_join_fuzz)
  chronos_enable_static_analysis(chronos_asof_join_fuzz)

  add_executable(chronos_relational_plan_fuzz tests/fuzz/relational_plan_fuzz.cpp)
  set_target_properties(chronos_relational_plan_fuzz PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_relational_plan_fuzz PRIVATE cxx_std_23)
  target_link_libraries(chronos_relational_plan_fuzz PRIVATE chronos::query)
  target_compile_options(chronos_relational_plan_fuzz PRIVATE -fsanitize=fuzzer)
  target_link_options(chronos_relational_plan_fuzz PRIVATE -fsanitize=fuzzer)
  chronos_set_project_warnings(chronos_relational_plan_fuzz)
  chronos_enable_sanitizers(chronos_relational_plan_fuzz)
  chronos_enable_static_analysis(chronos_relational_plan_fuzz)

  add_executable(chronos_physical_plan_fuzz tests/fuzz/physical_plan_fuzz.cpp)
  set_target_properties(chronos_physical_plan_fuzz PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_physical_plan_fuzz PRIVATE cxx_std_23)
  target_link_libraries(chronos_physical_plan_fuzz PRIVATE chronos::query)
  target_compile_options(chronos_physical_plan_fuzz PRIVATE -fsanitize=fuzzer)
  target_link_options(chronos_physical_plan_fuzz PRIVATE -fsanitize=fuzzer)
  chronos_set_project_warnings(chronos_physical_plan_fuzz)
  chronos_enable_sanitizers(chronos_physical_plan_fuzz)
  chronos_enable_static_analysis(chronos_physical_plan_fuzz)

  add_executable(chronos_physical_optimizer_fuzz tests/fuzz/physical_optimizer_fuzz.cpp)
  set_target_properties(chronos_physical_optimizer_fuzz PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_physical_optimizer_fuzz PRIVATE cxx_std_23)
  target_link_libraries(chronos_physical_optimizer_fuzz PRIVATE chronos::query)
  target_compile_options(chronos_physical_optimizer_fuzz PRIVATE -fsanitize=fuzzer)
  target_link_options(chronos_physical_optimizer_fuzz PRIVATE -fsanitize=fuzzer)
  chronos_set_project_warnings(chronos_physical_optimizer_fuzz)
  chronos_enable_sanitizers(chronos_physical_optimizer_fuzz)
  chronos_enable_static_analysis(chronos_physical_optimizer_fuzz)

  add_executable(chronos_physical_lowering_fuzz tests/fuzz/physical_lowering_fuzz.cpp)
  set_target_properties(chronos_physical_lowering_fuzz PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_physical_lowering_fuzz PRIVATE cxx_std_23)
  target_link_libraries(chronos_physical_lowering_fuzz PRIVATE chronos::query)
  target_compile_options(chronos_physical_lowering_fuzz PRIVATE -fsanitize=fuzzer)
  target_link_options(chronos_physical_lowering_fuzz PRIVATE -fsanitize=fuzzer)
  chronos_set_project_warnings(chronos_physical_lowering_fuzz)
  chronos_enable_sanitizers(chronos_physical_lowering_fuzz)
  chronos_enable_static_analysis(chronos_physical_lowering_fuzz)

  add_executable(chronos_parallel_scheduler_fuzz tests/fuzz/parallel_scheduler_fuzz.cpp)
  set_target_properties(chronos_parallel_scheduler_fuzz PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_parallel_scheduler_fuzz PRIVATE cxx_std_23)
  target_link_libraries(chronos_parallel_scheduler_fuzz PRIVATE chronos::query Threads::Threads)
  target_compile_options(chronos_parallel_scheduler_fuzz PRIVATE -fsanitize=fuzzer)
  target_link_options(chronos_parallel_scheduler_fuzz PRIVATE -fsanitize=fuzzer)
  chronos_set_project_warnings(chronos_parallel_scheduler_fuzz)
  chronos_enable_sanitizers(chronos_parallel_scheduler_fuzz)
  chronos_enable_static_analysis(chronos_parallel_scheduler_fuzz)

  add_executable(chronos_spill_sort_fuzz tests/fuzz/spill_sort_fuzz.cpp)
  set_target_properties(chronos_spill_sort_fuzz PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_spill_sort_fuzz PRIVATE cxx_std_23)
  target_link_libraries(chronos_spill_sort_fuzz PRIVATE chronos::query)
  target_compile_options(chronos_spill_sort_fuzz PRIVATE -fsanitize=fuzzer)
  target_link_options(chronos_spill_sort_fuzz PRIVATE -fsanitize=fuzzer)
  chronos_set_project_warnings(chronos_spill_sort_fuzz)
  chronos_enable_sanitizers(chronos_spill_sort_fuzz)
  chronos_enable_static_analysis(chronos_spill_sort_fuzz)

  add_executable(chronos_cseg_scan_fuzz tests/fuzz/cseg_scan_fuzz.cpp)
  set_target_properties(chronos_cseg_scan_fuzz PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_cseg_scan_fuzz PRIVATE cxx_std_23)
  target_include_directories(chronos_cseg_scan_fuzz PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests")
  target_link_libraries(chronos_cseg_scan_fuzz PRIVATE chronos::query zstd::libzstd)
  target_compile_options(chronos_cseg_scan_fuzz PRIVATE -fsanitize=fuzzer)
  target_link_options(chronos_cseg_scan_fuzz PRIVATE -fsanitize=fuzzer)
  chronos_set_project_warnings(chronos_cseg_scan_fuzz)
  chronos_enable_sanitizers(chronos_cseg_scan_fuzz)
  chronos_enable_static_analysis(chronos_cseg_scan_fuzz)

  add_executable(chronos_head_scan_fuzz tests/fuzz/head_scan_fuzz.cpp)
  set_target_properties(chronos_head_scan_fuzz PROPERTIES CXX_EXTENSIONS OFF)
  target_compile_features(chronos_head_scan_fuzz PRIVATE cxx_std_23)
  target_include_directories(chronos_head_scan_fuzz PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests")
  target_link_libraries(chronos_head_scan_fuzz PRIVATE chronos::query)
  target_compile_options(chronos_head_scan_fuzz PRIVATE -fsanitize=fuzzer)
  target_link_options(chronos_head_scan_fuzz PRIVATE -fsanitize=fuzzer)
  chronos_set_project_warnings(chronos_head_scan_fuzz)
  chronos_enable_sanitizers(chronos_head_scan_fuzz)
  chronos_enable_static_analysis(chronos_head_scan_fuzz)
endif()

chronos_configure_installation()
