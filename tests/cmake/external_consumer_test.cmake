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
target_link_libraries(consumer PRIVATE chronos::cluster chronos::cseg chronos::head chronos::ingest chronos::manifest chronos::query chronos::network chronos::service chronos::tiering)
if(TARGET chronos::interop)
  target_link_libraries(consumer PRIVATE chronos::interop)
  target_compile_definitions(consumer PRIVATE CHRONOS_TEST_HAS_INTEROP=1)
endif()
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
#ifdef CHRONOS_TEST_HAS_INTEROP
#include <chronos/interop/arrow_parquet.hpp>
#endif
#include <chronos/cluster/tablet_physical_receipt_reclamation.hpp>
#include <chronos/cluster/distributed_query_transport.hpp>
#include <chronos/cluster/distributed_query_execution.hpp>
#include <chronos/cluster/distributed_query_tls_client.hpp>
#include <chronos/cluster/distributed_query_tls_server.hpp>
#include <chronos/cluster/distributed_query_tcp_server.hpp>
#include <chronos/cluster/distributed_query_tcp_client.hpp>
#include <chronos/cluster/distributed_query_tcp_execution.hpp>
#include <chronos/service/replicated_distributed_query.hpp>
#include <chronos/cseg/compression.hpp>
#include <chronos/cseg/format.hpp>
#include <chronos/cseg/inspection.hpp>
#include <chronos/cseg/layout.hpp>
#include <chronos/cseg/metadata_codec.hpp>
#include <chronos/cseg/part_codec.hpp>
#include <chronos/cseg/page_codec.hpp>
#include <chronos/cseg/plain_page.hpp>
#include <chronos/cseg/projected_reader.hpp>
#include <chronos/cseg/pruning.hpp>
#include <chronos/cseg/types.hpp>
#include <chronos/cseg/validator.hpp>
#include <chronos/head/mutable_head.hpp>
#include <chronos/ingest/columnar_append.hpp>
#include <chronos/ingest/columnar_append_executor.hpp>
#include <chronos/ingest/columnar_append_format.hpp>
#include <chronos/ingest/columnar_append_recovery.hpp>
#include <chronos/ingest/identity.hpp>
#include <chronos/ingest/retry_directory.hpp>
#include <chronos/ingest/sealed_head_flush_queue.hpp>
#include <chronos/ingest/sha256.hpp>
#include <chronos/ingest/tablet_state.hpp>
#include <chronos/manifest/format.hpp>
#include <chronos/manifest/checkpoint_builder.hpp>
#include <chronos/manifest/codec.hpp>
#include <chronos/manifest/compaction.hpp>
#include <chronos/manifest/compaction_coordinator.hpp>
#include <chronos/manifest/compaction_equivalence.hpp>
#include <chronos/manifest/compaction_planner.hpp>
#include <chronos/manifest/generation_builder.hpp>
#include <chronos/manifest/layout.hpp>
#include <chronos/manifest/naming.hpp>
#include <chronos/manifest/part_validation.hpp>
#include <chronos/manifest/publication.hpp>
#include <chronos/manifest/raft_tablet_physical_snapshot.hpp>
#include <chronos/manifest/sealed_head_flush.hpp>
#include <chronos/manifest/sealed_head_flush_coordinator.hpp>
#include <chronos/manifest/startup_recovery.hpp>
#include <chronos/manifest/storage.hpp>
#include <chronos/manifest/temporal_publication.hpp>
#include <chronos/query/asof_join.hpp>
#include <chronos/query/relational_plan.hpp>
#include <chronos/query/lexer.hpp>
#include <chronos/query/latest.hpp>
#include <chronos/query/literal.hpp>
#include <chronos/query/parser.hpp>
#include <chronos/query/parallel_scheduler.hpp>
#include <chronos/query/aggregate.hpp>
#include <chronos/query/column_output.hpp>
#include <chronos/query/cseg_scan.hpp>
#include <chronos/query/database_cseg_scan.hpp>
#include <chronos/query/distributed.hpp>
#include <chronos/query/distributed_fragment.hpp>
#include <chronos/query/distributed_fragment_binding.hpp>
#include <chronos/query/distributed_fragment_dispatch.hpp>
#include <chronos/query/distributed_fragment_worker.hpp>
#include <chronos/query/head_scan.hpp>
#include <chronos/query/physical_operator.hpp>
#include <chronos/query/physical_lowering.hpp>
#include <chronos/query/physical_optimizer.hpp>
#include <chronos/query/physical_plan.hpp>
#include <chronos/query/resource_context.hpp>
#include <chronos/query/row_version.hpp>
#include <chronos/query/catalog.hpp>
#include <chronos/query/binder.hpp>
#include <chronos/query/evaluator.hpp>
#include <chronos/query/executor.hpp>
#include <chronos/query/explain.hpp>
#include <chronos/query/snapshot.hpp>
#include <chronos/query/snapshot_pipeline.hpp>
#include <chronos/query/sort.hpp>
#include <chronos/query/spill_sort.hpp>
#include <chronos/query/statement_binder.hpp>
#include <chronos/query/timestamp_range.hpp>
#include <chronos/query/value.hpp>
#include <chronos/query/vector_chunk.hpp>
#include <chronos/query/vector_expression.hpp>
#include <chronos/manifest/types.hpp>
#include <chronos/manifest/validation.hpp>
#include <chronos/network/protocol.hpp>
#include <chronos/network/security.hpp>
#include <chronos/network/tcp_socket.hpp>
#include <chronos/network/tls_socket.hpp>
#include <chronos/tiering/cold_manifest.hpp>
#include <chronos/tiering/cold_manifest_storage.hpp>
#include <chronos/tiering/object_store.hpp>
#include <chronos/tiering/tiered_distributed_fragment_worker.hpp>
#include <chronos/tiering/tiered_part_loader.hpp>
#include <chronos/tiering/tiered_parts.hpp>
#include <chronos/tiering/tiered_publication.hpp>
#include <chronos/tiering/tiered_reclamation.hpp>
#include <chronos/tiering/tiered_pair_commit.hpp>
#include <chronos/network/spsc_queue.hpp>
#include <chronos/network/messages.hpp>
#include <chronos/network/connection_state.hpp>
#include <chronos/network/connection_buffers.hpp>
#include <chronos/network/client_session.hpp>
#include <chronos/network/epoll_reactor.hpp>
#include <chronos/wal/application.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

int main() {
  const auto encode_distributed_query_request =
      &chronos::cluster::encode_distributed_query_request_v1;
  const auto decode_distributed_query_response =
      &chronos::cluster::decode_distributed_query_response_v1;
  using ConsumeDistributedQueryRequest = chronos::common::Result<
      chronos::cluster::DistributedQueryRequestReadStep> (
      chronos::cluster::DistributedQueryRequestReader::*)(chronos::common::ByteView);
  const ConsumeDistributedQueryRequest consume_distributed_query_request =
      &chronos::cluster::DistributedQueryRequestReader::consume;
  const auto create_distributed_query_sender =
      &chronos::cluster::DistributedQuerySender::create;
  const auto create_distributed_query_execution =
      &chronos::cluster::DistributedQueryExecution::create;
  const auto create_distributed_query_execution_from_bound_snapshot =
      &chronos::cluster::DistributedQueryExecution::create_from_bound_snapshot;
  const auto create_replicated_distributed_aggregate_query =
      &chronos::service::create_replicated_distributed_aggregate_query;
  const auto create_replicated_follower_distributed_aggregate_query =
      &chronos::service::create_replicated_follower_distributed_aggregate_query;
  const auto create_distributed_query_tls_client =
      &chronos::cluster::DistributedQueryTlsClient::create;
  const auto create_distributed_query_tls_server =
      &chronos::cluster::DistributedQueryTlsServer::create;
  const auto start_distributed_query_tcp_server =
      &chronos::cluster::DistributedQueryTcpServer::start;
  const auto begin_distributed_query_tcp_client =
      &chronos::cluster::DistributedQueryTcpClient::begin;
  const auto create_distributed_query_tcp_execution =
      &chronos::cluster::DistributedQueryTcpExecution::create;
  const auto cancel_distributed_query_tcp_execution =
      &chronos::cluster::DistributedQueryTcpExecution::cancel;
  const auto rebind_distributed_query_tcp_execution =
      &chronos::cluster::DistributedQueryTcpExecution::rebind;
  const auto bind_compatible_distributed_snapshot =
      &chronos::query::bind_compatible_distributed_aggregate_snapshot;
  const auto bind_follower_group_backed_distributed_snapshot =
      &chronos::query::bind_follower_group_backed_distributed_aggregate_snapshot;
  const auto create_tls_client_context = &chronos::network::TlsClientContext::create;
  const auto connect_tls_socket = &chronos::network::TlsSocket::connect;
  const auto begin_tcp_connect = &chronos::network::TcpSocket::begin_connect;
  const auto bind_tcp_listener = &chronos::network::TcpListener::bind;
  const auto create_s3_object_store = &chronos::tiering::S3ObjectStore::create;
  const auto create_s3_environment_provider =
      &chronos::tiering::S3EnvironmentCredentialProvider::create;
  const auto create_s3_provider_chain = &chronos::tiering::S3CredentialProviderChain::create;
  const auto create_s3_container_provider =
      &chronos::tiering::S3ContainerCredentialProvider::create;
  const auto create_s3_instance_provider =
      &chronos::tiering::S3InstanceCredentialProvider::create;
  using RestoreTieredCatalog = chronos::common::Status (
      chronos::tiering::TieredPartManager::*)(
      std::span<const chronos::tiering::ColdPartDescriptor>);
  const RestoreTieredCatalog restore_tiered_catalog =
      &chronos::tiering::TieredPartManager::restore_catalog;
  const auto remove_exact_object = &chronos::tiering::ObjectStore::remove_if_exact;
  const auto encode_cold_manifest = &chronos::tiering::encode_cold_location_manifest_v1;
  const auto decode_cold_manifest = &chronos::tiering::decode_cold_location_manifest_v1_exact;
  const auto create_cold_manifest_storage = &chronos::tiering::ColdLocationManifestStorage::create;
  const auto cold_manifest_file_name = &chronos::tiering::cold_location_manifest_file_name;
  const auto create_tiered_storage_publisher =
      &chronos::tiering::TieredDatabaseStoragePublisher::create;
  const auto create_tiered_pair_storage = &chronos::tiering::TieredPairCommitStorage::create;
  const auto decode_tiered_pair = &chronos::tiering::decode_tiered_pair_commit_v1_exact;
  const auto load_tiered_parts = &chronos::tiering::load_tiered_temporal_part_images;
  const auto load_remote_temporal_part =
      &chronos::tiering::load_validated_remote_temporal_part_image;
  const auto execute_tiered_distributed_fragment =
      &chronos::tiering::execute_tiered_distributed_aggregate_fragment;
  const auto authorize_tiered_local_reclamation =
      &chronos::tiering::TieredLocalPartReclamationCoordinator::authorize;
  const auto reclaim_tiered_local_parts =
      &chronos::tiering::TieredLocalPartReclamationCoordinator::reclaim;
  const auto authorize_tiered_remote_reclamation =
      &chronos::tiering::TieredRemoteObjectReclamationCoordinator::authorize;
  const auto reclaim_tiered_remote_objects =
      &chronos::tiering::TieredRemoteObjectReclamationCoordinator::reclaim;
  const auto load_selected_tiered_pair =
      &chronos::tiering::TieredPairCommitStorage::load_selected_record;
  const auto reclaim_physical_receipt =
      &chronos::cluster::reclaim_tablet_physical_part_receipt;
  (void)encode_distributed_query_request;
  (void)decode_distributed_query_response;
  (void)consume_distributed_query_request;
  (void)create_distributed_query_sender;
  (void)create_distributed_query_execution;
  (void)create_distributed_query_execution_from_bound_snapshot;
  (void)create_replicated_distributed_aggregate_query;
  (void)create_replicated_follower_distributed_aggregate_query;
  (void)create_distributed_query_tls_client;
  (void)create_distributed_query_tls_server;
  (void)start_distributed_query_tcp_server;
  (void)begin_distributed_query_tcp_client;
  (void)create_distributed_query_tcp_execution;
  (void)cancel_distributed_query_tcp_execution;
  (void)rebind_distributed_query_tcp_execution;
  (void)bind_compatible_distributed_snapshot;
  (void)bind_follower_group_backed_distributed_snapshot;
  (void)create_tls_client_context;
  (void)connect_tls_socket;
  (void)begin_tcp_connect;
  (void)bind_tcp_listener;
  (void)create_s3_object_store;
  (void)create_s3_environment_provider;
  (void)restore_tiered_catalog;
  (void)remove_exact_object;
  (void)encode_cold_manifest;
  (void)decode_cold_manifest;
  (void)create_cold_manifest_storage;
  (void)cold_manifest_file_name;
  (void)create_tiered_storage_publisher;
  (void)create_tiered_pair_storage;
  (void)decode_tiered_pair;
  (void)load_tiered_parts;
  (void)load_remote_temporal_part;
  (void)execute_tiered_distributed_fragment;
  (void)authorize_tiered_local_reclamation;
  (void)reclaim_tiered_local_parts;
  (void)authorize_tiered_remote_reclamation;
  (void)reclaim_tiered_remote_objects;
  (void)load_selected_tiered_pair;
  const auto build_source_retirement =
      &chronos::manifest::build_raft_tablet_source_retirement_manifest;
  const auto publish_source_retirement =
      &chronos::manifest::TemporalDatabaseStoragePublisher::publish_source_retirement_manifest;
  const auto reclaim_source_parts =
      &chronos::manifest::ManifestStorage::reclaim_retired_temporal_parts;
  const auto recover_source_retirement =
      &chronos::manifest::ManifestStorage::recover_temporal_source_retirement;
  using EventTimeMatchFunction = chronos::common::Result<bool> (*)(
      std::int64_t, std::int64_t,
      const std::optional<chronos::cseg::EventTimePredicate>&);
  const EventTimeMatchFunction event_time_match =
      &chronos::cseg::cseg_event_time_range_may_match;
  using ExecuteFunction = chronos::common::Result<chronos::ingest::ColumnarAppendExecutionResult> (*)(
      const chronos::ingest::ColumnarAppendExecutionInput&, chronos::ingest::RetryDirectory&,
      chronos::ingest::TabletState&, chronos::wal::WalCommitCoordinator&);
  const ExecuteFunction execute = &chronos::ingest::execute_columnar_append;
  using RecoverFunction =
      chronos::common::Result<chronos::ingest::RecoveredColumnarAppendState> (*)(
          const chronos::wal::WalWriterConfig&, const chronos::wal::WalRecoveryOptions&,
          chronos::ingest::ColumnarAppendRecoveryConfig);
  const RecoverFunction recover = &chronos::ingest::recover_columnar_append_wal;
  using ReclaimRecoveredWalFunction =
      chronos::common::Result<chronos::wal::WalSegmentReclamationReport> (
          chronos::ingest::RecoveredColumnarAppendState::*)(
          const chronos::wal::WalReplayCheckpoint&);
  const ReclaimRecoveredWalFunction reclaim_recovered_wal =
      &chronos::ingest::RecoveredColumnarAppendState::reclaim_checkpointed_segments;
  using InspectWalSuffixFunction = chronos::common::Result<chronos::wal::WalRecoveryReport> (*)(
      std::string_view, const chronos::wal::WalReplayCheckpoint&,
      chronos::wal::WalReplaySink&);
  const InspectWalSuffixFunction inspect_wal_suffix = &chronos::wal::inspect_wal_suffix;
  using RecoverWalCheckpointFunction =
      chronos::common::Result<chronos::wal::WalRecoveryReport> (*)(
          const chronos::wal::WalWriterConfig&, const chronos::wal::WalRecoveryOptions&,
          const chronos::wal::WalReplayCheckpoint&, chronos::wal::WalReplaySink&);
  const RecoverWalCheckpointFunction recover_wal_checkpoint =
      &chronos::wal::recover_wal_from_checkpoint;
  using OpenWalCheckpointFunction = chronos::common::Result<chronos::wal::WalWriter> (*)(
      const chronos::wal::WalWriterConfig&, const chronos::wal::WalRecoveryOptions&,
      const chronos::wal::WalReplayCheckpoint&, chronos::wal::WalReplaySink&);
  const OpenWalCheckpointFunction open_wal_checkpoint =
      &chronos::wal::WalWriter::open_existing_from_checkpoint;
  using ReclaimWalFunction = chronos::common::Result<chronos::wal::WalSegmentReclamationReport> (
      chronos::wal::WalWriter::*)(const chronos::wal::WalReplayCheckpoint&);
  const ReclaimWalFunction reclaim_wal =
      &chronos::wal::WalWriter::reclaim_checkpointed_segments;
  using WalReclamationMetricsFunction = chronos::wal::WalSegmentReclamationMetrics (
      chronos::wal::WalWriter::*)() const noexcept;
  const WalReclamationMetricsFunction wal_reclamation_metrics =
      &chronos::wal::WalWriter::reclamation_metrics;
  using RegisterSchemaFunction = chronos::common::Status (chronos::ingest::TabletState::*)(
      std::shared_ptr<const chronos::schema::TableSchema>, chronos::head::MutableHeadCapacity);
  const RegisterSchemaFunction register_schema = &chronos::ingest::TabletState::register_schema;
  using RetireSealedGenerationFunction =
      chronos::common::Result<chronos::ingest::TabletSnapshot> (
          chronos::ingest::TabletState::*)(
          const chronos::ingest::SealedGenerationRetirementReceipt&);
  const RetireSealedGenerationFunction retire_sealed_generation =
      &chronos::ingest::TabletState::retire_sealed_generation;
  using CreateFlushQueueFunction =
      chronos::common::Result<std::shared_ptr<chronos::ingest::SealedHeadFlushQueue>> (*)(
          chronos::ingest::SealedHeadFlushQueueConfig);
  const CreateFlushQueueFunction create_flush_queue =
      &chronos::ingest::SealedHeadFlushQueue::create;
  using AcquireFlushWorkFunction =
      chronos::common::Result<std::optional<chronos::ingest::SealedHeadFlushWork>> (
          chronos::ingest::SealedHeadFlushQueue::*)();
  const AcquireFlushWorkFunction acquire_flush_work =
      &chronos::ingest::SealedHeadFlushQueue::try_acquire;
  static_assert(chronos::columnar::bitmap_size(9U) == 2U);
  static_assert(chronos::columnar::format::kBatchHeaderLength == 96U);
  static_assert(chronos::cseg::format::kFileHeaderLength == 256U);
  const std::array<std::uint64_t, 5> page_lengths{8U, 8U, 8U, 8U, 8U};
  const auto cseg_layout = chronos::cseg::plan_cseg_v1_layout(
      {.user_column_count = 1U, .granule_count = 1U}, page_lengths);
  const std::array<std::byte, 1> page{std::byte{0x41}};
  const auto stored_page =
      chronos::cseg::compress_cseg_page_v1(page, chronos::cseg::PageCompression::kNone);
  const auto cseg_metadata = chronos::cseg::decode_cseg_v1_metadata_prefix({});
  const std::array<std::byte, 1> bool_values{std::byte{1U}};
  const auto physical = chronos::columnar::PhysicalColumnView::create(
      {.type = chronos::schema::LogicalType::create(
                   chronos::schema::LogicalTypeKind::kBool)
                   .value(),
       .nullable = false,
       .row_count = 1U,
       .null_count = 0U},
      {.validity = {}, .offsets = {}, .values = bool_values});
  const auto plain_page = physical.has_value()
                              ? chronos::cseg::encode_cseg_v1_plain_page(*physical)
                              : chronos::common::Result<chronos::cseg::EncodedCsegPlainPage>{
                                    chronos::common::make_unexpected(physical.error())};
  const auto encoded_cseg_page =
      physical.has_value()
          ? chronos::cseg::encode_cseg_v1_page(*physical,
                                               chronos::cseg::PageCompression::kNone)
          : chronos::common::Result<chronos::cseg::EncodedCsegPage>{
                chronos::common::make_unexpected(physical.error())};
  using DecodeCsegPageFunction = chronos::common::Result<chronos::cseg::DecodedCsegPage> (*)(
      chronos::common::ByteView, const chronos::cseg::CsegColumnDescriptor&,
      const chronos::cseg::CsegPageDescriptor&);
  const DecodeCsegPageFunction decode_cseg_page = &chronos::cseg::decode_cseg_v1_page;
  using DecodeCsegPartFunction = chronos::cseg::CsegPartDecodeResult (*)(
      chronos::common::ByteView, chronos::cseg::CsegMetadataDecodeLimits);
  const DecodeCsegPartFunction decode_cseg_part = &chronos::cseg::decode_cseg_v1_part_prefix;
  const auto cseg_part = decode_cseg_part({}, {});
  using ValidateCsegPartFunction = chronos::common::Status (*)(
      const chronos::cseg::DecodedCsegPartView&, chronos::cseg::CsegValidationLimits);
  const ValidateCsegPartFunction validate_cseg_part =
      &chronos::cseg::validate_cseg_v1_part_contents;
  using InspectCsegPartFunction = chronos::cseg::CsegInspectionResult (*)(
      chronos::common::ByteView, chronos::cseg::CsegInspectionLimits);
  const InspectCsegPartFunction inspect_cseg_part = &chronos::cseg::inspect_cseg_v1_part;
  using OpenProjectedReaderFunction = chronos::cseg::CsegProjectedReaderOpenResult (*)(
      chronos::common::ByteView, const chronos::schema::SchemaLineage&,
      chronos::schema::SchemaId, const chronos::schema::TabletId&,
      chronos::cseg::CsegProjectedReaderLimits);
  const OpenProjectedReaderFunction open_projected_reader =
      &chronos::cseg::open_cseg_v1_projected_reader_prefix;
  using PlanProjectedGranuleFunction =
      chronos::common::Result<chronos::cseg::CsegProjectedGranuleReadPlan> (
          chronos::cseg::CsegProjectedReaderView::*)(
          std::size_t, std::span<const std::uint32_t>) const;
  const PlanProjectedGranuleFunction plan_projected_granule =
      &chronos::cseg::CsegProjectedReaderView::plan_granule;
  using CreateCsegPartPinFunction = chronos::common::Result<chronos::query::CsegPartPin> (*)(
      std::shared_ptr<const void>, chronos::common::ByteView, std::size_t);
  const CreateCsegPartPinFunction create_cseg_part_pin = &chronos::query::CsegPartPin::create;
  using CreateSharedCsegPartPinFunction =
      chronos::common::Result<chronos::query::CsegPartPin> (*)(
          std::shared_ptr<const void>, chronos::common::ByteView,
          chronos::query::CsegPartPinRetainedBytes);
  const CreateSharedCsegPartPinFunction create_shared_cseg_part_pin =
      &chronos::query::CsegPartPin::create_with_shared_retained_bytes;
  using PinSnapshotCsegPartFunction = chronos::common::Result<chronos::query::CsegPartPin> (*)(
      std::shared_ptr<const chronos::manifest::SnapshotPartImage>);
  const PinSnapshotCsegPartFunction pin_snapshot_cseg_part =
      &chronos::query::pin_snapshot_cseg_part;
  using CreatePrunedCsegScanFunction =
      chronos::common::Result<std::unique_ptr<chronos::query::PhysicalOperator>> (*)(
          const chronos::query::QueryResourceContext&, chronos::query::CsegPartPin,
          const chronos::schema::SchemaLineage&, chronos::schema::SchemaId,
          const chronos::schema::TabletId&, std::vector<std::uint32_t>,
          chronos::cseg::EventTimePredicate, chronos::query::CsegScanLimits);
  const CreatePrunedCsegScanFunction create_pruned_cseg_scan =
      &chronos::query::CsegScanOperator::create_event_time_pruned;
  using CreateSharedCsegScanFunction =
      chronos::common::Result<std::unique_ptr<chronos::query::PhysicalOperator>> (*)(
          const chronos::query::QueryResourceContext&, chronos::query::CsegPartPin,
          chronos::query::QuerySharedMemoryReservation, const chronos::schema::SchemaLineage&,
          chronos::schema::SchemaId, const chronos::schema::TabletId&,
          std::vector<std::uint32_t>, chronos::query::CsegScanLimits);
  const CreateSharedCsegScanFunction create_shared_cseg_scan =
      &chronos::query::CsegScanOperator::create_with_shared_pin;
  using CreateSharedPrunedCsegScanFunction =
      chronos::common::Result<std::unique_ptr<chronos::query::PhysicalOperator>> (*)(
          const chronos::query::QueryResourceContext&, chronos::query::CsegPartPin,
          chronos::query::QuerySharedMemoryReservation, const chronos::schema::SchemaLineage&,
          chronos::schema::SchemaId, const chronos::schema::TabletId&,
          std::vector<std::uint32_t>, chronos::cseg::EventTimePredicate,
          chronos::query::CsegScanLimits);
  const CreateSharedPrunedCsegScanFunction create_shared_pruned_cseg_scan =
      &chronos::query::CsegScanOperator::create_event_time_pruned_with_shared_pin;
  using PlanSnapshotCsegPartScanFunction =
      chronos::common::Result<chronos::query::SnapshotCsegPartScanPlan> (*)(
          const chronos::manifest::DatabaseStorageSnapshot&,
          const chronos::schema::TabletId&,
          const std::optional<chronos::cseg::EventTimePredicate>&,
          chronos::query::SnapshotCsegPartScanPlanLimits);
  const PlanSnapshotCsegPartScanFunction plan_snapshot_cseg_part_scan =
      &chronos::query::plan_snapshot_cseg_part_scan;
  using LoadSnapshotCsegPartScanImagesFunction =
      chronos::common::Result<std::vector<
          std::shared_ptr<const chronos::manifest::SnapshotPartImage>>> (*)(
          const chronos::manifest::ManifestStorage&,
          const chronos::manifest::DatabaseStorageSnapshot&,
          const chronos::query::SnapshotCsegPartScanPlan&,
          const chronos::schema::SchemaLineage&,
          chronos::manifest::ReferencedPartValidationLimits);
  const LoadSnapshotCsegPartScanImagesFunction load_snapshot_cseg_part_scan_images =
      &chronos::query::load_snapshot_cseg_part_scan_images;
  using CreateSnapshotCsegPartScanFunction =
      chronos::common::Result<std::unique_ptr<chronos::query::PhysicalOperator>> (*)(
          const chronos::query::QueryResourceContext&,
          const chronos::query::SnapshotCsegPartScanPlan&,
          std::vector<std::shared_ptr<const chronos::manifest::SnapshotPartImage>>,
          const chronos::schema::SchemaLineage&, chronos::schema::SchemaId,
          const std::vector<std::uint32_t>&, chronos::query::CsegScanLimits);
  const CreateSnapshotCsegPartScanFunction create_snapshot_cseg_part_scan =
      &chronos::query::create_snapshot_cseg_part_scan;
  using CreateSharedSnapshotCsegPartScanFunction =
      chronos::common::Result<std::unique_ptr<chronos::query::PhysicalOperator>> (*)(
          const chronos::query::QueryResourceContext&,
          chronos::query::QuerySharedMemoryReservation,
          const chronos::query::SnapshotCsegPartScanPlan&,
          std::vector<std::shared_ptr<const chronos::manifest::SnapshotPartImage>>,
          const chronos::schema::SchemaLineage&, chronos::schema::SchemaId,
          const std::vector<std::uint32_t>&, chronos::query::CsegScanLimits);
  const CreateSharedSnapshotCsegPartScanFunction create_shared_snapshot_cseg_part_scan =
      &chronos::query::create_snapshot_cseg_part_scan_with_shared_publication;
  using CreateSnapshotTabletScanFunction =
      chronos::common::Result<std::unique_ptr<chronos::query::PhysicalOperator>> (*)(
          const chronos::query::QueryResourceContext&,
          const chronos::manifest::DatabaseStorageSnapshot&,
          const chronos::query::SnapshotCsegPartScanPlan&,
          std::vector<std::shared_ptr<const chronos::manifest::SnapshotPartImage>>,
          const chronos::schema::SchemaLineage&, chronos::schema::SchemaId,
          const std::vector<std::uint32_t>&, chronos::query::SnapshotTabletScanLimits);
  const CreateSnapshotTabletScanFunction create_snapshot_tablet_scan =
      &chronos::query::create_snapshot_tablet_scan;
  using CreateSharedSnapshotTabletScanFunction =
      chronos::common::Result<std::unique_ptr<chronos::query::PhysicalOperator>> (*)(
          const chronos::query::QueryResourceContext&,
          chronos::query::QuerySharedMemoryReservation,
          const chronos::manifest::DatabaseStorageSnapshot&,
          const chronos::query::SnapshotCsegPartScanPlan&,
          std::vector<std::shared_ptr<const chronos::manifest::SnapshotPartImage>>,
          const chronos::schema::SchemaLineage&, chronos::schema::SchemaId,
          const std::vector<std::uint32_t>&, chronos::query::SnapshotTabletScanLimits);
  const CreateSharedSnapshotTabletScanFunction create_shared_snapshot_tablet_scan =
      &chronos::query::create_snapshot_tablet_scan_with_shared_publication;
  using InstantiateSnapshotTabletPipelineFunction =
      chronos::common::Result<std::unique_ptr<chronos::query::PhysicalOperator>> (*)(
          const chronos::query::QueryResourceContext&,
          const chronos::manifest::ManifestStorage&,
          const chronos::manifest::DatabaseStorageSnapshot&,
          const chronos::schema::TabletId&, const chronos::schema::SchemaLineage&,
          chronos::schema::SchemaId, const chronos::query::PhysicalPipelinePlan&,
          chronos::query::SnapshotTabletPipelineLimits);
  const InstantiateSnapshotTabletPipelineFunction instantiate_snapshot_tablet_pipeline =
      &chronos::query::instantiate_snapshot_tablet_pipeline;
  using InstantiateOptimizedSnapshotTabletPipelineFunction =
      chronos::common::Result<std::unique_ptr<chronos::query::PhysicalOperator>> (*)(
          const chronos::query::QueryResourceContext&,
          const chronos::manifest::ManifestStorage&,
          const chronos::manifest::DatabaseStorageSnapshot&,
          const chronos::schema::TabletId&, const chronos::schema::SchemaLineage&,
          chronos::schema::SchemaId,
          const chronos::query::OptimizedPhysicalPipelinePlan&,
          std::vector<chronos::query::ExternalSortExecutionTarget>,
          chronos::query::SnapshotTabletPipelineLimits);
  const InstantiateOptimizedSnapshotTabletPipelineFunction
      instantiate_optimized_snapshot_tablet_pipeline =
          &chronos::query::instantiate_optimized_snapshot_tablet_pipeline;
  using InstantiateSnapshotAsofPlanFunction =
      chronos::common::Result<std::unique_ptr<chronos::query::PhysicalOperator>> (*)(
          const chronos::query::QueryResourceContext&,
          const chronos::manifest::ManifestStorage&,
          const chronos::manifest::DatabaseStorageSnapshot&,
          std::span<const chronos::query::SnapshotTabletSourceBinding>,
          const chronos::query::PhysicalAsofPlan&);
  const InstantiateSnapshotAsofPlanFunction instantiate_snapshot_asof_plan =
      &chronos::query::instantiate_snapshot_asof_plan;
  using CreateHeadScanFunction =
      chronos::common::Result<std::unique_ptr<chronos::query::PhysicalOperator>> (*)(
          const chronos::query::QueryResourceContext&, chronos::head::HeadSnapshot,
          const chronos::schema::SchemaLineage&, chronos::schema::SchemaId,
          const chronos::schema::TabletId&, std::vector<std::uint32_t>,
          chronos::query::HeadScanLimits);
  const CreateHeadScanFunction create_head_scan = &chronos::query::HeadScanOperator::create;
  using CreateSharedHeadScanFunction =
      chronos::common::Result<std::unique_ptr<chronos::query::PhysicalOperator>> (*)(
          const chronos::query::QueryResourceContext&, chronos::head::HeadSnapshot,
          chronos::query::QuerySharedMemoryReservation,
          const chronos::schema::SchemaLineage&, chronos::schema::SchemaId,
          const chronos::schema::TabletId&, std::vector<std::uint32_t>,
          chronos::query::HeadScanLimits);
  const CreateSharedHeadScanFunction create_shared_head_scan =
      &chronos::query::HeadScanOperator::create_with_shared_publication;
  using CreateExactHeadScanFunction =
      chronos::common::Result<std::unique_ptr<chronos::query::PhysicalOperator>> (*)(
          const chronos::query::QueryResourceContext&, chronos::head::HeadSnapshot,
          const chronos::schema::SchemaLineage&, chronos::schema::SchemaId,
          const chronos::schema::TabletId&, std::vector<std::uint32_t>,
          chronos::query::TimestampRangePredicate, chronos::query::HeadScanLimits);
  const CreateExactHeadScanFunction create_exact_head_scan =
      &chronos::query::HeadScanOperator::create_event_time_filtered;
  using CreateSharedExactHeadScanFunction =
      chronos::common::Result<std::unique_ptr<chronos::query::PhysicalOperator>> (*)(
          const chronos::query::QueryResourceContext&, chronos::head::HeadSnapshot,
          chronos::query::QuerySharedMemoryReservation,
          const chronos::schema::SchemaLineage&, chronos::schema::SchemaId,
          const chronos::schema::TabletId&, std::vector<std::uint32_t>,
          chronos::query::TimestampRangePredicate, chronos::query::HeadScanLimits);
  const CreateSharedExactHeadScanFunction create_shared_exact_head_scan =
      &chronos::query::HeadScanOperator::create_event_time_filtered_with_shared_publication;
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
  const std::optional<chronos::ingest::ColumnarRecoveryTabletSeed> durable_recovery_seed;
  static_assert(chronos::ingest::columnar_append_v1::kCommandHeaderLength == 160U);
  static_assert(chronos::manifest::format::kFileHeaderLength == 256U);
  const auto manifest_layout = chronos::manifest::plan_manifest_v1_layout({});
  const auto sql_tokens = chronos::query::tokenize_sql_v1("SELECT * FROM metrics");
  const auto sql_timestamp =
      chronos::query::parse_sql_timestamp_ns_literal("1970-01-01 00:00:00.000000001Z");
  const auto sql_select = chronos::query::parse_sql_v1_select("SELECT * FROM metrics");
  using ParseCreateFunction = chronos::query::SqlResult<chronos::query::ParsedSqlCreateTable> (*)(
      std::string_view, chronos::query::SqlParserLimits);
  using ParseInsertFunction = chronos::query::SqlResult<chronos::query::ParsedSqlInsert> (*)(
      std::string_view, chronos::query::SqlParserLimits);
  const ParseCreateFunction parse_create = &chronos::query::parse_sql_v1_create_table;
  const ParseInsertFunction parse_insert = &chronos::query::parse_sql_v1_insert;
  const auto query_catalog = chronos::query::QueryCatalogSnapshot::create(1U, {});
  const auto query_scalar = chronos::query::ScalarValue::float64(1.0);
  const auto query_resources = chronos::query::QueryResourceContext::create(1'024U);
  using ReserveSharedFunction =
      chronos::common::Result<chronos::query::QuerySharedMemoryReservation> (
          chronos::query::QueryResourceContext::*)(std::size_t) const;
  const ReserveSharedFunction reserve_shared =
      &chronos::query::QueryResourceContext::reserve_shared;
  using CreateSharedAccountedChunkFunction =
      chronos::common::Result<chronos::query::AccountedVectorChunk> (*)(
          chronos::query::VectorChunk, chronos::query::QueryMemoryReservation,
          chronos::query::QuerySharedMemoryReservation,
          const chronos::query::QueryResourceContext&);
  const CreateSharedAccountedChunkFunction create_shared_accounted_chunk =
      &chronos::query::AccountedVectorChunk::create;
  using CreateParallelMergeFunction =
      chronos::common::Result<std::unique_ptr<chronos::query::ParallelMergeOperator>> (*)(
          const chronos::query::QueryResourceContext&,
          std::vector<std::unique_ptr<chronos::query::PhysicalOperator>>,
          chronos::query::ParallelSchedulerLimits,
          std::span<const chronos::runtime::ThreadPlacement>);
  const CreateParallelMergeFunction create_parallel_merge =
      &chronos::query::ParallelMergeOperator::create;
  const auto physical_end = chronos::query::PhysicalOperatorStep::end();
  const auto row_version_layout = chronos::query::vector_row_version_layout(2U);
  const auto row_version_type = chronos::query::vector_row_version_column_type(
      chronos::query::VectorRowVersionColumnKind::kWalId);
  using CreateColumnSubsetFunction =
      chronos::common::Result<std::unique_ptr<chronos::query::PhysicalOperator>> (*)(
          std::unique_ptr<chronos::query::PhysicalOperator>, std::vector<std::size_t>);
  const CreateColumnSubsetFunction create_column_subset =
      &chronos::query::ColumnSubsetOperator::create;
  using CreateSourceColumnOutputFunction =
      chronos::common::Result<std::unique_ptr<chronos::query::PhysicalOperator>> (*)(
          std::unique_ptr<chronos::query::PhysicalOperator>, std::vector<std::size_t>,
          chronos::query::VectorChunkLimits);
  const CreateSourceColumnOutputFunction create_source_column_output =
      &chronos::query::SourceColumnOutputOperator::create;
  using CreateColumnOutputFunction =
      chronos::common::Result<std::unique_ptr<chronos::query::PhysicalOperator>> (*)(
          std::unique_ptr<chronos::query::PhysicalOperator>,
          std::vector<chronos::query::ColumnOutputPosition>, chronos::query::VectorChunkLimits);
  const CreateColumnOutputFunction create_column_output =
      &chronos::query::ColumnOutputOperator::create;
  using CreateVectorExpressionFunction = chronos::common::Result<chronos::query::VectorExpression> (*)(
      std::vector<chronos::query::VectorExpressionInstruction>,
      chronos::query::VectorExpressionLimits);
  const CreateVectorExpressionFunction create_vector_expression =
      &chronos::query::VectorExpression::create;
  const auto installed_expression_type =
      chronos::schema::LogicalType::create(chronos::schema::LogicalTypeKind::kInt64).value();
  const chronos::query::VectorCastExpression installed_cast{
      .operand_instruction = 0U, .target_type = installed_expression_type};
  const auto installed_coalesce = chronos::query::VectorBinaryOperation::kCoalesce;
  const auto installed_lower = chronos::query::VectorUnaryOperation::kLowerAscii;
  const auto installed_text_type =
      chronos::schema::LogicalType::create(chronos::schema::LogicalTypeKind::kString).value();
  std::vector<chronos::query::VectorExpressionInstruction> installed_text_instructions;
  installed_text_instructions.emplace_back(chronos::query::VectorConstantExpression{
      chronos::query::ScalarValue::text(installed_text_type, "a").value()});
  installed_text_instructions.emplace_back(chronos::query::VectorConstantExpression{
      chronos::query::ScalarValue::text(installed_text_type, "b").value()});
  installed_text_instructions.emplace_back(chronos::query::VectorBinaryExpression{
      .operation = chronos::query::VectorBinaryOperation::kLess,
      .left_instruction = 0U,
      .right_instruction = 1U});
  const auto installed_text_predicate = create_vector_expression(
      std::move(installed_text_instructions), chronos::query::VectorExpressionLimits{});
  using LowerSelectFunction = chronos::query::SqlResult<chronos::query::PhysicalPipelinePlan> (*)(
      const chronos::query::BoundSqlSelect&, chronos::query::PhysicalSelectLoweringLimits);
  const LowerSelectFunction lower_select = &chronos::query::lower_bound_sql_select;
  using LowerAsofSelectFunction = chronos::query::SqlResult<chronos::query::PhysicalAsofPlan> (*)(
      const chronos::query::BoundSqlSelect&, chronos::query::PhysicalSelectLoweringLimits);
  const LowerAsofSelectFunction lower_asof_select =
      &chronos::query::lower_bound_sql_asof_select;
  const chronos::query::PhysicalSelectLoweringLimits installed_lowering_limits{};
  const auto installed_order_key_limit = installed_lowering_limits.sort_limits.maximum_keys;
  using CreateTimestampRangeFilterFunction =
      chronos::common::Result<std::unique_ptr<chronos::query::PhysicalOperator>> (*)(
          std::unique_ptr<chronos::query::PhysicalOperator>, std::size_t,
          chronos::query::TimestampRangePredicate);
  const CreateTimestampRangeFilterFunction create_timestamp_range_filter =
      &chronos::query::TimestampRangeFilterOperator::create;
  using CreateLimitFunction =
      chronos::common::Result<std::unique_ptr<chronos::query::PhysicalOperator>> (*)(
          std::unique_ptr<chronos::query::PhysicalOperator>, std::uint64_t);
  const CreateLimitFunction create_limit = &chronos::query::LimitOperator::create;
  const chronos::query::TimestampRangePredicate timestamp_range{
      .lower = chronos::query::TimestampRangeBound{.value = -1, .inclusive = false},
      .upper = chronos::query::TimestampRangeBound{.value = 1, .inclusive = true}};
  const auto physical_plan = chronos::query::PhysicalPipelinePlan::create({}, {});
  const chronos::query::VectorAggregateDefinition installed_count{
      .operation = chronos::query::VectorAggregateOperation::kCountStar,
      .input = std::nullopt};
  const auto installed_count_shape =
      chronos::query::vector_aggregate_output_shape(installed_count);
  const auto installed_aggregate_plan = chronos::query::PhysicalPipelinePlan::create(
      {}, {chronos::query::UngroupedAggregateStage{.definitions = {installed_count}}});
  const chronos::query::VectorGroupKeyDefinition installed_group_key{
      .column_ordinal = 0U, .type = installed_expression_type, .nullable = false};
  const auto installed_grouped_plan = chronos::query::PhysicalPipelinePlan::create(
      {{.type = installed_expression_type, .nullable = false}},
      {chronos::query::GroupedAggregateStage{.keys = {installed_group_key},
                                             .definitions = {installed_count}}});
  const chronos::query::SortLimits installed_sort_limits{
      .maximum_rows = 8U,
      .maximum_keys = 1U,
      .maximum_state_bytes = 1U << 20U,
      .output_limits = {.maximum_rows = 8U,
                        .maximum_columns = 1U,
                        .maximum_buffer_bytes = 4'096U,
                        .maximum_retained_buffer_bytes = 8'192U}};
  const auto installed_sort_state_bytes =
      chronos::query::sort_state_reservation_bytes(installed_sort_limits);
  const auto installed_spill_state_bytes =
      chronos::query::spill_sort_configuration_reservation_bytes(
          chronos::query::SpillSortLimits{});
  const auto installed_sort_plan = chronos::query::PhysicalPipelinePlan::create(
      {{.type = installed_expression_type, .nullable = false}},
      {chronos::query::SortStage{
          .keys = {{.column_ordinal = 0U,
                    .direction = chronos::query::PhysicalSortDirection::kDescending,
                    .null_placement = chronos::query::ScalarNullPlacement::kFirst}},
          .limits = installed_sort_limits}});
  using CreateOptimizedPipelineFunction =
      chronos::common::Result<chronos::query::OptimizedPhysicalPipelinePlan> (*)(
          chronos::query::PhysicalPipelinePlan,
          chronos::query::PhysicalExecutionStatistics,
          chronos::query::PhysicalExecutionCapabilities,
          chronos::query::PhysicalOptimizerPolicy);
  const CreateOptimizedPipelineFunction create_optimized_pipeline =
      &chronos::query::OptimizedPhysicalPipelinePlan::create;
  using CreateLatestFunction = chronos::common::Result<std::unique_ptr<chronos::query::PhysicalOperator>> (*)(
      std::unique_ptr<chronos::query::PhysicalOperator>,
      chronos::query::VectorLatestByDefinition, chronos::query::LatestByLimits);
  const CreateLatestFunction create_latest = &chronos::query::LatestByOperator::create;
  const auto installed_asof_state_bytes = chronos::query::asof_join_state_reservation_bytes(
      chronos::query::AsofJoinLimits{});
  const chronos::query::ConstantColumnOutputPosition installed_nullable_constant{
      .value = chronos::query::ScalarValue::signed_value(installed_expression_type, 1).value(),
      .force_nullable = true};
  const auto vector_chunk = chronos::query::VectorChunk::create(
      {}, chronos::query::VectorSelection::all(1U).value());
  const auto backed_vector_chunk = chronos::query::VectorChunk::create_backed(
      {}, chronos::query::VectorSelection::all(1U).value());
  using BindSelectFunction = chronos::query::SqlResult<chronos::query::BoundSqlSelect> (*)(
      chronos::query::ParsedSqlSelect,
      std::shared_ptr<const chronos::query::QueryCatalogSnapshot>,
      chronos::query::SqlBinderLimits);
  const BindSelectFunction bind_select = &chronos::query::bind_sql_v1_select;
  using BindCreateFunction = chronos::query::SqlResult<chronos::query::BoundSqlCreateTable> (*)(
      chronos::query::ParsedSqlCreateTable,
      std::shared_ptr<const chronos::query::QueryCatalogSnapshot>);
  const BindCreateFunction bind_create = &chronos::query::bind_sql_v1_create_table;
  using BindInsertFunction = chronos::query::SqlResult<chronos::query::BoundSqlInsert> (*)(
      chronos::query::ParsedSqlInsert,
      const std::shared_ptr<const chronos::query::QueryCatalogSnapshot>&,
      chronos::query::SqlInsertBinderLimits);
  const BindInsertFunction bind_insert = &chronos::query::bind_sql_v1_insert;
  using MaterializeInsertFunction =
      chronos::query::SqlResult<chronos::query::MaterializedSqlInsert> (*)(
          const chronos::query::BoundSqlInsert&);
  const MaterializeInsertFunction materialize_insert =
      &chronos::query::materialize_sql_v1_insert_rows;
  using EvaluateExpressionFunction = chronos::query::SqlResult<chronos::query::ScalarValue> (*)(
      const chronos::query::BoundSqlSelect&, const chronos::query::SqlExpression&,
      const chronos::query::ScalarEvaluationContext&);
  const EvaluateExpressionFunction evaluate_expression =
      &chronos::query::evaluate_sql_v1_expression;
  using CreateScalarSnapshotFunction = chronos::common::Result<chronos::query::ScalarTableSnapshot> (
      *)(std::shared_ptr<const chronos::schema::TableSchema>, std::uint64_t,
         std::vector<chronos::query::ScalarInputRow>);
  const CreateScalarSnapshotFunction create_scalar_snapshot =
      &chronos::query::ScalarTableSnapshot::create;
  using ExecuteSelectFunction = chronos::query::SqlResult<chronos::query::ScalarQueryResult> (*)(
      const chronos::query::BoundSqlSelect&, const chronos::query::ScalarSnapshotProvider&,
      chronos::query::ScalarQueryLimits);
  const ExecuteSelectFunction execute_select = &chronos::query::execute_sql_v1_select;
  using AggregateQueryFunction = bool (chronos::query::BoundSqlSelect::*)() const noexcept;
  const AggregateQueryFunction aggregate_query = &chronos::query::BoundSqlSelect::aggregate_query;
  const auto encode_exchange_message = &chronos::query::encode_exchange_message;
  const auto decode_exchange_message = &chronos::query::decode_exchange_message_exact;
  const auto consume_exchange_frame = &chronos::query::ExchangeFrameReader::consume;
  const auto create_exchange_write_cursor = &chronos::query::ExchangeFrameWriteCursor::create;
  const auto create_distributed_coordinator =
      &chronos::query::DistributedAggregateCoordinator::create;
  const auto encode_distributed_fragment =
      &chronos::query::encode_distributed_aggregate_fragment;
  const auto decode_distributed_fragment =
      &chronos::query::decode_distributed_aggregate_fragment_exact;
  const auto encode_distributed_fragment_dispatch =
      &chronos::query::encode_distributed_aggregate_fragment_dispatch;
  const auto decode_distributed_fragment_dispatch =
      &chronos::query::decode_distributed_aggregate_fragment_dispatch_exact;
  const auto bind_distributed_fragment =
      &chronos::query::bind_distributed_aggregate_fragment;
  using ExecuteDistributedFragment = chronos::common::Result<chronos::query::ExchangeMessage> (*)(
      const chronos::query::DistributedAggregateWorkerRequest&);
  const ExecuteDistributedFragment execute_distributed_fragment =
      &chronos::query::execute_distributed_aggregate_fragment;
  using ExplainFunction = chronos::query::SqlResult<std::string> (*)(
      const chronos::query::BoundSqlSelect&);
  const ExplainFunction explain_select = &chronos::query::explain_sql_v1_select;
  using ExecuteAnalyzeFunction =
      chronos::query::SqlResult<chronos::query::ScalarExplainAnalyzeResult> (*)(
          const chronos::query::BoundSqlSelect&,
          const chronos::query::ScalarSnapshotProvider&, chronos::query::ScalarQueryLimits);
  const ExecuteAnalyzeFunction execute_analyze =
      &chronos::query::execute_sql_v1_explain_analyze;
  const auto manifest_name = chronos::manifest::manifest_file_name(1U);
  using DecodeManifestFunction = chronos::manifest::ManifestDecodeResult (*)(
      chronos::common::ByteView, chronos::manifest::ManifestDecodeLimits);
  const DecodeManifestFunction decode_manifest = &chronos::manifest::decode_manifest_v1_exact;
  using ValidateManifestTransitionFunction = chronos::common::Status (*)(
      const chronos::manifest::DecodedManifestView&, const chronos::manifest::DecodedManifestView&,
      std::span<const chronos::manifest::TabletSchemaBinding>);
  const ValidateManifestTransitionFunction validate_manifest_transition =
      &chronos::manifest::validate_manifest_v1_transition;
  using ValidateManifestCompactionTransitionFunction = chronos::common::Status (*)(
      const chronos::manifest::DecodedManifestView&, const chronos::manifest::DecodedManifestView&,
      std::span<const chronos::manifest::TabletSchemaBinding>,
      const chronos::manifest::ManifestCompactionReplacement&);
  const ValidateManifestCompactionTransitionFunction validate_manifest_compaction_transition =
      &chronos::manifest::validate_manifest_v1_compaction_transition;
  using ValidateCompactionEquivalenceFunction = chronos::common::Status (*)(
      std::span<const chronos::manifest::CompactionPartImage>,
      std::span<const chronos::manifest::CompactionPartImage>,
      const chronos::schema::TableSchema&, const chronos::schema::TabletId&,
      const chronos::wal::WalId&, chronos::manifest::CompactionEquivalenceLimits);
  const ValidateCompactionEquivalenceFunction validate_compaction_equivalence =
      &chronos::manifest::validate_append_only_cseg_v1_equivalence;
  using MergeCompactionFunction = chronos::common::Result<chronos::manifest::EncodedCompactionPart> (*)(
      const chronos::manifest::AppendOnlyCompactionRequest&);
  const MergeCompactionFunction merge_compaction =
      &chronos::manifest::merge_append_only_cseg_v1;
  using BuildCompactionManifestFunction = chronos::common::Result<chronos::manifest::EncodedManifest> (*)(
      const chronos::manifest::AppendOnlyCompactionManifestBuildInput&);
  const BuildCompactionManifestFunction build_compaction_manifest =
      &chronos::manifest::build_manifest_v1_for_append_only_compaction;
  using ValidateReferencedPartsFunction = chronos::common::Status (*)(
      const chronos::manifest::DecodedManifestView&,
      std::span<const chronos::manifest::TabletSchemaBinding>,
      std::span<const chronos::manifest::ReferencedPartImage>,
      chronos::manifest::ReferencedPartValidationLimits);
  const ValidateReferencedPartsFunction validate_referenced_parts =
      &chronos::manifest::validate_manifest_v1_referenced_parts;
  using OpenManifestStorageFunction = chronos::common::Result<chronos::manifest::ManifestStorage> (*)(
      const chronos::manifest::ManifestStorageConfig&);
  const OpenManifestStorageFunction open_manifest_storage =
      &chronos::manifest::ManifestStorage::open_existing;
  using ScanManifestNamespaceFunction =
      chronos::common::Result<chronos::manifest::ManifestNamespaceSnapshot> (
          chronos::manifest::ManifestStorage::*)() const;
  const ScanManifestNamespaceFunction scan_manifest_namespace =
      &chronos::manifest::ManifestStorage::scan_namespace;
  using CleanupManifestTemporariesFunction =
      chronos::common::Result<chronos::manifest::TemporaryCleanupReport> (
          chronos::manifest::ManifestStorage::*)();
  const CleanupManifestTemporariesFunction cleanup_manifest_temporaries =
      &chronos::manifest::ManifestStorage::cleanup_temporaries;
  using ReclaimRetiredPartsFunction =
      chronos::common::Result<chronos::manifest::PartReclamationReport> (
          chronos::manifest::ManifestStorage::*)(
          const chronos::manifest::PartReclamationRequest&);
  const ReclaimRetiredPartsFunction reclaim_retired_parts =
      &chronos::manifest::ManifestStorage::reclaim_retired_parts;
  using InstallManifestFunction = chronos::common::Result<chronos::manifest::InstalledManifest> (
      chronos::manifest::ManifestStorage::*)(const chronos::manifest::ManifestInstallRequest&);
  const InstallManifestFunction install_manifest =
      &chronos::manifest::ManifestStorage::install_manifest;
  using ManifestMetricsFunction = chronos::manifest::ManifestInstallationMetrics (
      chronos::manifest::ManifestStorage::*)() const noexcept;
  const ManifestMetricsFunction manifest_metrics =
      &chronos::manifest::ManifestStorage::manifest_metrics;
  using LoadSelectedManifestFunction =
      chronos::common::Result<chronos::manifest::LoadedManifestGeneration> (
          chronos::manifest::ManifestStorage::*)(
          const chronos::manifest::ManifestLoadRequest&) const;
  const LoadSelectedManifestFunction load_selected_manifest =
      &chronos::manifest::ManifestStorage::load_selected_manifest;
  const auto load_temporal_manifest_metadata =
      &chronos::manifest::ManifestStorage::load_temporal_manifest_metadata;
  const auto reclaim_tiered_temporal_parts =
      &chronos::manifest::ManifestStorage::reclaim_tiered_local_temporal_parts;
  using LoadSelectedPartImagesFunction =
      chronos::common::Result<std::vector<chronos::manifest::LoadedPartImage>> (
          chronos::manifest::ManifestStorage::*)(
          const chronos::manifest::LoadedManifestGeneration&,
          std::span<const chronos::cseg::PartId>,
          std::span<const chronos::manifest::TabletSchemaBinding>,
          chronos::manifest::ReferencedPartValidationLimits) const;
  const LoadSelectedPartImagesFunction load_selected_part_images =
      &chronos::manifest::ManifestStorage::load_selected_part_images;
  using LoadSnapshotPartImagesFunction =
      chronos::common::Result<std::vector<chronos::manifest::SnapshotPartImage>> (
          chronos::manifest::ManifestStorage::*)(
          const chronos::manifest::DatabaseStorageSnapshot&,
          std::span<const chronos::cseg::PartId>,
          std::span<const chronos::manifest::TabletSchemaBinding>,
          chronos::manifest::ReferencedPartValidationLimits) const;
  const LoadSnapshotPartImagesFunction load_snapshot_part_images =
      &chronos::manifest::ManifestStorage::load_snapshot_part_images;
  using SnapshotPublicationBytesFunction =
      std::size_t (chronos::manifest::SnapshotPartImage::*)() const noexcept;
  const SnapshotPublicationBytesFunction snapshot_publication_bytes =
      &chronos::manifest::SnapshotPartImage::publication_retained_buffer_bytes;
  using FlushSealedHeadFunction =
      chronos::common::Result<chronos::manifest::EncodedSealedHeadPart> (*)(
          const chronos::manifest::SealedHeadFlushRequest&);
  const FlushSealedHeadFunction flush_sealed_head = &chronos::manifest::encode_sealed_head_v1;
  using BuildManifestFunction =
      chronos::common::Result<chronos::manifest::EncodedManifest> (*)(
          const chronos::manifest::SealedHeadManifestBuildInput&);
  const BuildManifestFunction build_manifest =
      &chronos::manifest::build_manifest_v1_for_sealed_head;
  using BuildCheckpointFunction =
      chronos::common::Result<chronos::manifest::CheckpointedManifestGeneration> (*)(
          const chronos::manifest::ManifestCheckpointBuildInput&);
  const BuildCheckpointFunction build_checkpoint =
      &chronos::manifest::build_manifest_v1_checkpointed_generation;
  using CreateStoragePublisherFunction =
      chronos::common::Result<chronos::manifest::DatabaseStoragePublisher> (*)(
          std::shared_ptr<const chronos::manifest::LoadedManifestGeneration>,
          std::span<const chronos::manifest::DatabaseStorageTabletInput>);
  const CreateStoragePublisherFunction create_storage_publisher =
      &chronos::manifest::DatabaseStoragePublisher::create;
  using PublishCompactionFunction =
      chronos::common::Result<chronos::manifest::DatabaseStorageSnapshot> (
          chronos::manifest::DatabaseStoragePublisher::*)(
          const chronos::manifest::DurableCompactionPublicationRequest&);
  const PublishCompactionFunction publish_compaction =
      &chronos::manifest::DatabaseStoragePublisher::publish_compaction_manifest;
  using DrainRetiredPartsFunction =
      chronos::common::Result<std::vector<chronos::manifest::RetiredPartSet>> (
          chronos::manifest::DatabaseStoragePublisher::*)();
  const DrainRetiredPartsFunction drain_retired_parts =
      &chronos::manifest::DatabaseStoragePublisher::drain_retired_part_sets;
  using CreateCompactionCoordinatorFunction =
      chronos::common::Result<chronos::manifest::AppendOnlyCompactionCoordinator> (*)(
          chronos::manifest::ManifestStorage&, chronos::manifest::DatabaseStoragePublisher&);
  const CreateCompactionCoordinatorFunction create_compaction_coordinator =
      &chronos::manifest::AppendOnlyCompactionCoordinator::create;
  using PlanCompactionFunction =
      chronos::common::Result<std::optional<chronos::manifest::PlannedAppendOnlyCompaction>> (*)(
          std::span<const chronos::manifest::PartDescriptor>,
          chronos::manifest::AppendOnlyCompactionPlannerLimits);
  const PlanCompactionFunction plan_compaction =
      &chronos::manifest::plan_append_only_compaction;
  using CreateFlushCoordinatorFunction =
      chronos::common::Result<chronos::manifest::SealedHeadFlushCoordinator> (*)(
          std::shared_ptr<chronos::ingest::SealedHeadFlushQueue>,
          chronos::manifest::ManifestStorage&, chronos::manifest::DatabaseStoragePublisher&);
  const CreateFlushCoordinatorFunction create_flush_coordinator =
      &chronos::manifest::SealedHeadFlushCoordinator::create;
  using RecoverManifestColumnarFunction =
      chronos::common::Result<chronos::manifest::RecoveredManifestColumnarState> (*)(
          chronos::manifest::ManifestColumnarStartupConfig);
  const RecoverManifestColumnarFunction recover_manifest_columnar =
      &chronos::manifest::recover_manifest_columnar_database;
  const auto protocol_ping = chronos::network::encode_frame(
      {.message_type = chronos::network::MessageType::kPing, .request_id = 1U}, {});
  const auto connection_state = chronos::network::ServerConnectionState::create();
  const auto connection_buffers = chronos::network::ConnectionBuffers::create();
  const auto network_queue = chronos::network::SpscNetworkTaskQueue::create(4U);
  const chronos::network::EpollServerConfig epoll_config;
  const auto installed_result_type = chronos::schema::LogicalType::create(
      chronos::schema::LogicalTypeKind::kInt64);
  const chronos::network::NetworkSecurityConfig installed_security;
  const auto installed_client = chronos::network::NativeClientSession::create();
  return reclaim_physical_receipt != nullptr && build_source_retirement != nullptr &&
                 publish_source_retirement != nullptr &&
                 reclaim_source_parts != nullptr &&
                 recover_source_retirement != nullptr &&
                 event_time_match != nullptr &&
                 execute != nullptr && recover != nullptr &&
                 reclaim_recovered_wal != nullptr && inspect_wal_suffix != nullptr &&
                 recover_wal_checkpoint != nullptr && open_wal_checkpoint != nullptr &&
                 reclaim_wal != nullptr && wal_reclamation_metrics != nullptr &&
                 register_schema != nullptr && retire_sealed_generation != nullptr &&
                 create_flush_queue != nullptr && acquire_flush_work != nullptr &&
                 limits.max_columns == 4096U &&
                 head_capacity.row_capacity == 4U &&
                 tablet_config.maximum_schema_versions == 2U &&
                 !durable_recovery_seed.has_value() &&
                 tablet_config.maximum_sealed_generations == 2U && digest.has_value() &&
                 retry_directory.has_value() &&
                 retry_directory->metrics().maximum_entries == 8U && !decoded.has_value() &&
                 cseg_layout.has_value() && cseg_layout->total_length == 1'248U &&
                 manifest_layout.has_value() && manifest_layout->total_length == 264U &&
                 sql_tokens.has_value() && sql_tokens->tokens().size() == 5U &&
                 sql_timestamp.has_value() && *sql_timestamp == 1 &&
                 sql_select.has_value() && sql_select->items().size() == 1U &&
                 parse_create != nullptr && parse_insert != nullptr &&
                 query_catalog.has_value() && query_catalog->tables().empty() &&
                 query_scalar.has_value() && !query_scalar->is_null() &&
                 query_resources.has_value() &&
                 query_resources->available_memory_bytes() == 1'024U &&
                 reserve_shared != nullptr && create_shared_accounted_chunk != nullptr &&
                 create_parallel_merge != nullptr &&
                 chronos::query::kDefaultParallelSchedulerTaskLimit == 256U &&
                 chronos::query::kDefaultParallelSchedulerWorkerLimit == 16U &&
                 physical_end.kind() == chronos::query::PhysicalOperatorStepKind::kEnd &&
                 row_version_layout.has_value() &&
                 row_version_layout->wal_id_column_ordinal() == 2U &&
                 row_version_type.has_value() &&
                 row_version_type->kind() == chronos::schema::LogicalTypeKind::kUuid &&
                 create_column_subset != nullptr && create_source_column_output != nullptr &&
                 create_column_output != nullptr && create_vector_expression != nullptr &&
                 installed_cast.target_type == installed_expression_type &&
                 installed_coalesce == chronos::query::VectorBinaryOperation::kCoalesce &&
                 installed_lower == chronos::query::VectorUnaryOperation::kLowerAscii &&
                 installed_text_predicate.has_value() &&
                 installed_text_predicate->result_shape().type.kind() ==
                     chronos::schema::LogicalTypeKind::kBool &&
                 installed_count_shape.has_value() && !installed_count_shape->nullable &&
                 installed_aggregate_plan.has_value() &&
                 installed_aggregate_plan->output_columns().size() == 1U &&
                 installed_grouped_plan.has_value() &&
                 installed_grouped_plan->output_columns().size() == 2U &&
                 installed_sort_state_bytes.has_value() &&
                 installed_spill_state_bytes.has_value() &&
                 installed_sort_plan.has_value() &&
                 installed_sort_plan->output_columns().size() == 1U &&
                 create_optimized_pipeline != nullptr &&
                 chronos::query::kDefaultPhysicalOptimizerSortLimit == 256U &&
                 create_latest != nullptr &&
                 chronos::query::kDefaultLatestByKeyLimit == 256U &&
                 installed_asof_state_bytes.has_value() &&
                 chronos::query::kDefaultAsofJoinKeyLimit == 256U &&
                 chronos::query::kDefaultPhysicalAsofPlanJoinLimit == 63U &&
                 lower_asof_select != nullptr &&
                 installed_nullable_constant.force_nullable &&
                 installed_lowering_limits.grouped_aggregate_limits.maximum_groups ==
                     chronos::query::kMaximumGroupedAggregateGroups &&
                 installed_lowering_limits.aggregate_limits.maximum_variable_extremum_bytes ==
                     chronos::query::kDefaultAggregateExtremumByteLimit &&
                 installed_lowering_limits.grouped_aggregate_limits
                         .maximum_variable_extremum_bytes ==
                     chronos::query::kDefaultAggregateExtremumByteLimit &&
                 installed_order_key_limit == chronos::query::kDefaultSortKeyLimit &&
                 lower_select != nullptr &&
                 create_timestamp_range_filter != nullptr &&
                 timestamp_range.matches(0) && create_head_scan != nullptr &&
                 create_shared_head_scan != nullptr &&
                 create_exact_head_scan != nullptr && create_shared_exact_head_scan != nullptr &&
                 create_limit != nullptr &&
                 physical_plan.has_value() && physical_plan->output_columns().empty() &&
                 vector_chunk.has_value() && vector_chunk->selected_row_count() == 1U &&
                 !backed_vector_chunk.has_value() &&
                 chronos::query::kMaximumSqlV1Sources == 64U && aggregate_query != nullptr &&
                 encode_exchange_message != nullptr && decode_exchange_message != nullptr &&
                 consume_exchange_frame != nullptr && create_exchange_write_cursor != nullptr &&
                 create_distributed_coordinator != nullptr &&
                 encode_distributed_fragment != nullptr &&
                 decode_distributed_fragment != nullptr &&
                 encode_distributed_fragment_dispatch != nullptr &&
                 decode_distributed_fragment_dispatch != nullptr &&
                 bind_distributed_fragment != nullptr &&
                 execute_distributed_fragment != nullptr &&
                 bind_select != nullptr && bind_create != nullptr && bind_insert != nullptr &&
                 materialize_insert != nullptr &&
                 evaluate_expression != nullptr &&
                 create_scalar_snapshot != nullptr &&
                 execute_select != nullptr && explain_select != nullptr &&
                 execute_analyze != nullptr &&
                 protocol_ping.has_value() &&
                 protocol_ping->size() == chronos::network::kFrameHeaderSize &&
                 connection_state.has_value() &&
                 connection_buffers.has_value() &&
                 network_queue.has_value() && network_queue->capacity() == 4U &&
                 epoll_config.maximum_connections == 1024U &&
                 installed_result_type.has_value() &&
                 installed_security.mode ==
                     chronos::network::TransportSecurityMode::kLoopbackPlaintext &&
                 installed_client.has_value() &&
                 manifest_name.has_value() &&
                 *manifest_name == "manifest-00000000000000000001.cman" &&
                 decode_manifest != nullptr && validate_manifest_transition != nullptr &&
                 validate_manifest_compaction_transition != nullptr &&
                 validate_compaction_equivalence != nullptr &&
                 merge_compaction != nullptr &&
                 build_compaction_manifest != nullptr &&
                 validate_referenced_parts != nullptr &&
                 open_manifest_storage != nullptr &&
                 scan_manifest_namespace != nullptr &&
                 cleanup_manifest_temporaries != nullptr &&
                 reclaim_retired_parts != nullptr &&
                 install_manifest != nullptr && manifest_metrics != nullptr &&
                 load_selected_manifest != nullptr && load_temporal_manifest_metadata != nullptr &&
                 reclaim_tiered_temporal_parts != nullptr &&
                 load_selected_part_images != nullptr &&
                 load_snapshot_part_images != nullptr && snapshot_publication_bytes != nullptr &&
                 flush_sealed_head != nullptr &&
                 build_manifest != nullptr && build_checkpoint != nullptr &&
                 create_storage_publisher != nullptr && publish_compaction != nullptr &&
                 drain_retired_parts != nullptr &&
                 create_compaction_coordinator != nullptr &&
                 plan_compaction != nullptr &&
                 create_flush_coordinator != nullptr &&
                 recover_manifest_columnar != nullptr &&
                 stored_page.has_value() && stored_page->bytes().size() == 1U &&
                 plain_page.has_value() && plain_page->bytes().size() == 1U &&
                 encoded_cseg_page.has_value() && encoded_cseg_page->bytes().size() == 1U &&
                 decode_cseg_page != nullptr &&
                 !cseg_part.has_value() &&
                 cseg_part.error().kind() ==
                     chronos::cseg::CsegPartDecodeErrorKind::kIncomplete &&
                 validate_cseg_part != nullptr &&
                 inspect_cseg_part != nullptr &&
                 open_projected_reader != nullptr &&
                 plan_projected_granule != nullptr &&
                 create_cseg_part_pin != nullptr && create_shared_cseg_part_pin != nullptr &&
                 pin_snapshot_cseg_part != nullptr &&
                 create_pruned_cseg_scan != nullptr && create_shared_cseg_scan != nullptr &&
                 create_shared_pruned_cseg_scan != nullptr &&
                 plan_snapshot_cseg_part_scan != nullptr &&
                 load_snapshot_cseg_part_scan_images != nullptr &&
                 create_snapshot_cseg_part_scan != nullptr &&
                 create_shared_snapshot_cseg_part_scan != nullptr &&
                 create_snapshot_tablet_scan != nullptr &&
                 create_shared_snapshot_tablet_scan != nullptr &&
                 instantiate_snapshot_tablet_pipeline != nullptr &&
                 instantiate_optimized_snapshot_tablet_pipeline != nullptr &&
                 instantiate_snapshot_asof_plan != nullptr &&
                 !cseg_metadata.has_value() &&
                 cseg_metadata.error().kind() ==
                     chronos::cseg::CsegMetadataDecodeErrorKind::kIncomplete &&
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
