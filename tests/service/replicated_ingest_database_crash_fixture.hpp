#ifndef CHRONOS_TESTS_SERVICE_REPLICATED_INGEST_DATABASE_CRASH_FIXTURE_HPP_
#define CHRONOS_TESTS_SERVICE_REPLICATED_INGEST_DATABASE_CRASH_FIXTURE_HPP_

#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/ingest/columnar_append.hpp"
#include "chronos/ingest/retry_directory.hpp"
#include "chronos/ingest/tablet_state.hpp"
#include "chronos/raft/metadata_codec.hpp"
#include "chronos/raft/schema_definition_codec.hpp"
#include "chronos/raft/tablet_group_binding_codec.hpp"
#include "chronos/runtime/database_bootstrap.hpp"
#include "chronos/service/replicated_ingest_runtime.hpp"
#include "columnar/columnar_test_support.hpp"
#include "ingest/ingest_test_support.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace chronos::service::test {

[[nodiscard]] inline common::Uuid crash_id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(static_cast<std::byte>(seed));
  return common::Uuid{bytes};
}

[[nodiscard]] inline raft::GroupId crash_metadata_group() {
  return crash_id(0x90U);
}

[[nodiscard]] inline raft::GroupId crash_tablet_group() {
  return crash_id(0x91U);
}

[[nodiscard]] inline schema::TabletId crash_tablet_id() {
  return columnar::test::id<schema::TabletId>(92U);
}

[[nodiscard]] inline runtime::DatabaseBootstrapDescriptor crash_descriptor() {
  return {.database_id = crash_id(0x93U),
          .metadata_group_id = crash_metadata_group(),
          .local_node_id = 1U,
          .mutable_head_rows = 8U,
          .maximum_sealed_generations = 2U,
          .variable_column_bytes = 8U,
          .maximum_retry_entries = 8U,
          .wal_segment_target_bytes = std::uint64_t{64U} * 1024U,
          .raft_segment_target_bytes = std::uint64_t{64U} * 1024U};
}

[[nodiscard]] inline std::vector<raft::RaftGroupConfiguration> crash_groups() {
  return {{crash_metadata_group(), {1U}}, {crash_tablet_group(), {1U}}};
}

[[nodiscard]] inline runtime::DatabaseBootstrapConfig
new_crash_bootstrap_config(const std::filesystem::path& root) {
  return {.database_root = root.string(), .new_database = crash_descriptor()};
}

[[nodiscard]] inline runtime::DatabaseBootstrapConfig
existing_crash_bootstrap_config(const std::filesystem::path& root) {
  return {.database_root = root.string()};
}

[[nodiscard]] inline std::vector<std::byte> crash_command() {
  auto batch = columnar::OwnedColumnarBatch::create(columnar::test::batch_schema(),
                                                    columnar::test::batch_columns())
                   .value();
  const auto batch_bytes = columnar::encode_columnar_batch_v1(batch).value();
  const auto append = ingest::encode_columnar_append_v1(
                          {.client_id = ingest::test::request_id<ingest::ClientId>(11U),
                           .client_batch_id = ingest::test::request_id<ingest::ClientBatchId>(43U),
                           .tablet_id = crash_tablet_id()},
                          batch_bytes)
                          .value();
  return {append.bytes().begin(), append.bytes().end()};
}

[[nodiscard]] inline ReplicatedIngestRuntimeConfig
crash_runtime_config(const runtime::DatabaseBootstrap& bootstrap) {
  auto tablet = ingest::TabletState::create(
                    columnar::test::batch_schema(), crash_tablet_id(),
                    {.head_capacity = {.row_capacity = 8U, .variable_value_bytes = {0U, 8U, 0U}},
                     .maximum_schema_versions = 1U,
                     .maximum_sealed_generations = 2U,
                     .maximum_retry_entries = 8U})
                    .value();
  auto retries = ingest::RetryDirectory::create({.maximum_entries = 8U}).value();
  std::vector<ingest::AsyncRaftTabletApplicationConfig> tablets;
  tablets.push_back({.group_id = crash_tablet_group(),
                     .snapshot_storage = std::nullopt,
                     .retry_directory = std::move(retries),
                     .tablet = std::move(tablet),
                     .retained_schemas = {columnar::test::batch_schema()},
                     .decode_limits = {}});
  return {.local_node_id = 1U,
          .log = {.directory_path = bootstrap.raft_directory_path(),
                  .target_segment_size = crash_descriptor().raft_segment_target_bytes},
          .groups = crash_groups(),
          .tablets = std::move(tablets),
          .metadata = {.group_id = crash_metadata_group()}};
}

[[nodiscard]] inline std::vector<raft::DurableRaftRequest> crash_metadata_requests() {
  const raft::ProposeOperation schema{
      raft::kRaftSchemaDefinitionEntryType,
      raft::encode_schema_definition_v1(
          {.name = "events", .quoted = false, .schema = columnar::test::batch_schema()})
          .value()};
  const raft::ProposeOperation policy{
      raft::kRaftMetadataCommandEntryType,
      raft::encode_metadata_command_v1(
          raft::TablePolicyMetadata{columnar::test::batch_schema()->table_id(), 1'000'000'000LL,
                                    86'400'000'000'000LL, 86'400'000'000'000LL, 0LL, 8U})
          .value()};
  const raft::ProposeOperation placement{
      raft::kRaftMetadataCommandEntryType,
      raft::encode_metadata_command_v1(
          raft::TabletPlacementMetadata{
              columnar::test::batch_schema()->table_id(), crash_tablet_id(), 1U, {1U}, 1U})
          .value()};
  const raft::ProposeOperation binding{
      raft::kRaftTabletGroupBindingEntryType,
      raft::encode_tablet_group_binding_v1({crash_tablet_id(), crash_tablet_group()}).value()};
  return {{crash_metadata_group(), schema},
          {crash_metadata_group(), policy},
          {crash_metadata_group(), placement},
          {crash_metadata_group(), binding}};
}

} // namespace chronos::service::test

#endif // CHRONOS_TESTS_SERVICE_REPLICATED_INGEST_DATABASE_CRASH_FIXTURE_HPP_
