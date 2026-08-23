#include "chronos/raft/metadata_runtime.hpp"
#include "chronos/raft/schema_definition_codec.hpp"
#include "chronos/raft/tablet_group_binding_codec.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-metadata-raft-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  TemporaryDirectory(const TemporaryDirectory&) = delete;
  TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

[[nodiscard]] GroupId group_id() {
  common::Uuid::Bytes bytes{};
  bytes.fill(std::byte{0x55U});
  return GroupId{bytes};
}

[[nodiscard]] GroupId tablet_group_id() {
  common::Uuid::Bytes bytes{};
  bytes.fill(std::byte{0x56U});
  return GroupId{bytes};
}

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] ProposeOperation proposal(MetadataCommand command) {
  return ProposeOperation{kRaftMetadataCommandEntryType,
                          encode_metadata_command_v1(std::move(command)).value()};
}

[[nodiscard]] CatalogTableDefinition schema_definition() {
  const auto table = id<schema::TableId>(20U);
  const auto schema_id = id<schema::SchemaId>(21U);
  const auto timestamp = id<schema::ColumnId>(22U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(
                        timestamp, "ts",
                        schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(),
                        false)
                        .value());
  auto value = schema::TableSchema::create(table, schema_id, schema::SchemaVersion::initial(),
                                           std::nullopt, std::move(columns),
                                           {.event_time_column = timestamp,
                                            .physical_ordering_key = {timestamp},
                                            .partition_columns = {timestamp},
                                            .shard_key = {timestamp},
                                            .deduplication_key = {timestamp}});
  return {.name = "events",
          .quoted = false,
          .schema = std::make_shared<const schema::TableSchema>(std::move(*value))};
}

[[nodiscard]] ProposeOperation schema_proposal(const CatalogTableDefinition& definition) {
  return {kRaftSchemaDefinitionEntryType, encode_schema_definition_v1(definition).value()};
}

[[nodiscard]] ProposeOperation binding_proposal(const schema::TabletId& tablet_id) {
  return {kRaftTabletGroupBindingEntryType,
          encode_tablet_group_binding_v1({tablet_id, tablet_group_id()}).value()};
}

TEST(DurableMetadataStateMachineTest, AppliesAndRebuildsCommittedMetadataGroup) {
  TemporaryDirectory directory;
  const RaftPersistentLogConfig log_config{.directory_path = directory.path().string()};
  const std::vector<RaftGroupConfiguration> groups{{group_id(), {1U}}};
  auto runtime = DurableMultiRaftRuntime::create_new(1U, log_config, groups);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  ASSERT_TRUE(runtime->execute_batch({{group_id(), StartElectionOperation{}}}).has_value());
  auto recovered = DurableMetadataStateMachine::recover(group_id(), *runtime);
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_FALSE(recovered->snapshot_cleanup_metrics().has_value());
  std::optional<DurableMetadataStateMachine> metadata{std::move(*recovered)};

  const auto table = id<schema::TableId>(1U);
  const auto schema_id = id<schema::SchemaId>(2U);
  const auto tablet = id<schema::TabletId>(3U);
  std::vector<DurableRaftRequest> requests;
  requests.emplace_back(group_id(), proposal(ClusterNodeMetadata{1U, "node-1"}));
  requests.emplace_back(
      group_id(), proposal(SchemaMetadata{table, schema_id, schema::SchemaVersion::initial()}));
  requests.emplace_back(group_id(), proposal(TabletPlacementMetadata{table, tablet, 1U, {1U}, 1U}));
  requests.emplace_back(group_id(), binding_proposal(tablet));
  requests.emplace_back(group_id(), proposal(RetentionMetadata{table, 1000, 100U}));
  ASSERT_TRUE(runtime->execute_batch(std::move(requests)).has_value());
  EXPECT_EQ(runtime->find_group(group_id())->commit_index(), 5U);
  EXPECT_EQ(metadata->state().applied_index(), 0U);
  EXPECT_EQ(metadata->prove_applied_quorum_sync(5U).error().code(),
            common::StatusCode::kUnavailable);

  auto report = metadata->apply_committed();
  ASSERT_TRUE(report.has_value()) << report.error().to_string();
  EXPECT_EQ(report->first_applied_index, 1U);
  EXPECT_EQ(report->last_applied_index, 5U);
  EXPECT_EQ(report->applied_commands, 5U);
  EXPECT_EQ(metadata->state().find_node(1U)->endpoint, "node-1");
  EXPECT_EQ(metadata->state().find_schema(schema_id)->table_id, table);
  EXPECT_EQ(metadata->state().find_tablet(tablet)->placement_epoch, 1U);
  EXPECT_EQ(metadata->state().find_tablet_group_binding(tablet)->group_id, tablet_group_id());
  EXPECT_EQ(metadata->state().find_retention(table)->retry_retention_positions, 100U);
  EXPECT_TRUE(metadata->prove_applied_quorum_sync(5U).has_value());

  metadata.reset();
  const std::uint64_t durable_sequence = runtime->durable_physical_sequence();
  ASSERT_TRUE(runtime->close().is_ok());
  auto reopened = DurableMultiRaftRuntime::open_existing(1U, log_config, {}, groups);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  auto rebuilt = DurableMetadataStateMachine::recover(group_id(), *reopened);
  ASSERT_TRUE(rebuilt.has_value()) << rebuilt.error().to_string();
  EXPECT_EQ(rebuilt->state().applied_index(), 5U);
  EXPECT_EQ(rebuilt->state().find_node(1U)->endpoint, "node-1");
  EXPECT_EQ(rebuilt->state().find_tablet(tablet)->leader_hint, 1U);
  EXPECT_EQ(rebuilt->state().find_tablet_group_binding(tablet)->group_id, tablet_group_id());
  EXPECT_EQ(reopened->durable_physical_sequence(), durable_sequence);
}

TEST(DurableMetadataStateMachineTest, AppliesReservedEntriesAsOrderedInternalNoops) {
  TemporaryDirectory directory;
  const RaftPersistentLogConfig log_config{.directory_path = directory.path().string()};
  const std::vector<RaftGroupConfiguration> groups{{group_id(), {1U, 2U}}};
  auto runtime = DurableMultiRaftRuntime::create_new(1U, log_config, groups);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  ASSERT_TRUE(runtime->execute_batch({{group_id(), StartElectionOperation{}}}).has_value());
  ASSERT_TRUE(
      runtime->execute_batch({{group_id(), ReceiveOperation{2U, RequestVoteResponse{1U, true}}}})
          .has_value());
  ASSERT_EQ(runtime->find_group(group_id())->role(), Role::kLeader);

  ASSERT_TRUE(
      runtime->execute_batch({{group_id(), BeginMembershipChangeOperation{{1U}}}}).has_value());
  ASSERT_TRUE(runtime
                  ->execute_batch(
                      {{group_id(), ReceiveOperation{2U, AppendEntriesResponse{1U, true, 1U,
                                                                               std::nullopt, 0U}}}})
                  .has_value());
  ASSERT_TRUE(
      runtime->execute_batch({{group_id(), FinalizeMembershipChangeOperation{}}}).has_value());
  ASSERT_TRUE(runtime
                  ->execute_batch(
                      {{group_id(), ReceiveOperation{2U, AppendEntriesResponse{1U, true, 2U,
                                                                               std::nullopt, 0U}}}})
                  .has_value());
  ASSERT_EQ(runtime->find_group(group_id())->commit_index(), 2U);
  ASSERT_TRUE(runtime->execute_batch({{group_id(), StartElectionOperation{}}}).has_value());
  ASSERT_TRUE(runtime->execute_batch({{group_id(), CommitCurrentTermOperation{}}}).has_value());
  ASSERT_EQ(runtime->find_group(group_id())->commit_index(), 3U);

  auto metadata = DurableMetadataStateMachine::recover(group_id(), *runtime);
  ASSERT_TRUE(metadata.has_value()) << metadata.error().to_string();
  EXPECT_EQ(metadata->state().applied_index(), 3U);
  EXPECT_EQ(runtime->find_group(group_id())->applied_index(), 3U);

  ASSERT_TRUE(runtime->execute_batch({{group_id(), proposal(ClusterNodeMetadata{1U, "node-1"})}})
                  .has_value());
  auto report = metadata->apply_committed();
  ASSERT_TRUE(report.has_value()) << report.error().to_string();
  EXPECT_EQ(report->first_applied_index, 4U);
  EXPECT_EQ(report->last_applied_index, 4U);
  EXPECT_EQ(report->applied_commands, 1U);
  EXPECT_EQ(metadata->state().find_node(1U)->endpoint, "node-1");
}

TEST(DurableMetadataStateMachineTest, RejectsTabletBindingThatAliasesMetadataGroup) {
  TemporaryDirectory directory;
  const RaftPersistentLogConfig log_config{.directory_path = directory.path().string()};
  const std::vector<RaftGroupConfiguration> groups{{group_id(), {1U}}};
  auto runtime = DurableMultiRaftRuntime::create_new(1U, log_config, groups);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  ASSERT_TRUE(runtime->execute_batch({{group_id(), StartElectionOperation{}}}).has_value());
  const auto table = id<schema::TableId>(51U);
  const auto tablet = id<schema::TabletId>(52U);
  ASSERT_TRUE(
      runtime
          ->execute_batch(
              {{group_id(), proposal(TabletPlacementMetadata{table, tablet, 1U, {1U}, 1U})}})
          .has_value());
  ASSERT_TRUE(
      runtime
          ->execute_batch(
              {{group_id(),
                ProposeOperation{kRaftTabletGroupBindingEntryType,
                                 encode_tablet_group_binding_v1({tablet, group_id()}).value()}}})
          .has_value());
  auto metadata = DurableMetadataStateMachine::recover(group_id(), *runtime);
  ASSERT_FALSE(metadata.has_value());
  EXPECT_EQ(metadata.error().code(), common::StatusCode::kInvalidArgument);
}

TEST(DurableMetadataStateMachineTest, RebuildsCompleteCatalogDefinitionFromRetainedRaftLog) {
  TemporaryDirectory directory;
  const RaftPersistentLogConfig log_config{.directory_path = directory.path().string()};
  const std::vector<RaftGroupConfiguration> groups{{group_id(), {1U}}};
  auto runtime = DurableMultiRaftRuntime::create_new(1U, log_config, groups);
  ASSERT_TRUE(runtime.has_value());
  ASSERT_TRUE(runtime->execute_batch({{group_id(), StartElectionOperation{}}}).has_value());
  const CatalogTableDefinition definition = schema_definition();
  ASSERT_TRUE(runtime->execute_batch({{group_id(), schema_proposal(definition)}}).has_value());
  ASSERT_TRUE(
      runtime
          ->execute_batch({{group_id(), proposal(TablePolicyMetadata{definition.schema->table_id(),
                                                                     100, 1000, 500, 10, 100U})}})
          .has_value());
  auto recovered = DurableMetadataStateMachine::recover(group_id(), *runtime);
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  std::optional<DurableMetadataStateMachine> metadata{std::move(*recovered)};
  const auto* installed =
      metadata->state().find_active_table_definition(definition.schema->table_id());
  ASSERT_NE(installed, nullptr);
  EXPECT_TRUE(*installed == definition);
  ASSERT_NE(metadata->state().find_table_policy(definition.schema->table_id()), nullptr);
  EXPECT_EQ(metadata->state().find_table_policy(definition.schema->table_id())->retention_ns, 1000);

  metadata.reset();
  ASSERT_TRUE(runtime->close().is_ok());
  auto reopened = DurableMultiRaftRuntime::open_existing(1U, log_config, {}, groups);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  auto rebuilt = DurableMetadataStateMachine::recover(group_id(), *reopened);
  ASSERT_TRUE(rebuilt.has_value()) << rebuilt.error().to_string();
  installed = rebuilt->state().find_schema_definition(definition.schema->schema_id());
  ASSERT_NE(installed, nullptr);
  EXPECT_TRUE(*installed == definition);
  ASSERT_NE(rebuilt->state().find_table_policy(definition.schema->table_id()), nullptr);
  EXPECT_EQ(rebuilt->state().find_table_policy(definition.schema->table_id())->allowed_lateness_ns,
            10);
}

TEST(DurableMetadataStateMachineTest, ProjectsAnOwningDeterministicRecoveryCatalog) {
  TemporaryDirectory directory;
  const RaftPersistentLogConfig log_config{.directory_path = directory.path().string()};
  const std::vector<RaftGroupConfiguration> groups{{group_id(), {1U}}};
  auto runtime = DurableMultiRaftRuntime::create_new(1U, log_config, groups);
  ASSERT_TRUE(runtime.has_value());
  ASSERT_TRUE(runtime->execute_batch({{group_id(), StartElectionOperation{}}}).has_value());
  const CatalogTableDefinition definition = schema_definition();
  const auto tablet = id<schema::TabletId>(31U);
  ASSERT_TRUE(runtime->execute_batch({{group_id(), proposal(ClusterNodeMetadata{1U, "node-1"})}})
                  .has_value());
  ASSERT_TRUE(runtime->execute_batch({{group_id(), schema_proposal(definition)}}).has_value());
  ASSERT_TRUE(
      runtime
          ->execute_batch({{group_id(), proposal(TablePolicyMetadata{definition.schema->table_id(),
                                                                     100, 1000, 500, 10, 100U})}})
          .has_value());
  ASSERT_TRUE(
      runtime
          ->execute_batch({{group_id(), proposal(TabletPlacementMetadata{
                                            definition.schema->table_id(), tablet, 1U, {1U}, 1U})}})
          .has_value());
  ASSERT_TRUE(runtime->execute_batch({{group_id(), binding_proposal(tablet)}}).has_value());
  auto metadata = DurableMetadataStateMachine::recover(group_id(), *runtime);
  ASSERT_TRUE(metadata.has_value()) << metadata.error().to_string();
  std::optional<DurableMetadataStateMachine> owner{std::move(*metadata)};
  auto projected = owner->state().catalog_snapshot();
  ASSERT_TRUE(projected.has_value()) << projected.error().to_string();
  EXPECT_EQ(projected->applied_index, 5U);
  ASSERT_EQ(projected->cluster_nodes.size(), 1U);
  EXPECT_EQ(projected->cluster_nodes.front(), (ClusterNodeMetadata{1U, "node-1"}));
  ASSERT_EQ(projected->schema_definitions.size(), 1U);
  EXPECT_TRUE(projected->schema_definitions.front() == definition);
  ASSERT_EQ(projected->active_schemas.size(), 1U);
  EXPECT_EQ(projected->active_schemas.front(),
            (ActiveSchemaMetadata{definition.schema->table_id(), definition.schema->schema_id()}));
  ASSERT_EQ(projected->tablet_placements.size(), 1U);
  EXPECT_EQ(projected->tablet_placements.front().tablet_id, tablet);
  ASSERT_EQ(projected->tablet_group_bindings.size(), 1U);
  EXPECT_EQ(projected->tablet_group_bindings.front(),
            (TabletGroupBindingMetadata{tablet, tablet_group_id()}));
  ASSERT_EQ(projected->table_policies.size(), 1U);
  EXPECT_EQ(projected->table_policies.front().allowed_lateness_ns, 10);

  std::optional<MetadataCatalogSnapshot> retained{std::move(*projected)};
  owner.reset();
  ASSERT_TRUE(runtime->close().is_ok());
  EXPECT_TRUE(retained->schema_definitions.front() == definition);
}

TEST(DurableMetadataStateMachineTest, CompactsBoundaryMembershipBeforeRetainedReconfiguration) {
  TemporaryDirectory directory;
  const std::filesystem::path log_directory = directory.path() / "raft";
  const std::filesystem::path snapshot_directory = directory.path() / "metadata-snapshots";
  ASSERT_TRUE(std::filesystem::create_directories(log_directory));
  ASSERT_TRUE(std::filesystem::create_directories(snapshot_directory));
  const RaftPersistentLogConfig log_config{.directory_path = log_directory.string()};
  const std::vector<RaftGroupConfiguration> groups{{group_id(), {1U, 2U}}};
  auto runtime = DurableMultiRaftRuntime::create_new(1U, log_config, groups);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  ASSERT_TRUE(runtime->execute_batch({{group_id(), StartElectionOperation{}}}).has_value());
  ASSERT_TRUE(
      runtime->execute_batch({{group_id(), ReceiveOperation{2U, RequestVoteResponse{1U, true}}}})
          .has_value());
  auto snapshot_storage =
      MetadataSnapshotStorage::create({.directory_path = snapshot_directory.string(),
                                       .group_id = group_id(),
                                       .codec_limits = {},
                                       .file_permissions = 0600U});
  ASSERT_TRUE(snapshot_storage.has_value()) << snapshot_storage.error().to_string();
  auto metadata =
      DurableMetadataStateMachine::recover(group_id(), *runtime, std::move(*snapshot_storage));
  ASSERT_TRUE(metadata.has_value()) << metadata.error().to_string();

  ASSERT_TRUE(runtime->execute_batch({{group_id(), proposal(ClusterNodeMetadata{1U, "node-1"})}})
                  .has_value());
  ASSERT_TRUE(runtime
                  ->execute_batch(
                      {{group_id(),
                        ReceiveOperation{2U, AppendEntriesResponse{.term = 1U,
                                                                   .success = true,
                                                                   .match_index = 1U,
                                                                   .conflict_term = std::nullopt,
                                                                   .conflict_index = 0U}}}})
                  .has_value());
  ASSERT_TRUE(metadata->apply_committed().has_value());
  ASSERT_EQ(runtime->find_group(group_id())->applied_index(), 1U);

  ASSERT_TRUE(
      runtime->execute_batch({{group_id(), BeginMembershipChangeOperation{{1U}}}}).has_value());
  ASSERT_TRUE(runtime
                  ->execute_batch(
                      {{group_id(),
                        ReceiveOperation{2U, AppendEntriesResponse{.term = 1U,
                                                                   .success = true,
                                                                   .match_index = 2U,
                                                                   .conflict_term = std::nullopt,
                                                                   .conflict_index = 0U}}}})
                  .has_value());
  ASSERT_TRUE(
      runtime->execute_batch({{group_id(), FinalizeMembershipChangeOperation{}}}).has_value());
  ASSERT_TRUE(runtime
                  ->execute_batch(
                      {{group_id(),
                        ReceiveOperation{2U, AppendEntriesResponse{.term = 1U,
                                                                   .success = true,
                                                                   .match_index = 3U,
                                                                   .conflict_term = std::nullopt,
                                                                   .conflict_index = 0U}}}})
                  .has_value());
  auto membership = metadata->apply_committed();
  ASSERT_TRUE(membership.has_value()) << membership.error().to_string();
  EXPECT_EQ(membership->first_applied_index, 2U);
  EXPECT_EQ(membership->last_applied_index, 3U);
  EXPECT_EQ(membership->applied_commands, 0U);

  auto compacted = metadata->compact_applied_prefix(1U);

  ASSERT_TRUE(compacted.has_value()) << compacted.error().to_string();
  EXPECT_EQ(compacted->snapshot.last_included_index, 1U);
  EXPECT_EQ(compacted->snapshot.configuration_index, 0U);
  EXPECT_EQ(compacted->snapshot.voters, (std::vector<NodeId>{1U, 2U}));
  EXPECT_EQ(compacted->application_entries, 1U);
  const RaftNode* node = runtime->find_group(group_id());
  ASSERT_NE(node, nullptr);
  EXPECT_EQ(node->persistent_state().snapshot, compacted->snapshot);
  ASSERT_EQ(node->persistent_state().log.size(), 2U);
  EXPECT_EQ(node->persistent_state().log.front().index, 2U);
  EXPECT_EQ(std::vector<NodeId>(node->voters().begin(), node->voters().end()),
            std::vector<NodeId>{1U});
}

TEST(DurableMetadataStateMachineTest, InstallsCompactsAndReopensSnapshotPlusCommittedSuffix) {
  TemporaryDirectory directory;
  const std::filesystem::path log_directory = directory.path() / "raft";
  const std::filesystem::path snapshot_directory = directory.path() / "metadata-snapshots";
  ASSERT_TRUE(std::filesystem::create_directories(log_directory));
  ASSERT_TRUE(std::filesystem::create_directories(snapshot_directory));
  const RaftPersistentLogConfig log_config{.directory_path = log_directory.string()};
  const std::vector<RaftGroupConfiguration> groups{{group_id(), {1U}}};
  auto runtime = DurableMultiRaftRuntime::create_new(1U, log_config, groups);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  ASSERT_TRUE(runtime->execute_batch({{group_id(), StartElectionOperation{}}}).has_value());
  auto snapshot_storage = MetadataSnapshotStorage::create(
      {.directory_path = snapshot_directory.string(), .group_id = group_id()});
  ASSERT_TRUE(snapshot_storage.has_value()) << snapshot_storage.error().to_string();
  auto recovered =
      DurableMetadataStateMachine::recover(group_id(), *runtime, std::move(*snapshot_storage));
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  std::optional<DurableMetadataStateMachine> metadata{std::move(*recovered)};
  auto initial_reclamation = metadata->reclaim_obsolete_snapshots();
  ASSERT_TRUE(initial_reclamation.has_value()) << initial_reclamation.error().to_string();
  EXPECT_EQ(initial_reclamation->authoritative_index, std::nullopt);
  EXPECT_EQ(initial_reclamation->reclaimed_files, 0U);
  EXPECT_EQ(metadata->snapshot_cleanup_metrics(),
            (std::optional<MetadataSnapshotCleanupMetrics>{
                MetadataSnapshotCleanupMetrics{.reclamation_attempts = 1U}}));

  const CatalogTableDefinition definition = schema_definition();
  const auto tablet = id<schema::TabletId>(41U);
  ASSERT_TRUE(runtime->execute_batch({{group_id(), schema_proposal(definition)}}).has_value());
  ASSERT_TRUE(
      runtime
          ->execute_batch({{group_id(), proposal(TablePolicyMetadata{definition.schema->table_id(),
                                                                     100, 1000, 500, 10, 100U})}})
          .has_value());
  ASSERT_TRUE(
      runtime
          ->execute_batch({{group_id(), proposal(TabletPlacementMetadata{
                                            definition.schema->table_id(), tablet, 1U, {1U}, 1U})}})
          .has_value());
  ASSERT_TRUE(runtime->execute_batch({{group_id(), binding_proposal(tablet)}}).has_value());
  auto applied = metadata->apply_committed();
  ASSERT_TRUE(applied.has_value()) << applied.error().to_string();
  ASSERT_EQ(applied->last_applied_index, 4U);

  auto first_compaction = metadata->compact_applied_prefix(4U);
  ASSERT_TRUE(first_compaction.has_value()) << first_compaction.error().to_string();
  EXPECT_EQ(first_compaction->snapshot.last_included_index, 4U);
  EXPECT_EQ(first_compaction->snapshot.configuration_index, 0U);
  EXPECT_EQ(first_compaction->snapshot.voters, std::vector<NodeId>{1U});

  ASSERT_TRUE(runtime->execute_batch({{group_id(), StartElectionOperation{}}}).has_value());
  ASSERT_TRUE(runtime->execute_batch({{group_id(), CommitCurrentTermOperation{}}}).has_value());
  applied = metadata->apply_committed();
  ASSERT_TRUE(applied.has_value()) << applied.error().to_string();
  ASSERT_EQ(applied->last_applied_index, 5U);
  ASSERT_EQ(applied->applied_commands, 0U);

  auto compacted = metadata->compact_applied_prefix(5U);
  ASSERT_TRUE(compacted.has_value()) << compacted.error().to_string();
  EXPECT_EQ(compacted->snapshot.last_included_index, 5U);
  EXPECT_EQ(compacted->snapshot.manifest_generation, 5U);
  EXPECT_EQ(compacted->snapshot.configuration_index, 0U);
  EXPECT_EQ(compacted->snapshot.voters, std::vector<NodeId>{1U});
  EXPECT_EQ(compacted->application_entries, 4U);
  EXPECT_FALSE(compacted->application_snapshot_already_present);
  EXPECT_TRUE(runtime->find_group(group_id())->persistent_state().log.empty());
  auto reclaimed = metadata->reclaim_obsolete_snapshots();
  ASSERT_TRUE(reclaimed.has_value()) << reclaimed.error().to_string();
  EXPECT_EQ(reclaimed->authoritative_index, 5U);
  EXPECT_EQ(reclaimed->reclaimed_files, 1U);
  EXPECT_EQ(
      metadata->snapshot_cleanup_metrics(),
      (std::optional<MetadataSnapshotCleanupMetrics>{MetadataSnapshotCleanupMetrics{
          .reclamation_attempts = 2U, .reclaimed_files = 1U, .reclamation_directory_syncs = 1U}}));
  EXPECT_FALSE(
      std::filesystem::exists(snapshot_directory / "metadata-snapshot-00000000000000000004.rmas"));
  EXPECT_TRUE(
      std::filesystem::exists(snapshot_directory / "metadata-snapshot-00000000000000000005.rmas"));

  ASSERT_TRUE(runtime->execute_batch({{group_id(), proposal(ClusterNodeMetadata{1U, "node-1"})}})
                  .has_value());
  applied = metadata->apply_committed();
  ASSERT_TRUE(applied.has_value()) << applied.error().to_string();
  EXPECT_EQ(applied->first_applied_index, 6U);
  EXPECT_EQ(metadata->state().find_node(1U)->endpoint, "node-1");

  metadata.reset();
  ASSERT_TRUE(runtime->close().is_ok());
  auto reopened = DurableMultiRaftRuntime::open_existing(1U, log_config, {}, groups);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  auto missing_snapshot = DurableMetadataStateMachine::recover(group_id(), *reopened);
  ASSERT_FALSE(missing_snapshot.has_value());
  EXPECT_EQ(missing_snapshot.error().code(), common::StatusCode::kNotSupported);
  auto reopened_snapshots = MetadataSnapshotStorage::open_existing(
      {.directory_path = snapshot_directory.string(), .group_id = group_id()});
  ASSERT_TRUE(reopened_snapshots.has_value()) << reopened_snapshots.error().to_string();
  auto rebuilt =
      DurableMetadataStateMachine::recover(group_id(), *reopened, std::move(*reopened_snapshots));
  ASSERT_TRUE(rebuilt.has_value()) << rebuilt.error().to_string();
  EXPECT_EQ(rebuilt->state().applied_index(), 6U);
  const auto* installed =
      rebuilt->state().find_active_table_definition(definition.schema->table_id());
  ASSERT_NE(installed, nullptr);
  EXPECT_TRUE(*installed == definition);
  ASSERT_NE(rebuilt->state().find_table_policy(definition.schema->table_id()), nullptr);
  EXPECT_EQ(rebuilt->state().find_table_policy(definition.schema->table_id())->retention_ns, 1000);
  ASSERT_NE(rebuilt->state().find_tablet_group_binding(tablet), nullptr);
  EXPECT_EQ(rebuilt->state().find_tablet_group_binding(tablet)->group_id, tablet_group_id());
  ASSERT_NE(rebuilt->state().find_node(1U), nullptr);
  EXPECT_EQ(rebuilt->state().find_node(1U)->endpoint, "node-1");
}

} // namespace
} // namespace chronos::raft
