#include "chronos/raft/async_metadata_application.hpp"
#include "chronos/raft/metadata_codec.hpp"
#include "chronos/raft/schema_definition_codec.hpp"
#include "chronos/raft/tablet_group_binding_codec.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
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
        (std::filesystem::temp_directory_path() / "chronos-async-metadata-XXXXXX").string();
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

[[nodiscard]] GroupId group_id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(static_cast<std::byte>(seed));
  return GroupId{bytes};
}

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return Identifier::from_bytes(bytes).value();
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
  auto schema = schema::TableSchema::create(table, schema_id, schema::SchemaVersion::initial(),
                                            std::nullopt, std::move(columns),
                                            {.event_time_column = timestamp,
                                             .physical_ordering_key = {timestamp},
                                             .partition_columns = {timestamp},
                                             .shard_key = {timestamp},
                                             .deduplication_key = {timestamp}});
  return {.name = "events",
          .quoted = false,
          .schema = std::make_shared<const schema::TableSchema>(std::move(*schema))};
}

[[nodiscard]] ProposeOperation schema_proposal(const CatalogTableDefinition& definition) {
  return {kRaftSchemaDefinitionEntryType, encode_schema_definition_v1(definition).value()};
}

[[nodiscard]] ProposeOperation metadata_proposal(MetadataCommand command) {
  return {kRaftMetadataCommandEntryType, encode_metadata_command_v1(std::move(command)).value()};
}

[[nodiscard]] ProposeOperation binding_proposal(const schema::TabletId& tablet_id,
                                                const GroupId& group_id) {
  return {kRaftTabletGroupBindingEntryType,
          encode_tablet_group_binding_v1({tablet_id, group_id}).value()};
}

[[nodiscard]] common::Result<std::shared_ptr<AsyncRaftMetadataApplication>>
application(const GroupId& metadata_group,
            std::optional<MetadataSnapshotStorage> snapshot_storage = std::nullopt) {
  return AsyncRaftMetadataApplication::create(
      {.group_id = metadata_group, .snapshot_storage = std::move(snapshot_storage)});
}

TEST(AsyncRaftMetadataApplicationTest,
     PublishesAppliedCatalogBeforeCompletionAndIgnoresUntouchedGroups) {
  TemporaryDirectory directory;
  const GroupId metadata_group = group_id(0x51U);
  const GroupId data_group = group_id(0x52U);
  auto extension = application(metadata_group);
  ASSERT_TRUE(extension.has_value()) << extension.error().to_string();
  auto runtime = AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()},
      {{metadata_group, {1U}}, {data_group, {1U}}}, {}, *extension);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  EXPECT_TRUE((*extension)->initialized());
  EXPECT_EQ((*extension)->group_id(), metadata_group);
  EXPECT_TRUE(runtime->owns_worker_extension(**extension));

  auto initial = (*extension)->catalog_snapshot();
  ASSERT_TRUE(initial.has_value()) << initial.error().to_string();
  EXPECT_EQ((*initial)->applied_index, 0U);

  auto unrelated = runtime->try_submit({{data_group, StartElectionOperation{}}});
  ASSERT_TRUE(unrelated.has_value()) << unrelated.error().to_string();
  ASSERT_TRUE(unrelated->wait().has_value());
  auto unchanged = (*extension)->catalog_snapshot();
  ASSERT_TRUE(unchanged.has_value()) << unchanged.error().to_string();
  EXPECT_EQ(unchanged->get(), initial->get());

  auto election = runtime->try_submit({{metadata_group, StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value()) << election.error().to_string();
  ASSERT_TRUE(election->wait().has_value());
  const CatalogTableDefinition definition = schema_definition();
  const auto tablet = id<schema::TabletId>(23U);
  auto proposals = runtime->try_submit(
      {{metadata_group, metadata_proposal(ClusterNodeMetadata{1U, "node-1"})},
       {metadata_group, schema_proposal(definition)},
       {metadata_group, metadata_proposal(TablePolicyMetadata{definition.schema->table_id(), 100,
                                                              1000, 500, 10, 100U})},
       {metadata_group, metadata_proposal(TabletPlacementMetadata{
                            definition.schema->table_id(), tablet, 1U, {1U}, 1U})},
       {metadata_group, binding_proposal(tablet, data_group)}});
  ASSERT_TRUE(proposals.has_value()) << proposals.error().to_string();
  const auto result = proposals->wait();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();

  auto published = (*extension)->catalog_snapshot();
  ASSERT_TRUE(published.has_value()) << published.error().to_string();
  EXPECT_NE(published->get(), initial->get());
  EXPECT_EQ((*published)->applied_index, 5U);
  ASSERT_EQ((*published)->cluster_nodes.size(), 1U);
  EXPECT_EQ((*published)->cluster_nodes.front(), (ClusterNodeMetadata{1U, "node-1"}));
  ASSERT_EQ((*published)->schema_definitions.size(), 1U);
  EXPECT_TRUE((*published)->schema_definitions.front() == definition);
  ASSERT_EQ((*published)->tablet_placements.size(), 1U);
  EXPECT_EQ((*published)->tablet_placements.front().tablet_id, tablet);
  ASSERT_EQ((*published)->tablet_group_bindings.size(), 1U);
  EXPECT_EQ((*published)->tablet_group_bindings.front(),
            (TabletGroupBindingMetadata{tablet, data_group}));
  ASSERT_EQ((*published)->table_policies.size(), 1U);
  EXPECT_EQ((*published)->table_policies.front().retention_ns, 1000);

  std::shared_ptr<const MetadataCatalogSnapshot> retained = *published;
  EXPECT_TRUE(runtime->shutdown().is_ok());
  EXPECT_FALSE((*extension)->initialized());
  EXPECT_EQ((*extension)->catalog_snapshot().error().code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(retained->applied_index, 5U);
  EXPECT_TRUE(retained->schema_definitions.front() == definition);
}

TEST(AsyncRaftMetadataApplicationTest, RebuildsCommittedCatalogBeforeReopenAdmission) {
  TemporaryDirectory directory;
  const GroupId metadata_group = group_id(0x53U);
  const RaftPersistentLogConfig log_config{.directory_path = directory.path().string()};
  const std::vector<RaftGroupConfiguration> groups{{metadata_group, {1U}}};
  const CatalogTableDefinition definition = schema_definition();
  {
    auto extension = application(metadata_group);
    ASSERT_TRUE(extension.has_value()) << extension.error().to_string();
    auto runtime = AsyncDurableMultiRaftRuntime::create_new(1U, log_config, groups, {}, *extension);
    ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
    auto election = runtime->try_submit({{metadata_group, StartElectionOperation{}}});
    ASSERT_TRUE(election.has_value());
    ASSERT_TRUE(election->wait().has_value());
    auto proposal = runtime->try_submit({{metadata_group, schema_proposal(definition)}});
    ASSERT_TRUE(proposal.has_value());
    ASSERT_TRUE(proposal->wait().has_value());
    ASSERT_TRUE(runtime->shutdown().is_ok());
  }

  auto rebuilt_extension = application(metadata_group);
  ASSERT_TRUE(rebuilt_extension.has_value()) << rebuilt_extension.error().to_string();
  auto reopened = AsyncDurableMultiRaftRuntime::open_existing(1U, log_config, {}, groups, {},
                                                              *rebuilt_extension);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_TRUE(reopened->is_accepting());
  auto rebuilt = (*rebuilt_extension)->catalog_snapshot();
  ASSERT_TRUE(rebuilt.has_value()) << rebuilt.error().to_string();
  EXPECT_EQ((*rebuilt)->applied_index, 1U);
  ASSERT_EQ((*rebuilt)->schema_definitions.size(), 1U);
  EXPECT_TRUE((*rebuilt)->schema_definitions.front() == definition);
  EXPECT_TRUE(reopened->shutdown().is_ok());
}

TEST(AsyncRaftMetadataApplicationTest, RecoversTheExactInstalledApplicationSnapshot) {
  TemporaryDirectory directory;
  const GroupId metadata_group = group_id(0x54U);
  const std::filesystem::path log_directory = directory.path() / "raft";
  const std::filesystem::path snapshot_directory = directory.path() / "metadata-snapshots";
  ASSERT_TRUE(std::filesystem::create_directories(log_directory));
  ASSERT_TRUE(std::filesystem::create_directories(snapshot_directory));
  const RaftPersistentLogConfig log_config{.directory_path = log_directory.string()};
  const std::vector<RaftGroupConfiguration> groups{{metadata_group, {1U}}};
  const CatalogTableDefinition definition = schema_definition();
  {
    auto runtime = DurableMultiRaftRuntime::create_new(1U, log_config, groups);
    ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
    ASSERT_TRUE(runtime->execute_batch({{metadata_group, StartElectionOperation{}}}).has_value());
    ASSERT_TRUE(
        runtime->execute_batch({{metadata_group, schema_proposal(definition)}}).has_value());
    auto storage = MetadataSnapshotStorage::create(
        {.directory_path = snapshot_directory.string(), .group_id = metadata_group});
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    auto recovered =
        DurableMetadataStateMachine::recover(metadata_group, *runtime, std::move(*storage));
    ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
    std::optional<DurableMetadataStateMachine> metadata{std::move(*recovered)};
    auto compacted = metadata->compact_applied_prefix(1U);
    ASSERT_TRUE(compacted.has_value()) << compacted.error().to_string();
    metadata.reset();
    ASSERT_TRUE(runtime->close().is_ok());
  }

  auto storage = MetadataSnapshotStorage::open_existing(
      {.directory_path = snapshot_directory.string(), .group_id = metadata_group});
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  auto extension = application(metadata_group, std::move(*storage));
  ASSERT_TRUE(extension.has_value()) << extension.error().to_string();
  auto reopened =
      AsyncDurableMultiRaftRuntime::open_existing(1U, log_config, {}, groups, {}, *extension);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  auto snapshot = (*extension)->catalog_snapshot();
  ASSERT_TRUE(snapshot.has_value()) << snapshot.error().to_string();
  EXPECT_EQ((*snapshot)->applied_index, 1U);
  ASSERT_EQ((*snapshot)->schema_definitions.size(), 1U);
  EXPECT_TRUE((*snapshot)->schema_definitions.front() == definition);
  EXPECT_TRUE(reopened->shutdown().is_ok());
}

TEST(AsyncRaftMetadataApplicationTest, FailsClosedOnCorruptCommittedMetadataCommand) {
  TemporaryDirectory directory;
  const GroupId metadata_group = group_id(0x55U);
  auto extension = application(metadata_group);
  ASSERT_TRUE(extension.has_value()) << extension.error().to_string();
  auto runtime = AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{metadata_group, {1U}}}, {}, *extension);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  auto election = runtime->try_submit({{metadata_group, StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value());
  ASSERT_TRUE(election->wait().has_value());
  auto corrupt = runtime->try_submit(
      {{metadata_group, ProposeOperation{kRaftMetadataCommandEntryType, {std::byte{0xFFU}}}}});
  ASSERT_TRUE(corrupt.has_value());
  const auto result = corrupt->wait();
  ASSERT_FALSE(result.has_value());
  EXPECT_TRUE((*extension)->failed());
  EXPECT_FALSE(runtime->is_accepting());
  EXPECT_EQ((*extension)->catalog_snapshot().error(), (*extension)->failure_status());
  EXPECT_EQ(runtime->shutdown(), (*extension)->failure_status());
}

TEST(AsyncRaftMetadataApplicationTest, RejectsNilGroupBeforeStartingAWorker) {
  auto invalid = AsyncRaftMetadataApplication::create({});
  ASSERT_FALSE(invalid.has_value());
  EXPECT_EQ(invalid.error().code(), common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::raft
