#include "chronos/common/status.hpp"
#include "chronos/raft/metadata_runtime.hpp"
#include "chronos/raft/schema_definition_codec.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"

#include <array>
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
        (std::filesystem::temp_directory_path() / "chronos-metadata-policy-XXXXXX").string();
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

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] GroupId group_id() {
  common::Uuid::Bytes bytes{};
  bytes.fill(std::byte{0x57U});
  return GroupId{bytes};
}

[[nodiscard]] CatalogTableDefinition schema_definition() {
  const auto table = id<schema::TableId>(60U);
  const auto schema_id = id<schema::SchemaId>(61U);
  const auto timestamp = id<schema::ColumnId>(62U);
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
  return {.name = "policy_events",
          .quoted = false,
          .schema = std::make_shared<const schema::TableSchema>(std::move(*value))};
}

[[nodiscard]] TablePolicyMetadata first_policy(const schema::TableId& table_id) {
  return {table_id, 100, 1000, 500, 10, 100U};
}

[[nodiscard]] TablePolicyMetadata second_policy(const schema::TableId& table_id) {
  return {table_id, 200, 2000, 800, 20, 200U};
}

enum class PolicyStep : std::uint8_t {
  kSchema,
  kLegacyOld,
  kCompleteFirst,
  kCompleteSecond,
  kLegacyMatchingFirst,
  kLegacyMatchingSecond,
  kLegacyDivergentHistory,
  kLegacyDivergentRetry,
};

enum class PolicyTransition : std::uint8_t {
  kLegacyBeforeSchemaThenComplete,
  kLegacyThenComplete,
  kCompleteUpdate,
  kCompleteThenMatchingLegacy,
  kCompleteUpdateThenMatchingLegacy,
  kCompleteThenDivergentHistory,
  kCompleteThenDivergentRetry,
};

struct PolicyTransitionCase {
  const char* name;
  PolicyTransition transition;
  LogIndex snapshot_index;
  bool accepted;
};

[[nodiscard]] std::vector<PolicyStep> steps(const PolicyTransition transition) {
  switch (transition) {
  case PolicyTransition::kLegacyBeforeSchemaThenComplete:
    return {PolicyStep::kLegacyOld, PolicyStep::kSchema, PolicyStep::kCompleteFirst};
  case PolicyTransition::kLegacyThenComplete:
    return {PolicyStep::kSchema, PolicyStep::kLegacyOld, PolicyStep::kCompleteFirst};
  case PolicyTransition::kCompleteUpdate:
    return {PolicyStep::kSchema, PolicyStep::kCompleteFirst, PolicyStep::kCompleteSecond};
  case PolicyTransition::kCompleteThenMatchingLegacy:
    return {PolicyStep::kSchema, PolicyStep::kCompleteFirst, PolicyStep::kLegacyMatchingFirst};
  case PolicyTransition::kCompleteUpdateThenMatchingLegacy:
    return {PolicyStep::kSchema, PolicyStep::kCompleteFirst, PolicyStep::kCompleteSecond,
            PolicyStep::kLegacyMatchingSecond};
  case PolicyTransition::kCompleteThenDivergentHistory:
    return {PolicyStep::kSchema, PolicyStep::kCompleteFirst, PolicyStep::kLegacyDivergentHistory};
  case PolicyTransition::kCompleteThenDivergentRetry:
    return {PolicyStep::kSchema, PolicyStep::kCompleteFirst, PolicyStep::kLegacyDivergentRetry};
  }
  return {};
}

[[nodiscard]] ProposeOperation proposal_for(const PolicyStep step,
                                            const CatalogTableDefinition& definition) {
  if (step == PolicyStep::kSchema) {
    return {kRaftSchemaDefinitionEntryType, encode_schema_definition_v1(definition).value()};
  }
  const schema::TableId table_id = definition.schema->table_id();
  MetadataCommand command;
  switch (step) {
  case PolicyStep::kLegacyOld:
    command = RetentionMetadata{table_id, 300, 30U};
    break;
  case PolicyStep::kCompleteFirst:
    command = first_policy(table_id);
    break;
  case PolicyStep::kCompleteSecond:
    command = second_policy(table_id);
    break;
  case PolicyStep::kLegacyMatchingFirst:
    command = RetentionMetadata{table_id, 500, 100U};
    break;
  case PolicyStep::kLegacyMatchingSecond:
    command = RetentionMetadata{table_id, 800, 200U};
    break;
  case PolicyStep::kLegacyDivergentHistory:
    command = RetentionMetadata{table_id, 501, 100U};
    break;
  case PolicyStep::kLegacyDivergentRetry:
    command = RetentionMetadata{table_id, 500, 101U};
    break;
  case PolicyStep::kSchema:
    break;
  }
  return {kRaftMetadataCommandEntryType, encode_metadata_command_v1(std::move(command)).value()};
}

[[nodiscard]] TablePolicyMetadata expected_policy(const PolicyTransition transition,
                                                  const schema::TableId& table_id) {
  if (transition == PolicyTransition::kCompleteUpdate ||
      transition == PolicyTransition::kCompleteUpdateThenMatchingLegacy) {
    return second_policy(table_id);
  }
  return first_policy(table_id);
}

class MetadataPolicyTransitionTest : public testing::TestWithParam<PolicyTransitionCase> {};

TEST_P(MetadataPolicyTransitionTest, PreservesExactAuthorityAcrossSnapshotAndCommittedSuffix) {
  const PolicyTransitionCase parameter = GetParam();
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
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

  const CatalogTableDefinition definition = schema_definition();
  const schema::TableId table_id = definition.schema->table_id();
  const std::vector<PolicyStep> commands = steps(parameter.transition);
  ASSERT_LT(parameter.snapshot_index, commands.size() + 1U);
  bool rejected = false;
  for (std::size_t ordinal = 0U; ordinal < commands.size(); ++ordinal) {
    auto proposed =
        runtime->execute_batch({{group_id(), proposal_for(commands[ordinal], definition)}});
    ASSERT_TRUE(proposed.has_value()) << proposed.error().to_string();
    ASSERT_EQ(proposed->size(), 1U);
    ASSERT_TRUE(proposed->front().status.is_ok()) << proposed->front().status.to_string();

    auto applied = metadata->apply_committed();
    const LogIndex index = ordinal + 1U;
    const bool rejects_this_command = !parameter.accepted && ordinal + 1U == commands.size();
    if (rejects_this_command) {
      ASSERT_FALSE(applied.has_value());
      EXPECT_EQ(applied.error().code(), common::StatusCode::kInvalidArgument);
      EXPECT_TRUE(metadata->failed());
      EXPECT_EQ(runtime->find_group(group_id())->applied_index(), parameter.snapshot_index);
      rejected = true;
      break;
    }
    ASSERT_TRUE(applied.has_value()) << applied.error().to_string();
    EXPECT_EQ(applied->first_applied_index, index);
    EXPECT_EQ(applied->last_applied_index, index);
    EXPECT_EQ(applied->applied_commands, 1U);
    if (index == parameter.snapshot_index) {
      auto compacted = metadata->compact_applied_prefix(index);
      ASSERT_TRUE(compacted.has_value()) << compacted.error().to_string();
      EXPECT_EQ(compacted->snapshot.last_included_index, index);
      EXPECT_EQ(compacted->application_entries, index);
    }
  }
  EXPECT_EQ(rejected, !parameter.accepted);

  const TablePolicyMetadata expected = expected_policy(parameter.transition, table_id);
  const RetentionMetadata expected_retention{table_id, expected.system_history_ns,
                                             expected.retry_retention_positions};
  const TablePolicyMetadata* live_policy = metadata->state().find_table_policy(table_id);
  ASSERT_NE(live_policy, nullptr);
  EXPECT_EQ(*live_policy, expected);
  const RetentionMetadata* live_retention = metadata->state().find_retention(table_id);
  ASSERT_NE(live_retention, nullptr);
  EXPECT_EQ(*live_retention, expected_retention);

  metadata.reset();
  ASSERT_TRUE(runtime->close().is_ok());
  auto reopened = DurableMultiRaftRuntime::open_existing(1U, log_config, {}, groups);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  auto reopened_snapshots = MetadataSnapshotStorage::open_existing(
      {.directory_path = snapshot_directory.string(), .group_id = group_id()});
  ASSERT_TRUE(reopened_snapshots.has_value()) << reopened_snapshots.error().to_string();
  auto rebuilt =
      DurableMetadataStateMachine::recover(group_id(), *reopened, std::move(*reopened_snapshots));
  if (!parameter.accepted) {
    ASSERT_FALSE(rebuilt.has_value());
    EXPECT_EQ(rebuilt.error().code(), common::StatusCode::kInvalidArgument);
    return;
  }

  ASSERT_TRUE(rebuilt.has_value()) << rebuilt.error().to_string();
  EXPECT_EQ(rebuilt->state().applied_index(), commands.size());
  const TablePolicyMetadata* rebuilt_policy = rebuilt->state().find_table_policy(table_id);
  ASSERT_NE(rebuilt_policy, nullptr);
  EXPECT_EQ(*rebuilt_policy, expected);
  const RetentionMetadata* rebuilt_retention = rebuilt->state().find_retention(table_id);
  ASSERT_NE(rebuilt_retention, nullptr);
  EXPECT_EQ(*rebuilt_retention, expected_retention);
  auto catalog = rebuilt->state().catalog_snapshot();
  ASSERT_TRUE(catalog.has_value()) << catalog.error().to_string();
  ASSERT_EQ(catalog->table_policies.size(), 1U);
  EXPECT_EQ(catalog->table_policies.front(), expected);
}

constexpr std::array<PolicyTransitionCase, 12U> kPolicyTransitionCases{{
    {"LegacyBeforeSchemaCutBeforeSchema", PolicyTransition::kLegacyBeforeSchemaThenComplete, 1U,
     true},
    {"LegacyBeforeSchemaCutBeforeComplete", PolicyTransition::kLegacyBeforeSchemaThenComplete, 2U,
     true},
    {"LegacyBeforeSchemaCutAfterComplete", PolicyTransition::kLegacyBeforeSchemaThenComplete, 3U,
     true},
    {"LegacyThenCompleteCutAfterLegacy", PolicyTransition::kLegacyThenComplete, 2U, true},
    {"LegacyThenCompleteCutAfterComplete", PolicyTransition::kLegacyThenComplete, 3U, true},
    {"CompleteUpdateCutBeforeUpdate", PolicyTransition::kCompleteUpdate, 2U, true},
    {"CompleteUpdateCutAfterUpdate", PolicyTransition::kCompleteUpdate, 3U, true},
    {"MatchingLegacyInSuffix", PolicyTransition::kCompleteThenMatchingLegacy, 2U, true},
    {"UpdateAndMatchingLegacyInSuffix", PolicyTransition::kCompleteUpdateThenMatchingLegacy, 2U,
     true},
    {"MatchingUpdatedLegacyInSuffix", PolicyTransition::kCompleteUpdateThenMatchingLegacy, 3U,
     true},
    {"DivergentHistoryInSuffix", PolicyTransition::kCompleteThenDivergentHistory, 2U, false},
    {"DivergentRetryInSuffix", PolicyTransition::kCompleteThenDivergentRetry, 2U, false},
}};

// GoogleTest intentionally registers parameterized cases during static initialization.
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
INSTANTIATE_TEST_SUITE_P(SnapshotSuffixMatrix, MetadataPolicyTransitionTest,
                         testing::ValuesIn(kPolicyTransitionCases),
                         [](const testing::TestParamInfo<PolicyTransitionCase>& parameter) {
                           return std::string{parameter.param.name};
                         });

} // namespace
} // namespace chronos::raft
