#include "chronos/cluster/distributed_query_execution.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/schema_lineage.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-query-execution-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

template <typename Id> [[nodiscard]] Id id(const std::uint8_t seed) {
  return Id::from_uuid(uuid(seed)).value();
}

void write_file(const std::filesystem::path& path, const common::ByteView bytes) {
  std::ofstream output{path, std::ios::binary};
  for (const std::byte value : bytes)
    output.put(static_cast<char>(std::to_integer<unsigned char>(value)));
}

[[nodiscard]] schema::TableSchema schema_value() {
  const auto event = id<schema::ColumnId>(5U);
  const auto value = id<schema::ColumnId>(6U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(
                        event, "event_time",
                        schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(),
                        false)
                        .value());
  columns.push_back(schema::ColumnDefinition::create(
                        value, "value",
                        schema::LogicalType::create(schema::LogicalTypeKind::kFloat64).value(),
                        true)
                        .value());
  return schema::TableSchema::create(id<schema::TableId>(2U), id<schema::SchemaId>(4U),
                                     schema::SchemaVersion::initial(), std::nullopt,
                                     std::move(columns),
                                     {.event_time_column = event,
                                      .physical_ordering_key = {event},
                                      .partition_columns = {event},
                                      .shard_key = {event},
                                      .deduplication_key = {event}})
      .value();
}

struct ExecutionInput {
  query::DistributedAggregatePlan plan;
  std::vector<query::DistributedReadAdmission> admissions;
  query::CompatibleDistributedAggregateSnapshot snapshot;
};

[[nodiscard]] common::Result<ExecutionInput> make_input(const TemporaryDirectory& directory) {
  const schema::TableSchema schema = schema_value();
  const schema::SchemaLineage lineage = schema::SchemaLineage::create(schema).value();
  const std::array tablets{id<schema::TabletId>(3U), id<schema::TabletId>(9U)};
  const std::array groups{uuid(8U), uuid(10U)};
  const std::array positions{10U, 20U};
  std::vector<manifest::TemporalTabletDescriptor> descriptors;
  for (std::size_t index = 0U; index < tablets.size(); ++index) {
    descriptors.push_back({.table_id = schema.table_id(),
                           .tablet_id = tablets[index],
                           .recovery_schema_id = schema.schema_id(),
                           .recovery_schema_version = schema.version(),
                           .source_id = groups[index],
                           .durable_position = positions[index],
                           .reclaim_position = 0U,
                           .first_part_index = 0U,
                           .part_count = 0U,
                           .durable_version_count = 0U,
                           .commit_source = manifest::ManifestCommitSource::kRaft});
  }
  auto encoded = manifest::encode_manifest_v2_temporal({.generation = 1U,
                                                        .database_id = id<manifest::DatabaseId>(1U),
                                                        .wal_reclaim_checkpoint = std::nullopt,
                                                        .tablets = descriptors,
                                                        .parts = {},
                                                        .retries = {}});
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  if (!std::filesystem::create_directory(directory.path() / manifest::kPartsDirectoryName) ||
      !std::filesystem::create_directory(directory.path() / manifest::kManifestDirectoryName)) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "cannot create execution fixture"});
  }
  write_file(directory.path() / manifest::kManifestDirectoryName / manifest::kManifestLockFileName,
             {});
  write_file(directory.path() / manifest::kManifestDirectoryName /
                 *manifest::manifest_file_name(1U),
             encoded->bytes());
  auto storage =
      manifest::ManifestStorage::open_existing({.database_root = directory.path().string()});
  if (!storage.has_value())
    return common::make_unexpected(storage.error());
  std::vector<manifest::TabletSchemaBinding> schema_bindings;
  std::vector<manifest::TemporalTabletSourceBinding> source_bindings;
  for (std::size_t index = 0U; index < tablets.size(); ++index) {
    schema_bindings.push_back({tablets[index], std::cref(lineage)});
    source_bindings.push_back(
        {tablets[index], manifest::ManifestCommitSource::kRaft, groups[index]});
  }
  auto loaded = storage->load_selected_temporal_manifest(
      {.expected_database_id = id<manifest::DatabaseId>(1U),
       .schema_bindings = schema_bindings,
       .source_bindings = source_bindings});
  if (!loaded.has_value())
    return common::make_unexpected(loaded.error());
  auto owner =
      std::make_shared<const manifest::LoadedTemporalManifestGeneration>(std::move(*loaded));
  auto publisher = manifest::TemporalDatabaseStoragePublisher::create(owner, schema_bindings);
  if (!publisher.has_value())
    return common::make_unexpected(publisher.error());
  auto snapshot = publisher->snapshot();
  if (!snapshot.has_value())
    return common::make_unexpected(snapshot.error());

  query::DistributedAggregatePlan plan{
      .query_id = uuid(7U),
      .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
      .fragments = {{tablets[0], 0, 100, 11U, 10U, 10U}, {tablets[1], 101, 200, 12U, 20U, 20U}}};
  std::vector<query::DistributedReadAdmission> admissions{
      {tablets[0], 11U, 10U, 10U, raft::ReadBarrier{2U, 3U, 10U}},
      {tablets[1], 12U, 20U, 20U, raft::ReadBarrier{2U, 4U, 20U}}};
  const std::array placements{
      raft::TabletPlacementMetadata{schema.table_id(), tablets[0], 12U, {11U, 13U}, 11U},
      raft::TabletPlacementMetadata{schema.table_id(), tablets[1], 13U, {12U, 14U}, 12U}};
  const std::array<std::uint32_t, 2U> projection{0U, 1U};
  const std::array bindings{query::DistributedAggregateSnapshotFragmentBinding{
                                std::cref(admissions[0]), std::cref(schema), groups[0],
                                std::cref(placements[0]), projection, 1U, std::nullopt},
                            query::DistributedAggregateSnapshotFragmentBinding{
                                std::cref(admissions[1]), std::cref(schema), groups[1],
                                std::cref(placements[1]), projection, 1U, std::nullopt}};
  auto compatible = query::bind_compatible_distributed_aggregate_snapshot(
      plan, std::move(*snapshot), bindings,
      {.maximum_fragments = 2U, .maximum_total_projection_ordinals = 4U});
  if (!compatible.has_value())
    return common::make_unexpected(compatible.error());
  return ExecutionInput{std::move(plan), std::move(admissions), std::move(*compatible)};
}

[[nodiscard]] query::ExchangeMessage message(const schema::TabletId& tablet, const double value) {
  query::MergeableAggregateState partial;
  EXPECT_TRUE(partial.add(value).is_ok());
  return {uuid(7U), tablet, 1U, partial, true};
}

TEST(DistributedQueryExecutionTest, DeliversEveryTerminalResultExactlyOnceAndFinishes) {
  TemporaryDirectory directory;
  auto input = make_input(directory);
  ASSERT_TRUE(input.has_value()) << input.error().to_string();
  const auto tablets =
      std::array{input->plan.fragments[0].tablet_id, input->plan.fragments[1].tablet_id};
  auto execution = DistributedQueryExecution::create(
      1U, std::move(input->plan), std::move(input->admissions), std::move(input->snapshot));
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();
  EXPECT_EQ(execution->snapshot().snapshot().generation(), 1U);
  EXPECT_EQ(execution->finish().error().code(), common::StatusCode::kUnavailable);
  const auto now = DistributedQueryExecution::TimePoint{};
  ASSERT_TRUE(execution->begin_attempt(tablets[0], now).has_value());
  ASSERT_TRUE(execution->begin_attempt(tablets[1], now).has_value());

  const auto first =
      encode_distributed_query_response_v1({11U, 1U, uuid(7U), tablets[0], common::StatusCode::kOk,
                                            message(tablets[0], 2.5), std::nullopt})
          .value();
  ASSERT_TRUE(execution->accept_response(tablets[0], first, now).is_ok());
  EXPECT_EQ(execution->finish().error().code(), common::StatusCode::kUnavailable);
  const auto second =
      encode_distributed_query_response_v1({12U, 1U, uuid(7U), tablets[1], common::StatusCode::kOk,
                                            message(tablets[1], 3.5), std::nullopt})
          .value();
  ASSERT_TRUE(execution->accept_response(tablets[1], second, now).is_ok());
  const auto result = execution->finish();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_EQ(result->count, 2U);
  EXPECT_EQ(result->sum, 6.0);
  EXPECT_EQ(execution->accept_response(tablets[1], second, now).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(execution->begin_attempt(id<schema::TabletId>(99U), now).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedQueryExecutionTest, RetryBackoffDoesNotFailUntilSenderBecomesTerminal) {
  TemporaryDirectory directory;
  auto input = make_input(directory);
  ASSERT_TRUE(input.has_value()) << input.error().to_string();
  const schema::TabletId tablet = input->plan.fragments[0].tablet_id;
  auto execution = DistributedQueryExecution::create(
      1U, std::move(input->plan), std::move(input->admissions), std::move(input->snapshot),
      {.coordinator = {},
       .retry = {.maximum_attempts = 2U,
                 .initial_backoff = std::chrono::milliseconds{10},
                 .maximum_backoff = std::chrono::milliseconds{10}}});
  ASSERT_TRUE(execution.has_value());
  const auto now = DistributedQueryExecution::TimePoint{};
  ASSERT_TRUE(execution->begin_attempt(tablet, now).has_value());
  const auto retry = encode_distributed_query_response_v1(
                         {11U, 1U, uuid(7U), tablet, common::StatusCode::kUnavailable, std::nullopt,
                          DistributedQueryLeaderHint{13U, 14U}})
                         .value();
  ASSERT_TRUE(execution->accept_response(tablet, retry, now).is_ok());
  EXPECT_EQ(*execution->sender_state(tablet), DistributedQuerySenderState::kBackoff);
  EXPECT_EQ(*execution->suggested_leader(tablet), DistributedQueryLeaderHint(13U, 14U));
  ASSERT_TRUE(execution->begin_attempt(tablet, now + std::chrono::milliseconds{10}).has_value());
  ASSERT_TRUE(execution
                  ->record_transport_failure(tablet, common::StatusCode::kIoError,
                                             now + std::chrono::milliseconds{10})
                  .is_ok());
  EXPECT_EQ(*execution->sender_state(tablet), DistributedQuerySenderState::kFailed);
  EXPECT_EQ(execution->finish().error().code(), common::StatusCode::kIoError);
}

TEST(DistributedQueryExecutionTest, RejectsAdmissionOrderThatDiffersFromPinnedDispatches) {
  TemporaryDirectory directory;
  auto input = make_input(directory);
  ASSERT_TRUE(input.has_value()) << input.error().to_string();
  std::swap(input->admissions[0], input->admissions[1]);
  EXPECT_EQ(DistributedQueryExecution::create(1U, std::move(input->plan),
                                              std::move(input->admissions),
                                              std::move(input->snapshot))
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::cluster
