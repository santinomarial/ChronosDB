#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/manifest/temporal_validation.hpp"
#include "chronos/query/distributed_fragment_worker.hpp"
#include "chronos/schema/column_definition.hpp"
#include "support/failing_allocator.hpp"

#include <array>
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

namespace chronos::query {
namespace {

template <typename Operation>
[[nodiscard]] auto run_failure(const std::size_t fail_after, Operation&& operation) {
  using Result = decltype(operation());
  std::optional<Result> result;
  {
    ::chronos::test::ScopedAllocationFailure failure{fail_after};
    result.emplace(operation());
    failure.disable();
  }
  return std::move(*result);
}

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

template <typename Id> [[nodiscard]] Id id(const std::uint8_t seed) {
  return Id::from_uuid(uuid(seed)).value();
}

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-vector-aggregate-worker-XXXXXX")
            .string();
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

void write_file(const std::filesystem::path& path, const common::ByteView bytes) {
  std::ofstream output{path, std::ios::binary};
  ASSERT_TRUE(output.good());
  for (const std::byte value : bytes)
    output.put(static_cast<char>(std::to_integer<unsigned char>(value)));
  output.close();
  ASSERT_TRUE(output.good());
}

[[nodiscard]] std::shared_ptr<const schema::TableSchema> schema_value() {
  const schema::ColumnId event_id = id<schema::ColumnId>(5U);
  const schema::ColumnId value_id = id<schema::ColumnId>(6U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(
                        event_id, "event_time",
                        schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(),
                        false)
                        .value());
  columns.push_back(schema::ColumnDefinition::create(
                        value_id, "value",
                        schema::LogicalType::create(schema::LogicalTypeKind::kFloat64).value(),
                        true)
                        .value());
  return std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(id<schema::TableId>(2U), id<schema::SchemaId>(4U),
                                  schema::SchemaVersion::initial(), std::nullopt,
                                  std::move(columns),
                                  {.event_time_column = event_id,
                                   .physical_ordering_key = {event_id},
                                   .partition_columns = {event_id},
                                   .shard_key = {event_id},
                                   .deduplication_key = {event_id}})
          .value());
}

TEST(DistributedVectorAggregateWorkerAllocationFailureTest,
     ClassifiesEveryEmptyTabletAllocationBeforePublishingStates) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  ASSERT_TRUE(std::filesystem::create_directory(directory.path() / manifest::kPartsDirectoryName));
  ASSERT_TRUE(
      std::filesystem::create_directory(directory.path() / manifest::kManifestDirectoryName));
  write_file(directory.path() / manifest::kManifestDirectoryName / manifest::kManifestLockFileName,
             {});

  const auto schema = schema_value();
  const schema::SchemaLineage lineage = schema::SchemaLineage::create(*schema).value();
  const manifest::DatabaseId database_id = id<manifest::DatabaseId>(1U);
  const schema::TabletId tablet_id = id<schema::TabletId>(3U);
  const common::Uuid group_id = uuid(8U);
  const std::array tablets{
      manifest::TemporalTabletDescriptor{.table_id = schema->table_id(),
                                         .tablet_id = tablet_id,
                                         .recovery_schema_id = schema->schema_id(),
                                         .recovery_schema_version = schema->version(),
                                         .source_id = group_id,
                                         .durable_position = 10U,
                                         .reclaim_position = 0U,
                                         .first_part_index = 0U,
                                         .part_count = 0U,
                                         .durable_version_count = 0U,
                                         .commit_source = manifest::ManifestCommitSource::kRaft}};
  const auto encoded = manifest::encode_manifest_v2_temporal(
      {.generation = 1U, .database_id = database_id, .tablets = tablets});
  ASSERT_TRUE(encoded.has_value());
  write_file(directory.path() / manifest::kManifestDirectoryName /
                 *manifest::manifest_file_name(1U),
             encoded->bytes());
  auto storage =
      manifest::ManifestStorage::open_existing({.database_root = directory.path().string()});
  ASSERT_TRUE(storage.has_value());
  const std::array schema_bindings{
      manifest::TabletSchemaBinding{.tablet_id = tablet_id, .lineage = std::cref(lineage)}};
  const std::array source_bindings{
      manifest::TemporalTabletSourceBinding{.tablet_id = tablet_id,
                                            .commit_source = manifest::ManifestCommitSource::kRaft,
                                            .source_id = group_id}};
  auto loaded = storage->load_selected_temporal_manifest({.expected_database_id = database_id,
                                                          .schema_bindings = schema_bindings,
                                                          .source_bindings = source_bindings});
  ASSERT_TRUE(loaded.has_value());
  auto selected =
      std::make_shared<const manifest::LoadedTemporalManifestGeneration>(std::move(*loaded));
  auto publisher = manifest::TemporalDatabaseStoragePublisher::create(selected, schema_bindings);
  ASSERT_TRUE(publisher.has_value());
  auto snapshot = publisher->snapshot();
  ASSERT_TRUE(snapshot.has_value());

  const DistributedVectorFragmentDispatchV2 dispatch{
      .dispatch = {.query_id = uuid(7U),
                   .database_id = database_id,
                   .table_id = schema->table_id(),
                   .tablet_id = tablet_id,
                   .destination_schema_id = schema->schema_id(),
                   .raft_group_id = group_id,
                   .snapshot_generation = 1U,
                   .serving_node = 11U,
                   .applied_position = 10U,
                   .observed_leader_commit_position = 10U,
                   .placement_epoch = 12U,
                   .read_policy = {.consistency = DistributedReadConsistency::kLeaderLinearizable},
                   .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
                   .destination_column_ordinals = {0U, 1U},
                   .plan = {.mode = DistributedVectorPlanMode::kUngroupedAggregate,
                            .aggregates = {{.operation = VectorAggregateOperation::kCountStar,
                                            .input_index = std::nullopt},
                                           {.operation = VectorAggregateOperation::kSum,
                                            .input_index = 1U}}}},
      .result_schema = {
          .columns = {{"count",
                       schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(), false},
                      {"sum", schema->columns()[1].type(), true}}}};
  const raft::TabletPlacementMetadata placement{.table_id = schema->table_id(),
                                                .tablet_id = tablet_id,
                                                .placement_epoch = 12U,
                                                .replicas = {11U},
                                                .leader_hint = 11U};
  const DistributedVectorAggregateWorkerRequestV2 request{.dispatch = std::cref(dispatch),
                                                          .storage = std::cref(*storage),
                                                          .snapshot = std::cref(*snapshot),
                                                          .lineage = std::cref(lineage),
                                                          .placement = std::cref(placement),
                                                          .raft_group_id = group_id,
                                                          .local_node = 11U,
                                                          .local_linearizable_barrier =
                                                              raft::ReadBarrier{2U, 3U, 10U}};

  bool succeeded{};
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    auto result = run_failure(
        fail_after, [&] { return execute_distributed_vector_aggregate_fragment_v2(request); });
    if (result.has_value()) {
      EXPECT_EQ(result->input_rows, 0U);
      ASSERT_EQ(result->messages.size(), 2U);
      succeeded = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(succeeded);

  DistributedVectorFragmentDispatchV2 grouped_dispatch = dispatch;
  grouped_dispatch.dispatch.plan = {
      .mode = DistributedVectorPlanMode::kGroupedAggregate,
      .group_key_input_indices = {1U},
      .aggregates = {
          {.operation = VectorAggregateOperation::kCountStar, .input_index = std::nullopt},
          {.operation = VectorAggregateOperation::kSum, .input_index = 1U}}};
  grouped_dispatch.result_schema = {
      .columns = {
          {"value", schema->columns()[1].type(), true},
          {"count", schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(), false},
          {"sum", schema->columns()[1].type(), true}}};
  const DistributedVectorGroupedAggregateWorkerRequestV2 grouped_request{
      .dispatch = std::cref(grouped_dispatch),
      .storage = std::cref(*storage),
      .snapshot = std::cref(*snapshot),
      .lineage = std::cref(lineage),
      .placement = std::cref(placement),
      .raft_group_id = group_id,
      .local_node = 11U,
      .local_linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U}};
  bool grouped_succeeded{};
  for (std::size_t fail_after = 0U; fail_after < 192U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      return execute_distributed_vector_grouped_aggregate_fragment_v2(grouped_request);
    });
    if (result.has_value()) {
      EXPECT_EQ(result->input_rows, 0U);
      EXPECT_EQ(result->group_count, 0U);
      EXPECT_GT(result->encoded_bytes, 0U);
      ASSERT_EQ(result->messages.size(), 1U);
      QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
      auto decoded = decode_distributed_vector_grouped_aggregate_exchange_message_exact(
          result->messages[0].bytes(), result->authority.keys, result->authority.aggregates,
          resources);
      ASSERT_TRUE(decoded.has_value());
      EXPECT_TRUE(decoded->position().empty);
      EXPECT_TRUE(decoded->position().terminal);
      grouped_succeeded = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(grouped_succeeded);
}

} // namespace
} // namespace chronos::query
