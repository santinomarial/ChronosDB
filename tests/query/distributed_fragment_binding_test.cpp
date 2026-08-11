#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/manifest/temporal_validation.hpp"
#include "chronos/query/distributed_fragment_binding.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/schema_lineage.hpp"

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
        (std::filesystem::temp_directory_path() / "chronos-fragment-binding-XXXXXX").string();
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

[[nodiscard]] schema::TableSchema make_schema(const schema::LogicalTypeKind aggregate_kind) {
  const schema::TableId table_id = id<schema::TableId>(2U);
  const schema::SchemaId schema_id = id<schema::SchemaId>(4U);
  const schema::ColumnId event_id = id<schema::ColumnId>(5U);
  const schema::ColumnId value_id = id<schema::ColumnId>(6U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(
                        event_id, "event_time",
                        schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(),
                        false)
                        .value());
  columns.push_back(
      schema::ColumnDefinition::create(value_id, "value",
                                       schema::LogicalType::create(aggregate_kind).value(), true)
          .value());
  return schema::TableSchema::create(table_id, schema_id, schema::SchemaVersion::initial(),
                                     std::nullopt, std::move(columns),
                                     {.event_time_column = event_id,
                                      .physical_ordering_key = {event_id},
                                      .partition_columns = {event_id},
                                      .shard_key = {event_id},
                                      .deduplication_key = {event_id}})
      .value();
}

[[nodiscard]] common::Result<manifest::TemporalDatabaseStorageSnapshot>
make_snapshot(const TemporaryDirectory& directory, const schema::SchemaLineage& lineage,
              const common::Uuid& group_id, const std::uint64_t durable_position) {
  const manifest::DatabaseId database_id = id<manifest::DatabaseId>(1U);
  const schema::TableId table_id = id<schema::TableId>(2U);
  const schema::TabletId tablet_id = id<schema::TabletId>(3U);
  const schema::TableSchema& latest = *lineage.current();
  const manifest::TemporalTabletDescriptor tablet{.table_id = table_id,
                                                  .tablet_id = tablet_id,
                                                  .recovery_schema_id = latest.schema_id(),
                                                  .recovery_schema_version = latest.version(),
                                                  .source_id = group_id,
                                                  .durable_position = durable_position,
                                                  .reclaim_position = 0U,
                                                  .first_part_index = 0U,
                                                  .part_count = 0U,
                                                  .durable_version_count = 0U,
                                                  .commit_source =
                                                      manifest::ManifestCommitSource::kRaft};
  const std::array tablets{tablet};
  auto encoded = manifest::encode_manifest_v2_temporal({.generation = 1U,
                                                        .database_id = database_id,
                                                        .wal_reclaim_checkpoint = std::nullopt,
                                                        .tablets = tablets,
                                                        .parts = {},
                                                        .retries = {}});
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  if (!std::filesystem::create_directory(directory.path() / manifest::kPartsDirectoryName) ||
      !std::filesystem::create_directory(directory.path() / manifest::kManifestDirectoryName)) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "cannot create binding test directory"});
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
  const std::array schema_bindings{
      manifest::TabletSchemaBinding{.tablet_id = tablet_id, .lineage = std::cref(lineage)}};
  const std::array source_bindings{
      manifest::TemporalTabletSourceBinding{.tablet_id = tablet_id,
                                            .commit_source = manifest::ManifestCommitSource::kRaft,
                                            .source_id = group_id}};
  auto loaded = storage->load_selected_temporal_manifest({.expected_database_id = database_id,
                                                          .schema_bindings = schema_bindings,
                                                          .source_bindings = source_bindings,
                                                          .decode_limits = {},
                                                          .part_validation_limits = {}});
  if (!loaded.has_value())
    return common::make_unexpected(loaded.error());
  auto selected =
      std::make_shared<const manifest::LoadedTemporalManifestGeneration>(std::move(*loaded));
  auto publisher = manifest::TemporalDatabaseStoragePublisher::create(selected, schema_bindings);
  if (!publisher.has_value())
    return common::make_unexpected(publisher.error());
  return publisher->snapshot();
}

struct Authority {
  DistributedAggregatePlan plan;
  DistributedReadAdmission admission;
  raft::TabletPlacementMetadata placement;
};

[[nodiscard]] Authority authority() {
  const schema::TabletId tablet_id = id<schema::TabletId>(3U);
  return Authority{
      .plan = {.query_id = uuid(7U),
               .read_policy = {.consistency = DistributedReadConsistency::kLeaderLinearizable,
                               .maximum_staleness_positions = std::nullopt},
               .fragments = {{.tablet_id = tablet_id,
                              .minimum_event_time = 0,
                              .maximum_event_time = 100,
                              .leader_node = 11U,
                              .local_applied_position = 10U,
                              .known_leader_commit_position = 10U}}},
      .admission = {.tablet_id = tablet_id,
                    .serving_node = 11U,
                    .applied_position = 10U,
                    .observed_leader_commit_position = 10U,
                    .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U}},
      .placement = {.table_id = id<schema::TableId>(2U),
                    .tablet_id = tablet_id,
                    .placement_epoch = 12U,
                    .replicas = {11U, 12U},
                    .leader_hint = 11U}};
}

[[nodiscard]] DistributedAggregateFragmentBinding
binding(const Authority& value, const manifest::TemporalDatabaseStorageSnapshot& snapshot,
        const schema::TableSchema& schema_value, const common::Uuid& group_id,
        const std::span<const std::uint32_t> projection) {
  return {.plan = std::cref(value.plan),
          .admission = std::cref(value.admission),
          .snapshot = std::cref(snapshot),
          .destination_schema = std::cref(schema_value),
          .raft_group_id = group_id,
          .placement = std::cref(value.placement),
          .destination_column_ordinals = projection,
          .aggregate_input_index = 1U,
          .event_time_predicate = cseg::EventTimePredicate{
              .lower = cseg::EventTimeBound{4, true}, .upper = cseg::EventTimeBound{9, false}}};
}

TEST(DistributedFragmentBindingTest, BindsOneExactCommittedAuthoritySet) {
  TemporaryDirectory directory;
  const schema::TableSchema schema_value = make_schema(schema::LogicalTypeKind::kFloat64);
  const schema::SchemaLineage lineage = schema::SchemaLineage::create(schema_value).value();
  const common::Uuid group_id = uuid(8U);
  auto snapshot = make_snapshot(directory, lineage, group_id, 10U);
  ASSERT_TRUE(snapshot.has_value()) << snapshot.error().to_string();
  const Authority value = authority();
  const std::array<std::uint32_t, 2U> projection{0U, 1U};

  const auto dispatch = bind_distributed_aggregate_fragment(
      binding(value, *snapshot, schema_value, group_id, projection));
  ASSERT_TRUE(dispatch.has_value()) << dispatch.error().to_string();
  EXPECT_EQ(dispatch->raft_group_id, group_id);
  EXPECT_EQ(dispatch->fragment.query_id, value.plan.query_id);
  EXPECT_EQ(dispatch->fragment.database_id, snapshot->database_id());
  EXPECT_EQ(dispatch->fragment.snapshot_generation, 1U);
  EXPECT_EQ(dispatch->fragment.placement_epoch, 12U);
  EXPECT_EQ(dispatch->fragment.destination_column_ordinals,
            std::vector<std::uint32_t>(projection.begin(), projection.end()));
  EXPECT_EQ(dispatch->fragment.aggregate_input_index, 1U);
  EXPECT_EQ(dispatch->fragment.event_time_predicate,
            binding(value, *snapshot, schema_value, group_id, projection).event_time_predicate);
  EXPECT_TRUE(encode_distributed_aggregate_fragment_dispatch(*dispatch).has_value());
}

TEST(DistributedFragmentBindingTest, RejectsMixedSnapshotPlacementSchemaAndProjectionAuthority) {
  TemporaryDirectory directory;
  const schema::TableSchema schema_value = make_schema(schema::LogicalTypeKind::kFloat64);
  const schema::SchemaLineage lineage = schema::SchemaLineage::create(schema_value).value();
  const common::Uuid group_id = uuid(8U);
  auto snapshot = make_snapshot(directory, lineage, group_id, 10U);
  ASSERT_TRUE(snapshot.has_value()) << snapshot.error().to_string();
  const std::array<std::uint32_t, 2U> projection{0U, 1U};

  Authority value = authority();
  value.admission.applied_position = 11U;
  EXPECT_EQ(bind_distributed_aggregate_fragment(
                binding(value, *snapshot, schema_value, group_id, projection))
                .error()
                .code(),
            common::StatusCode::kUnavailable);

  value = authority();
  value.placement.replicas = {12U};
  value.placement.leader_hint = 12U;
  EXPECT_EQ(bind_distributed_aggregate_fragment(
                binding(value, *snapshot, schema_value, group_id, projection))
                .error()
                .code(),
            common::StatusCode::kUnavailable);

  value = authority();
  value.placement.replicas = {1U, 2U, 3U, 4U, 5U, 6U, 7U, 8U, 9U, 11U};
  EXPECT_EQ(bind_distributed_aggregate_fragment(
                binding(value, *snapshot, schema_value, group_id, projection))
                .error()
                .code(),
            common::StatusCode::kUnavailable);

  value = authority();
  EXPECT_EQ(bind_distributed_aggregate_fragment(
                binding(value, *snapshot, schema_value, uuid(9U), projection))
                .error()
                .code(),
            common::StatusCode::kUnavailable);

  const schema::TableSchema integer_schema = make_schema(schema::LogicalTypeKind::kInt64);
  EXPECT_EQ(bind_distributed_aggregate_fragment(
                binding(value, *snapshot, integer_schema, group_id, projection))
                .error()
                .code(),
            common::StatusCode::kNotSupported);

  const std::array<std::uint32_t, 2U> duplicate{1U, 1U};
  EXPECT_EQ(bind_distributed_aggregate_fragment(
                binding(value, *snapshot, schema_value, group_id, duplicate))
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::query
