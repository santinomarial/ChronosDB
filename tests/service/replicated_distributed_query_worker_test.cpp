#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/manifest/temporal_part_validation.hpp"
#include "chronos/manifest/temporal_validation.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/service/replicated_distributed_query_worker.hpp"
#include "cseg/cseg_test_fixture.hpp"

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

namespace chronos::service {
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
        (std::filesystem::temp_directory_path() / "chronos-query-worker-service-XXXXXX").string();
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

[[nodiscard]] std::shared_ptr<const schema::SchemaLineage> make_lineage() {
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
                        false)
                        .value());
  auto schema_value = schema::TableSchema::create(id<schema::TableId>(2U), id<schema::SchemaId>(4U),
                                                  schema::SchemaVersion::initial(), std::nullopt,
                                                  std::move(columns),
                                                  {.event_time_column = event_id,
                                                   .physical_ordering_key = {event_id},
                                                   .partition_columns = {event_id},
                                                   .shard_key = {event_id},
                                                   .deduplication_key = {event_id}});
  return std::make_shared<const schema::SchemaLineage>(
      schema::SchemaLineage::create(std::move(*schema_value)).value());
}

class ContextProvider final : public ReplicatedDistributedQueryWorkerContextProvider {
public:
  ContextProvider(manifest::TemporalDatabaseStorageSnapshot snapshot,
                  std::shared_ptr<const schema::SchemaLineage> lineage,
                  raft::TabletPlacementMetadata placement, raft::GroupId raft_group_id,
                  std::optional<raft::ReadBarrier> barrier)
      : snapshot_(std::move(snapshot)), lineage_(std::move(lineage)),
        placement_(std::move(placement)), raft_group_id_(raft_group_id), barrier_(barrier) {}

  common::Result<ReplicatedDistributedQueryWorkerContext>
  acquire(const query::DistributedAggregateFragmentDispatch&) override {
    ++calls;
    return ReplicatedDistributedQueryWorkerContext{.snapshot = snapshot_,
                                                   .lineage = lineage_,
                                                   .placement = placement_,
                                                   .raft_group_id = raft_group_id_,
                                                   .local_linearizable_barrier = barrier_};
  }

  void set_placement_epoch(const std::uint64_t epoch) noexcept {
    placement_.placement_epoch = epoch;
  }

  void set_raft_group_id(const raft::GroupId& group_id) noexcept {
    raft_group_id_ = group_id;
  }

  void clear_lineage() noexcept {
    lineage_.reset();
  }

  std::size_t calls{};

private:
  manifest::TemporalDatabaseStorageSnapshot snapshot_;
  std::shared_ptr<const schema::SchemaLineage> lineage_;
  raft::TabletPlacementMetadata placement_;
  raft::GroupId raft_group_id_;
  std::optional<raft::ReadBarrier> barrier_;
};

TEST(ReplicatedDistributedQueryWorkerTest, AcquiresFreshAuthorityAndExecutesRealCseg) {
  EXPECT_EQ(ReplicatedDistributedQueryWorker::create({}).error().code(),
            common::StatusCode::kInvalidArgument);

  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  ASSERT_TRUE(std::filesystem::create_directory(directory.path() / manifest::kPartsDirectoryName));
  ASSERT_TRUE(
      std::filesystem::create_directory(directory.path() / manifest::kManifestDirectoryName));
  write_file(directory.path() / manifest::kManifestDirectoryName / manifest::kManifestLockFileName,
             {});

  const auto lineage = make_lineage();
  const auto schema_value = lineage->current();
  ASSERT_NE(schema_value, nullptr);
  const manifest::DatabaseId database_id = id<manifest::DatabaseId>(1U);
  const schema::TabletId tablet_id = id<schema::TabletId>(3U);
  const common::Uuid group_id = uuid(8U);
  const cseg::EncodedCsegPart encoded_part = cseg::test::make_valid_temporal_float64_part(
      cseg::PageCompression::kNone,
      {.commit_source = cseg::temporal_format::CommitSource::kRaft, .source_id = group_id});
  auto part = manifest::describe_manifest_v2_temporal_part_image(
      encoded_part.bytes(), *schema_value, tablet_id, manifest::ManifestCommitSource::kRaft,
      group_id);
  ASSERT_TRUE(part.has_value()) << part.error().to_string();
  write_file(directory.path() / manifest::kPartsDirectoryName /
                 manifest::part_file_name(part->part_id),
             encoded_part.bytes());
  const std::array tablets{
      manifest::TemporalTabletDescriptor{.table_id = schema_value->table_id(),
                                         .tablet_id = tablet_id,
                                         .recovery_schema_id = schema_value->schema_id(),
                                         .recovery_schema_version = schema_value->version(),
                                         .source_id = group_id,
                                         .durable_position = 10U,
                                         .reclaim_position = 0U,
                                         .first_part_index = 0U,
                                         .part_count = 1U,
                                         .durable_version_count = 2U,
                                         .commit_source = manifest::ManifestCommitSource::kRaft}};
  const std::array parts{*part};
  auto encoded_manifest =
      manifest::encode_manifest_v2_temporal({.generation = 1U,
                                             .database_id = database_id,
                                             .wal_reclaim_checkpoint = std::nullopt,
                                             .tablets = tablets,
                                             .parts = parts,
                                             .retries = {}});
  ASSERT_TRUE(encoded_manifest.has_value()) << encoded_manifest.error().to_string();
  write_file(directory.path() / manifest::kManifestDirectoryName /
                 *manifest::manifest_file_name(1U),
             encoded_manifest->bytes());
  auto storage =
      manifest::ManifestStorage::open_existing({.database_root = directory.path().string()});
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  const std::array schema_bindings{
      manifest::TabletSchemaBinding{.tablet_id = tablet_id, .lineage = std::cref(*lineage)}};
  const std::array source_bindings{
      manifest::TemporalTabletSourceBinding{.tablet_id = tablet_id,
                                            .commit_source = manifest::ManifestCommitSource::kRaft,
                                            .source_id = group_id}};
  auto loaded = storage->load_selected_temporal_manifest({.expected_database_id = database_id,
                                                          .schema_bindings = schema_bindings,
                                                          .source_bindings = source_bindings,
                                                          .decode_limits = {},
                                                          .part_validation_limits = {}});
  ASSERT_TRUE(loaded.has_value()) << loaded.error().to_string();
  auto selected =
      std::make_shared<const manifest::LoadedTemporalManifestGeneration>(std::move(*loaded));
  auto publisher = manifest::TemporalDatabaseStoragePublisher::create(selected, schema_bindings);
  ASSERT_TRUE(publisher.has_value()) << publisher.error().to_string();
  auto snapshot = publisher->snapshot();
  ASSERT_TRUE(snapshot.has_value()) << snapshot.error().to_string();

  const query::DistributedAggregateFragmentDispatch dispatch{
      .raft_group_id = group_id,
      .fragment = {
          .query_id = uuid(7U),
          .database_id = database_id,
          .table_id = schema_value->table_id(),
          .tablet_id = tablet_id,
          .destination_schema_id = schema_value->schema_id(),
          .snapshot_generation = 1U,
          .serving_node = 11U,
          .applied_position = 10U,
          .observed_leader_commit_position = 10U,
          .placement_epoch = 12U,
          .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable,
                          .maximum_staleness_positions = std::nullopt},
          .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
          .destination_column_ordinals = {0U, 1U},
          .aggregate_input_index = 1U,
          .event_time_predicate = cseg::EventTimePredicate{.lower = cseg::EventTimeBound{15, true},
                                                           .upper = std::nullopt}}};
  ContextProvider provider{std::move(*snapshot),
                           lineage,
                           {.table_id = schema_value->table_id(),
                            .tablet_id = tablet_id,
                            .placement_epoch = 12U,
                            .replicas = {11U, 12U},
                            .leader_hint = 11U},
                           group_id,
                           raft::ReadBarrier{2U, 3U, 10U}};
  auto worker = ReplicatedDistributedQueryWorker::create(
      {.local_node_id = 11U, .storage = &*storage, .context_provider = &provider});
  ASSERT_TRUE(worker.has_value()) << worker.error().to_string();

  auto result = worker->execute(dispatch);
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_EQ(result->query_id, dispatch.fragment.query_id);
  EXPECT_EQ(result->tablet_id, tablet_id);
  EXPECT_EQ(result->partial.count, 1U);
  EXPECT_EQ(result->partial.sum, 2.5);
  EXPECT_TRUE(result->terminal);
  EXPECT_EQ(provider.calls, 1U);

  provider.set_raft_group_id(uuid(9U));
  EXPECT_EQ(worker->execute(dispatch).error().code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(provider.calls, 2U);

  provider.set_raft_group_id(group_id);
  provider.set_placement_epoch(13U);
  EXPECT_EQ(worker->execute(dispatch).error().code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(provider.calls, 3U);

  provider.clear_lineage();
  EXPECT_EQ(worker->execute(dispatch).error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(provider.calls, 4U);
}

} // namespace
} // namespace chronos::service
