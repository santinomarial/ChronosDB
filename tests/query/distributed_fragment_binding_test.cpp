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
#include <span>
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

struct SnapshotTabletSpec {
  schema::TabletId tablet_id;
  common::Uuid group_id;
  std::uint64_t durable_position{};
};

[[nodiscard]] common::Result<manifest::TemporalDatabaseStorageSnapshot>
make_snapshot(const TemporaryDirectory& directory, const schema::SchemaLineage& lineage,
              const std::span<const SnapshotTabletSpec> tablet_specs) {
  const manifest::DatabaseId database_id = id<manifest::DatabaseId>(1U);
  const schema::TableId table_id = id<schema::TableId>(2U);
  const schema::TableSchema& latest = *lineage.current();
  std::vector<manifest::TemporalTabletDescriptor> tablets;
  tablets.reserve(tablet_specs.size());
  for (const auto& spec : tablet_specs) {
    tablets.push_back({.table_id = table_id,
                       .tablet_id = spec.tablet_id,
                       .recovery_schema_id = latest.schema_id(),
                       .recovery_schema_version = latest.version(),
                       .source_id = spec.group_id,
                       .durable_position = spec.durable_position,
                       .reclaim_position = 0U,
                       .first_part_index = 0U,
                       .part_count = 0U,
                       .durable_version_count = 0U,
                       .commit_source = manifest::ManifestCommitSource::kRaft});
  }
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
  std::vector<manifest::TabletSchemaBinding> schema_bindings;
  std::vector<manifest::TemporalTabletSourceBinding> source_bindings;
  schema_bindings.reserve(tablet_specs.size());
  source_bindings.reserve(tablet_specs.size());
  for (const auto& spec : tablet_specs) {
    schema_bindings.push_back({.tablet_id = spec.tablet_id, .lineage = std::cref(lineage)});
    source_bindings.push_back({.tablet_id = spec.tablet_id,
                               .commit_source = manifest::ManifestCommitSource::kRaft,
                               .source_id = spec.group_id});
  }
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

[[nodiscard]] common::Result<manifest::TemporalDatabaseStorageSnapshot>
make_snapshot(const TemporaryDirectory& directory, const schema::SchemaLineage& lineage,
              const common::Uuid& group_id, const std::uint64_t durable_position) {
  const std::array specs{SnapshotTabletSpec{id<schema::TabletId>(3U), group_id, durable_position}};
  return make_snapshot(directory, lineage, specs);
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

TEST(DistributedFragmentBindingTest, BindsGroupedFloat64IntentThroughTheSameAuthoritySet) {
  TemporaryDirectory directory;
  const schema::TableSchema schema_value = make_schema(schema::LogicalTypeKind::kFloat64);
  const schema::SchemaLineage lineage = schema::SchemaLineage::create(schema_value).value();
  const common::Uuid group_id = uuid(8U);
  auto snapshot = make_snapshot(directory, lineage, group_id, 10U);
  ASSERT_TRUE(snapshot.has_value()) << snapshot.error().to_string();
  const Authority value = authority();
  const std::array<std::uint32_t, 2U> projection{0U, 1U};

  const DistributedGroupedFloat64FragmentBinding grouped{
      .aggregate = binding(value, *snapshot, schema_value, group_id, projection),
      .group_key_input_index = 1U};
  const auto bound = bind_distributed_grouped_float64_fragment(grouped);
  ASSERT_TRUE(bound.has_value()) << bound.error().to_string();
  EXPECT_EQ(bound->raft_group_id, group_id);
  EXPECT_EQ(bound->fragment.aggregate.query_id, value.plan.query_id);
  EXPECT_EQ(bound->fragment.aggregate.database_id, snapshot->database_id());
  EXPECT_EQ(bound->fragment.aggregate.destination_column_ordinals,
            std::vector<std::uint32_t>(projection.begin(), projection.end()));
  EXPECT_EQ(bound->fragment.group_key_input_index, 1U);
  EXPECT_TRUE(encode_distributed_grouped_float64_fragment(bound->fragment).has_value());
  const auto packaged = bind_distributed_grouped_float64_fragment_dispatch(grouped);
  ASSERT_TRUE(packaged.has_value()) << packaged.error().to_string();
  EXPECT_EQ(packaged->raft_group_id, group_id);
  EXPECT_TRUE(encode_distributed_grouped_float64_fragment_dispatch(*packaged).has_value());

  DistributedGroupedFloat64FragmentBinding unsupported = grouped;
  unsupported.group_key_input_index = 0U;
  EXPECT_EQ(bind_distributed_grouped_float64_fragment(unsupported).error().code(),
            common::StatusCode::kNotSupported);
  EXPECT_EQ(bind_distributed_grouped_float64_fragment_dispatch(unsupported).error().code(),
            common::StatusCode::kNotSupported);
  DistributedGroupedFloat64FragmentBinding out_of_bounds = grouped;
  out_of_bounds.group_key_input_index = 2U;
  EXPECT_EQ(bind_distributed_grouped_float64_fragment(out_of_bounds).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedFragmentBindingTest, PinsOneCompatibleEpochAcrossEveryPlannedTablet) {
  TemporaryDirectory directory;
  const schema::TableSchema schema_value = make_schema(schema::LogicalTypeKind::kFloat64);
  const schema::SchemaLineage lineage = schema::SchemaLineage::create(schema_value).value();
  const std::array specs{SnapshotTabletSpec{id<schema::TabletId>(3U), uuid(8U), 10U},
                         SnapshotTabletSpec{id<schema::TabletId>(9U), uuid(10U), 20U}};
  auto snapshot = make_snapshot(directory, lineage, specs);
  ASSERT_TRUE(snapshot.has_value()) << snapshot.error().to_string();
  std::weak_ptr<const manifest::LoadedTemporalManifestGeneration> pinned =
      snapshot->selected_manifest();

  const DistributedAggregatePlan plan{
      .query_id = uuid(7U),
      .read_policy = {.consistency = DistributedReadConsistency::kLeaderLinearizable,
                      .maximum_staleness_positions = std::nullopt},
      .fragments = {{.tablet_id = specs[0].tablet_id,
                     .minimum_event_time = 0,
                     .maximum_event_time = 100,
                     .leader_node = 11U,
                     .local_applied_position = 10U,
                     .known_leader_commit_position = 10U},
                    {.tablet_id = specs[1].tablet_id,
                     .minimum_event_time = 101,
                     .maximum_event_time = 200,
                     .leader_node = 12U,
                     .local_applied_position = 20U,
                     .known_leader_commit_position = 20U}}};
  const std::array admissions{
      DistributedReadAdmission{specs[0].tablet_id, 11U, 10U, 10U, raft::ReadBarrier{2U, 3U, 10U}},
      DistributedReadAdmission{specs[1].tablet_id, 12U, 20U, 20U, raft::ReadBarrier{2U, 4U, 20U}}};
  const std::array placements{raft::TabletPlacementMetadata{.table_id = id<schema::TableId>(2U),
                                                            .tablet_id = specs[0].tablet_id,
                                                            .placement_epoch = 12U,
                                                            .replicas = {11U, 13U},
                                                            .leader_hint = 11U},
                              raft::TabletPlacementMetadata{.table_id = id<schema::TableId>(2U),
                                                            .tablet_id = specs[1].tablet_id,
                                                            .placement_epoch = 13U,
                                                            .replicas = {12U, 14U},
                                                            .leader_hint = 12U}};
  const std::array<std::uint32_t, 2U> projection{0U, 1U};
  const std::array bindings{
      DistributedAggregateSnapshotFragmentBinding{.admission = std::cref(admissions[0]),
                                                  .destination_schema = std::cref(schema_value),
                                                  .raft_group_id = specs[0].group_id,
                                                  .placement = std::cref(placements[0]),
                                                  .destination_column_ordinals = projection,
                                                  .aggregate_input_index = 1U},
      DistributedAggregateSnapshotFragmentBinding{.admission = std::cref(admissions[1]),
                                                  .destination_schema = std::cref(schema_value),
                                                  .raft_group_id = specs[1].group_id,
                                                  .placement = std::cref(placements[1]),
                                                  .destination_column_ordinals = projection,
                                                  .aggregate_input_index = 1U}};

  const std::array reversed{bindings[1], bindings[0]};
  EXPECT_EQ(
      bind_compatible_distributed_aggregate_snapshot(plan, *snapshot, reversed).error().code(),
      common::StatusCode::kInvalidArgument);
  DistributedAggregatePlan duplicate_plan = plan;
  duplicate_plan.fragments[1].tablet_id = duplicate_plan.fragments[0].tablet_id;
  EXPECT_EQ(bind_compatible_distributed_aggregate_snapshot(duplicate_plan, *snapshot, bindings)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(bind_compatible_distributed_aggregate_snapshot(
                plan, *snapshot, bindings,
                {.maximum_fragments = 2U, .maximum_total_projection_ordinals = 3U})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);

  auto compatible = bind_compatible_distributed_aggregate_snapshot(
      plan, std::move(*snapshot), bindings,
      {.maximum_fragments = 2U, .maximum_total_projection_ordinals = 4U});
  ASSERT_TRUE(compatible.has_value()) << compatible.error().to_string();
  EXPECT_FALSE(pinned.expired());
  EXPECT_EQ(compatible->snapshot().generation(), 1U);
  ASSERT_EQ(compatible->dispatches().size(), 2U);
  for (std::size_t index = 0U; index < compatible->dispatches().size(); ++index) {
    EXPECT_EQ(compatible->dispatches()[index].fragment.tablet_id, plan.fragments[index].tablet_id);
    EXPECT_EQ(compatible->dispatches()[index].fragment.database_id,
              compatible->snapshot().database_id());
    EXPECT_EQ(compatible->dispatches()[index].fragment.snapshot_generation,
              compatible->snapshot().generation());
  }
}

TEST(DistributedFragmentBindingTest, ResolvesCommittedMetadataAndCurrentReplicaProofs) {
  TemporaryDirectory directory;
  const schema::TableSchema schema_value = make_schema(schema::LogicalTypeKind::kFloat64);
  const schema::SchemaLineage lineage = schema::SchemaLineage::create(schema_value).value();
  const std::array specs{SnapshotTabletSpec{id<schema::TabletId>(3U), uuid(8U), 10U},
                         SnapshotTabletSpec{id<schema::TabletId>(9U), uuid(10U), 20U}};
  auto snapshot = make_snapshot(directory, lineage, specs);
  ASSERT_TRUE(snapshot.has_value()) << snapshot.error().to_string();
  const DistributedAggregatePlan plan{
      .query_id = uuid(7U),
      .read_policy = {.consistency = DistributedReadConsistency::kLeaderLinearizable},
      .fragments = {{.tablet_id = specs[0].tablet_id,
                     .minimum_event_time = 0,
                     .maximum_event_time = 100,
                     .leader_node = 11U,
                     .local_applied_position = 10U,
                     .known_leader_commit_position = 10U},
                    {.tablet_id = specs[1].tablet_id,
                     .minimum_event_time = 101,
                     .maximum_event_time = 200,
                     .leader_node = 12U,
                     .local_applied_position = 20U,
                     .known_leader_commit_position = 20U}}};
  const std::array placements{
      raft::TabletPlacementMetadata{
          schema_value.table_id(), specs[0].tablet_id, 12U, {11U, 13U}, 11U},
      raft::TabletPlacementMetadata{
          schema_value.table_id(), specs[1].tablet_id, 13U, {12U, 14U}, 12U}};
  const raft::MetadataCatalogSnapshot catalog{
      .applied_index = 30U,
      .schema_definitions = {{"metrics", false,
                              std::make_shared<const schema::TableSchema>(schema_value)}},
      .active_schemas = {{schema_value.table_id(), schema_value.schema_id()}},
      .tablet_placements = {placements.begin(), placements.end()},
      .tablet_group_bindings = {{specs[0].tablet_id, specs[0].group_id},
                                {specs[1].tablet_id, specs[1].group_id}}};
  const std::array observations{raft::RaftGroupObservation{.group_id = specs[0].group_id,
                                                           .node_id = 11U,
                                                           .role = raft::Role::kLeader,
                                                           .current_term = 2U,
                                                           .leader_id = 11U,
                                                           .last_log_index = 10U,
                                                           .commit_index = 10U,
                                                           .applied_index = 10U,
                                                           .voters = {11U, 13U},
                                                           .committed_voters = {11U, 13U}},
                                raft::RaftGroupObservation{.group_id = specs[1].group_id,
                                                           .node_id = 12U,
                                                           .role = raft::Role::kLeader,
                                                           .current_term = 4U,
                                                           .leader_id = 12U,
                                                           .last_log_index = 20U,
                                                           .commit_index = 20U,
                                                           .applied_index = 20U,
                                                           .voters = {12U, 14U},
                                                           .committed_voters = {12U, 14U}}};
  const std::array proofs{
      DistributedAggregateReplicaProof{.observation = std::cref(observations[0]),
                                       .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U}},
      DistributedAggregateReplicaProof{.observation = std::cref(observations[1]),
                                       .linearizable_barrier = raft::ReadBarrier{4U, 5U, 20U}}};
  const std::array group_authorities{
      DistributedAggregateGroupReadAuthority{.barrier = {uuid(1U), raft::ReadBarrier{1U, 1U, 1U}},
                                             .observation = {.group_id = uuid(1U),
                                                             .node_id = 15U,
                                                             .role = raft::Role::kLeader,
                                                             .current_term = 1U,
                                                             .leader_id = 15U,
                                                             .last_log_index = 1U,
                                                             .commit_index = 1U,
                                                             .applied_index = 1U,
                                                             .voters = {15U},
                                                             .committed_voters = {15U}}},
      DistributedAggregateGroupReadAuthority{
          .barrier = {specs[0].group_id, *proofs[0].linearizable_barrier},
          .observation = observations[0]},
      DistributedAggregateGroupReadAuthority{
          .barrier = {specs[1].group_id, *proofs[1].linearizable_barrier},
          .observation = observations[1]}};
  const std::array<std::uint32_t, 2U> projection{0U, 1U};

  auto group_bound =
      bind_group_backed_distributed_aggregate_snapshot(plan, *snapshot,
                                                       {.catalog = std::cref(catalog),
                                                        .table_id = schema_value.table_id(),
                                                        .group_authorities = group_authorities,
                                                        .destination_column_ordinals = projection,
                                                        .aggregate_input_index = 1U});
  ASSERT_TRUE(group_bound.has_value()) << group_bound.error().to_string();
  ASSERT_EQ(group_bound->dispatches().size(), 2U);
  EXPECT_EQ(group_bound->dispatches()[0].raft_group_id, specs[0].group_id);
  EXPECT_EQ(group_bound->dispatches()[1].raft_group_id, specs[1].group_id);
  const std::array reversed_authorities{group_authorities[2], group_authorities[1],
                                        group_authorities[0]};
  EXPECT_EQ(
      bind_group_backed_distributed_aggregate_snapshot(plan, *snapshot,
                                                       {.catalog = std::cref(catalog),
                                                        .table_id = schema_value.table_id(),
                                                        .group_authorities = reversed_authorities,
                                                        .destination_column_ordinals = projection,
                                                        .aggregate_input_index = 1U})
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);
  EXPECT_EQ(bind_group_backed_distributed_aggregate_snapshot(
                plan, *snapshot,
                {.catalog = std::cref(catalog),
                 .table_id = schema_value.table_id(),
                 .group_authorities = std::span{group_authorities}.first(2U),
                 .destination_column_ordinals = projection,
                 .aggregate_input_index = 1U})
                .error()
                .code(),
            common::StatusCode::kUnavailable);

  auto compatible = bind_metadata_backed_distributed_aggregate_snapshot(
      plan, std::move(*snapshot),
      {.catalog = std::cref(catalog),
       .table_id = schema_value.table_id(),
       .replica_proofs = proofs,
       .destination_column_ordinals = projection,
       .aggregate_input_index = 1U});
  ASSERT_TRUE(compatible.has_value()) << compatible.error().to_string();
  ASSERT_EQ(compatible->dispatches().size(), 2U);
  for (std::size_t index = 0U; index < compatible->dispatches().size(); ++index) {
    EXPECT_EQ(compatible->dispatches()[index].raft_group_id, specs[index].group_id);
    EXPECT_EQ(compatible->dispatches()[index].fragment.destination_schema_id,
              schema_value.schema_id());
    EXPECT_EQ(compatible->dispatches()[index].fragment.placement_epoch,
              placements[index].placement_epoch);
    EXPECT_EQ(compatible->dispatches()[index].fragment.linearizable_barrier,
              proofs[index].linearizable_barrier);
  }
}

TEST(DistributedFragmentBindingTest, RejectsStaleOrReconfiguringMetadataBackedProofs) {
  TemporaryDirectory directory;
  const schema::TableSchema schema_value = make_schema(schema::LogicalTypeKind::kFloat64);
  const schema::SchemaLineage lineage = schema::SchemaLineage::create(schema_value).value();
  const common::Uuid group_id = uuid(8U);
  auto snapshot = make_snapshot(directory, lineage, group_id, 10U);
  ASSERT_TRUE(snapshot.has_value()) << snapshot.error().to_string();
  const Authority value = authority();
  raft::MetadataCatalogSnapshot catalog{
      .applied_index = 20U,
      .schema_definitions = {{"metrics", false,
                              std::make_shared<const schema::TableSchema>(schema_value)}},
      .active_schemas = {{schema_value.table_id(), schema_value.schema_id()}},
      .tablet_placements = {value.placement},
      .tablet_group_bindings = {{value.admission.tablet_id, group_id}}};
  raft::RaftGroupObservation observation{.group_id = group_id,
                                         .node_id = 11U,
                                         .role = raft::Role::kLeader,
                                         .current_term = 2U,
                                         .leader_id = 11U,
                                         .last_log_index = 10U,
                                         .commit_index = 10U,
                                         .applied_index = 10U,
                                         .voters = {11U, 12U},
                                         .committed_voters = {11U, 12U}};
  std::array proofs{
      DistributedAggregateReplicaProof{.observation = std::cref(observation),
                                       .linearizable_barrier = raft::ReadBarrier{1U, 3U, 10U}}};
  const std::array<std::uint32_t, 2U> projection{0U, 1U};
  const auto bind = [&] {
    return bind_metadata_backed_distributed_aggregate_snapshot(
        value.plan, *snapshot,
        {.catalog = std::cref(catalog),
         .table_id = schema_value.table_id(),
         .replica_proofs = proofs,
         .destination_column_ordinals = projection,
         .aggregate_input_index = 1U});
  };

  EXPECT_EQ(bind().error().code(), common::StatusCode::kUnavailable);
  proofs[0].linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U};
  observation.joint_membership_active = true;
  observation.joint_old_voters = observation.voters;
  EXPECT_EQ(bind().error().code(), common::StatusCode::kUnavailable);
  observation.joint_membership_active = false;
  observation.joint_old_voters.clear();
  observation.role = static_cast<raft::Role>(0U);
  EXPECT_EQ(bind().error().code(), common::StatusCode::kCorruption);
  observation.role = raft::Role::kLeader;
  catalog.tablet_group_bindings.clear();
  EXPECT_EQ(bind().error().code(), common::StatusCode::kUnavailable);
}

TEST(DistributedFragmentBindingTest, DerivesBoundedStaleAndLocalEventualAdmissions) {
  TemporaryDirectory directory;
  const schema::TableSchema schema_value = make_schema(schema::LogicalTypeKind::kFloat64);
  const schema::SchemaLineage lineage = schema::SchemaLineage::create(schema_value).value();
  const common::Uuid group_id = uuid(8U);
  auto snapshot = make_snapshot(directory, lineage, group_id, 10U);
  ASSERT_TRUE(snapshot.has_value()) << snapshot.error().to_string();
  Authority value = authority();
  const raft::MetadataCatalogSnapshot catalog{
      .applied_index = 20U,
      .schema_definitions = {{"metrics", false,
                              std::make_shared<const schema::TableSchema>(schema_value)}},
      .active_schemas = {{schema_value.table_id(), schema_value.schema_id()}},
      .tablet_placements = {value.placement},
      .tablet_group_bindings = {{value.admission.tablet_id, group_id}}};
  raft::RaftGroupObservation observation{.group_id = group_id,
                                         .node_id = 12U,
                                         .role = raft::Role::kFollower,
                                         .current_term = 2U,
                                         .leader_id = 11U,
                                         .last_log_index = 11U,
                                         .commit_index = 10U,
                                         .applied_index = 10U,
                                         .voters = {11U, 12U},
                                         .committed_voters = {11U, 12U}};
  std::array proofs{DistributedAggregateReplicaProof{.observation = std::cref(observation),
                                                     .observed_leader_commit_position = 11U}};
  const std::array<std::uint32_t, 2U> projection{0U, 1U};
  value.plan.read_policy = {.consistency = DistributedReadConsistency::kFollowerBoundedStale,
                            .maximum_staleness_positions = 2U};

  std::array follower_authorities{DistributedAggregateFollowerReadAuthority{
      .leader_observation = {.group_id = group_id,
                             .node_id = 11U,
                             .role = raft::Role::kLeader,
                             .current_term = 2U,
                             .leader_id = 11U,
                             .last_log_index = 11U,
                             .commit_index = 11U,
                             .applied_index = 11U,
                             .voters = {11U, 12U},
                             .committed_voters = {11U, 12U}},
      .follower_observation = observation}};
  auto correlated = bind_follower_group_backed_distributed_aggregate_snapshot(
      value.plan, *snapshot,
      {.catalog = std::cref(catalog),
       .table_id = schema_value.table_id(),
       .group_authorities = follower_authorities,
       .destination_column_ordinals = projection,
       .aggregate_input_index = 1U});
  ASSERT_TRUE(correlated.has_value()) << correlated.error().to_string();
  EXPECT_EQ(correlated->dispatches().front().fragment.serving_node, 12U);
  EXPECT_EQ(correlated->dispatches().front().fragment.observed_leader_commit_position, 11U);
  follower_authorities[0].leader_observation.current_term = 3U;
  EXPECT_EQ(bind_follower_group_backed_distributed_aggregate_snapshot(
                value.plan, *snapshot,
                {.catalog = std::cref(catalog),
                 .table_id = schema_value.table_id(),
                 .group_authorities = follower_authorities,
                 .destination_column_ordinals = projection,
                 .aggregate_input_index = 1U})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  auto bounded = bind_metadata_backed_distributed_aggregate_snapshot(
      value.plan, *snapshot,
      {.catalog = std::cref(catalog),
       .table_id = schema_value.table_id(),
       .replica_proofs = proofs,
       .destination_column_ordinals = projection,
       .aggregate_input_index = 1U});
  ASSERT_TRUE(bounded.has_value()) << bounded.error().to_string();
  EXPECT_EQ(bounded->dispatches().front().fragment.serving_node, 12U);
  EXPECT_EQ(bounded->dispatches().front().fragment.observed_leader_commit_position, 11U);

  value.plan.read_policy = {.consistency = DistributedReadConsistency::kLocalEventual};
  observation.role = raft::Role::kCandidate;
  observation.leader_id.reset();
  proofs[0].observed_leader_commit_position.reset();
  auto eventual = bind_metadata_backed_distributed_aggregate_snapshot(
      value.plan, *snapshot,
      {.catalog = std::cref(catalog),
       .table_id = schema_value.table_id(),
       .replica_proofs = proofs,
       .destination_column_ordinals = projection,
       .aggregate_input_index = 1U});
  ASSERT_TRUE(eventual.has_value()) << eventual.error().to_string();
  EXPECT_EQ(eventual->dispatches().front().fragment.serving_node, 12U);
  EXPECT_EQ(eventual->dispatches().front().fragment.observed_leader_commit_position, 0U);
  EXPECT_FALSE(eventual->dispatches().front().fragment.linearizable_barrier.has_value());
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
