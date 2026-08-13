#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/manifest/temporal_part_validation.hpp"
#include "chronos/manifest/temporal_validation.hpp"
#include "chronos/query/distributed_fragment_worker.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "cseg/cseg_test_fixture.hpp"

#include <array>
#include <bit>
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
#include <variant>
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
        (std::filesystem::temp_directory_path() / "chronos-fragment-worker-XXXXXX").string();
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

class OmittingPartLoader final : public DistributedTemporalPartBatchLoader {
public:
  common::Status load(const manifest::TemporalDatabaseStorageSnapshot&,
                      std::span<const cseg::PartId>, std::span<const manifest::TabletSchemaBinding>,
                      manifest::TemporalPartValidationLimits,
                      DistributedTemporalPartBatchConsumer&) const override {
    ++calls_;
    return common::Status::ok();
  }

  [[nodiscard]] std::size_t calls() const noexcept {
    return calls_;
  }

private:
  mutable std::size_t calls_{};
};

class Float64RowsConsumer final : public DistributedVectorRowsChunkConsumerV2 {
public:
  common::Status consume(const VectorChunk& chunk) override {
    ++calls;
    if (chunk.column_count() != 1U)
      return {common::StatusCode::kCorruption, "unexpected vector worker test width"};
    for (std::size_t row = 0U; row < chunk.selected_row_count(); ++row) {
      auto cell = chunk.cell({.column_ordinal = 0U, .selected_row = row});
      if (!cell.has_value())
        return cell.error();
      auto bytes = cell->bytes();
      if (!bytes.has_value() || bytes->size() != sizeof(std::uint64_t))
        return {common::StatusCode::kCorruption, "unexpected vector worker test cell"};
      std::uint64_t bits{};
      for (std::size_t index = 0U; index < bytes->size(); ++index) {
        bits |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>((*bytes)[index]))
                << (index * 8U);
      }
      values.push_back(std::bit_cast<double>(bits));
    }
    return common::Status::ok();
  }

  std::size_t calls{};
  std::vector<double> values;
};

[[nodiscard]] std::shared_ptr<const schema::TableSchema> make_schema() {
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

TEST(DistributedFragmentWorkerTest, ExecutesPinnedTemporalPartsAndReprovesLocalPlacement) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  ASSERT_TRUE(std::filesystem::create_directory(directory.path() / manifest::kPartsDirectoryName));
  ASSERT_TRUE(
      std::filesystem::create_directory(directory.path() / manifest::kManifestDirectoryName));
  write_file(directory.path() / manifest::kManifestDirectoryName / manifest::kManifestLockFileName,
             {});

  const auto schema_value = make_schema();
  const schema::SchemaLineage lineage = schema::SchemaLineage::create(*schema_value).value();
  const manifest::DatabaseId database_id = id<manifest::DatabaseId>(1U);
  const schema::TabletId tablet_id = id<schema::TabletId>(3U);
  const common::Uuid group_id = uuid(8U);
  const cseg::EncodedCsegPart encoded_part = cseg::test::make_valid_temporal_float64_part(
      cseg::PageCompression::kNone,
      {.commit_source = cseg::temporal_format::CommitSource::kRaft, .source_id = group_id});
  const auto part = manifest::describe_manifest_v2_temporal_part_image(
      encoded_part.bytes(), *schema_value, tablet_id, manifest::ManifestCommitSource::kRaft,
      group_id);
  ASSERT_TRUE(part.has_value()) << part.error().to_string();
  write_file(directory.path() / manifest::kPartsDirectoryName /
                 manifest::part_file_name(part->part_id),
             encoded_part.bytes());
  const manifest::TemporalTabletDescriptor tablet{
      .table_id = schema_value->table_id(),
      .tablet_id = tablet_id,
      .recovery_schema_id = schema_value->schema_id(),
      .recovery_schema_version = schema_value->version(),
      .source_id = group_id,
      .durable_position = 10U,
      .reclaim_position = 0U,
      .first_part_index = 0U,
      .part_count = 1U,
      .durable_version_count = 2U,
      .commit_source = manifest::ManifestCommitSource::kRaft};
  const std::array tablets{tablet};
  const std::array parts{*part};
  auto encoded = manifest::encode_manifest_v2_temporal({.generation = 1U,
                                                        .database_id = database_id,
                                                        .wal_reclaim_checkpoint = std::nullopt,
                                                        .tablets = tablets,
                                                        .parts = parts,
                                                        .retries = {}});
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  write_file(directory.path() / manifest::kManifestDirectoryName /
                 *manifest::manifest_file_name(1U),
             encoded->bytes());
  auto storage =
      manifest::ManifestStorage::open_existing({.database_root = directory.path().string()});
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
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
  ASSERT_TRUE(loaded.has_value()) << loaded.error().to_string();
  auto selected =
      std::make_shared<const manifest::LoadedTemporalManifestGeneration>(std::move(*loaded));
  auto publisher = manifest::TemporalDatabaseStoragePublisher::create(selected, schema_bindings);
  ASSERT_TRUE(publisher.has_value()) << publisher.error().to_string();
  auto snapshot = publisher->snapshot();
  ASSERT_TRUE(snapshot.has_value()) << snapshot.error().to_string();

  const DistributedAggregateFragmentDispatch dispatch{
      .raft_group_id = group_id,
      .fragment = {.query_id = uuid(7U),
                   .database_id = database_id,
                   .table_id = schema_value->table_id(),
                   .tablet_id = tablet_id,
                   .destination_schema_id = schema_value->schema_id(),
                   .snapshot_generation = 1U,
                   .serving_node = 11U,
                   .applied_position = 10U,
                   .observed_leader_commit_position = 10U,
                   .placement_epoch = 12U,
                   .read_policy = {.consistency = DistributedReadConsistency::kLeaderLinearizable,
                                   .maximum_staleness_positions = std::nullopt},
                   .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
                   .destination_column_ordinals = {0U, 1U},
                   .aggregate_input_index = 1U,
                   .event_time_predicate = cseg::EventTimePredicate{
                       .lower = cseg::EventTimeBound{15, true}, .upper = std::nullopt}}};
  raft::TabletPlacementMetadata placement{.table_id = schema_value->table_id(),
                                          .tablet_id = tablet_id,
                                          .placement_epoch = 12U,
                                          .replicas = {11U, 12U},
                                          .leader_hint = 11U};
  const auto request = [&](const std::uint64_t local_node) {
    return DistributedAggregateWorkerRequest{.dispatch = std::cref(dispatch),
                                             .storage = std::cref(*storage),
                                             .snapshot = std::cref(*snapshot),
                                             .lineage = std::cref(lineage),
                                             .placement = std::cref(placement),
                                             .raft_group_id = group_id,
                                             .local_node = local_node,
                                             .local_linearizable_barrier =
                                                 raft::ReadBarrier{2U, 3U, 10U},
                                             .limits = {}};
  };

  const auto result = execute_distributed_aggregate_fragment(request(11U));
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_EQ(result->query_id, dispatch.fragment.query_id);
  EXPECT_EQ(result->tablet_id, tablet_id);
  EXPECT_EQ(result->sequence, 1U);
  EXPECT_EQ(result->partial.count, 1U);
  EXPECT_EQ(result->partial.sum, 2.5);
  EXPECT_EQ(result->partial.minimum, 2.5);
  EXPECT_EQ(result->partial.maximum, 2.5);
  EXPECT_TRUE(result->terminal);

  DistributedGroupedFloat64FragmentDispatch grouped_dispatch{
      .raft_group_id = group_id,
      .fragment = {.aggregate = dispatch.fragment, .group_key_input_index = 1U}};
  grouped_dispatch.fragment.aggregate.event_time_predicate = std::nullopt;
  const auto grouped_request = [&](const std::uint64_t local_node) {
    return DistributedGroupedFloat64WorkerRequest{.dispatch = std::cref(grouped_dispatch),
                                                  .storage = std::cref(*storage),
                                                  .snapshot = std::cref(*snapshot),
                                                  .lineage = std::cref(lineage),
                                                  .placement = std::cref(placement),
                                                  .raft_group_id = group_id,
                                                  .local_node = local_node,
                                                  .local_linearizable_barrier =
                                                      raft::ReadBarrier{2U, 3U, 10U},
                                                  .limits = {}};
  };
  const auto grouped_result = execute_distributed_grouped_float64_fragment(grouped_request(11U));
  ASSERT_TRUE(grouped_result.has_value()) << grouped_result.error().to_string();
  const auto* messages = std::get_if<std::vector<GroupedFloat64ExchangeMessage>>(&*grouped_result);
  ASSERT_NE(messages, nullptr);
  ASSERT_EQ(messages->size(), 2U);
  EXPECT_EQ((*messages)[0].sequence, 1U);
  EXPECT_EQ((*messages)[0].group_key, 1.5);
  EXPECT_EQ((*messages)[0].partial.count, 1U);
  EXPECT_EQ((*messages)[0].partial.sum, 1.5);
  EXPECT_FALSE((*messages)[0].terminal);
  EXPECT_EQ((*messages)[1].sequence, 2U);
  EXPECT_EQ((*messages)[1].group_key, 2.5);
  EXPECT_EQ((*messages)[1].partial.sum, 2.5);
  EXPECT_TRUE((*messages)[1].terminal);

  grouped_dispatch.fragment.aggregate.event_time_predicate =
      cseg::EventTimePredicate{.lower = cseg::EventTimeBound{30, true}, .upper = std::nullopt};
  const auto empty = execute_distributed_grouped_float64_fragment(grouped_request(11U));
  ASSERT_TRUE(empty.has_value()) << empty.error().to_string();
  const auto* terminal = std::get_if<GroupedExchangeTerminalMessage>(&*empty);
  ASSERT_NE(terminal, nullptr);
  EXPECT_EQ(terminal->query_id, grouped_dispatch.fragment.aggregate.query_id);
  EXPECT_EQ(terminal->tablet_id, tablet_id);
  EXPECT_EQ(terminal->sequence, 1U);

  const DistributedVectorFragmentDispatchV2 vector_dispatch{
      .dispatch = {.query_id = dispatch.fragment.query_id,
                   .database_id = dispatch.fragment.database_id,
                   .table_id = dispatch.fragment.table_id,
                   .tablet_id = dispatch.fragment.tablet_id,
                   .destination_schema_id = dispatch.fragment.destination_schema_id,
                   .raft_group_id = dispatch.raft_group_id,
                   .snapshot_generation = dispatch.fragment.snapshot_generation,
                   .serving_node = dispatch.fragment.serving_node,
                   .applied_position = dispatch.fragment.applied_position,
                   .observed_leader_commit_position =
                       dispatch.fragment.observed_leader_commit_position,
                   .placement_epoch = dispatch.fragment.placement_epoch,
                   .read_policy = dispatch.fragment.read_policy,
                   .linearizable_barrier = dispatch.fragment.linearizable_barrier,
                   .destination_column_ordinals = {0U, 1U},
                   .event_time_predicate = std::nullopt,
                   .plan = {.mode = DistributedVectorPlanMode::kRows,
                            .row_output_indices = {1U},
                            .order_keys = {{.output_index = 0U,
                                            .direction = PhysicalSortDirection::kDescending,
                                            .null_placement = ScalarNullPlacement::kLast}},
                            .limit = 1U}},
      .result_schema = {.columns = {{"value", schema_value->columns()[1].type(), false}}}};
  const auto vector_request = [&](const DistributedVectorFragmentDispatchV2& selected_dispatch,
                                  const std::uint64_t local_node) {
    return DistributedVectorRowsWorkerRequestV2{.dispatch = std::cref(selected_dispatch),
                                                .storage = std::cref(*storage),
                                                .snapshot = std::cref(*snapshot),
                                                .lineage = std::cref(lineage),
                                                .placement = std::cref(placement),
                                                .raft_group_id = group_id,
                                                .local_node = local_node,
                                                .local_linearizable_barrier =
                                                    raft::ReadBarrier{2U, 3U, 10U},
                                                .limits = {}};
  };
  Float64RowsConsumer vector_rows;
  const auto vector_result = execute_distributed_vector_rows_fragment_v2(
      vector_request(vector_dispatch, 11U), vector_rows);
  ASSERT_TRUE(vector_result.has_value()) << vector_result.error().to_string();
  EXPECT_EQ(vector_result->output_rows, 2U);
  EXPECT_EQ(vector_result->output_chunks, 1U);
  EXPECT_EQ(vector_rows.calls, 1U);
  EXPECT_EQ(vector_rows.values, (std::vector<double>{1.5, 2.5}));

  DistributedVectorFragmentDispatchV2 mismatched_schema = vector_dispatch;
  mismatched_schema.result_schema.columns[0].nullable = true;
  Float64RowsConsumer rejected_rows;
  EXPECT_EQ(execute_distributed_vector_rows_fragment_v2(vector_request(mismatched_schema, 11U),
                                                        rejected_rows)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(rejected_rows.calls, 0U);

  DistributedVectorFragmentDispatchV2 aggregate_vector = vector_dispatch;
  aggregate_vector.dispatch.plan = {
      .mode = DistributedVectorPlanMode::kUngroupedAggregate,
      .aggregates = {{.operation = VectorAggregateOperation::kCountStar}}};
  Float64RowsConsumer aggregate_rows;
  OmittingPartLoader aggregate_omitting_loader;
  EXPECT_EQ(execute_distributed_vector_rows_fragment_v2(vector_request(aggregate_vector, 11U),
                                                        aggregate_omitting_loader, aggregate_rows)
                .error()
                .code(),
            common::StatusCode::kNotSupported);
  EXPECT_EQ(aggregate_omitting_loader.calls(), 0U);

  DistributedVectorFragmentDispatchV2 aggregate_dispatch = vector_dispatch;
  aggregate_dispatch.dispatch.event_time_predicate =
      cseg::EventTimePredicate{.lower = cseg::EventTimeBound{15, true}, .upper = std::nullopt};
  aggregate_dispatch.dispatch.plan = {
      .mode = DistributedVectorPlanMode::kUngroupedAggregate,
      .aggregates = {{.operation = VectorAggregateOperation::kCountStar,
                      .input_index = std::nullopt},
                     {.operation = VectorAggregateOperation::kSum, .input_index = 1U},
                     {.operation = VectorAggregateOperation::kAverage, .input_index = 1U},
                     {.operation = VectorAggregateOperation::kMaximum, .input_index = 1U}},
      .order_keys = {{.output_index = 2U,
                      .direction = PhysicalSortDirection::kDescending,
                      .null_placement = ScalarNullPlacement::kLast}},
      .limit = 1U};
  aggregate_dispatch.result_schema = {
      .columns = {
          {"count", schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(), false},
          {"sum", schema_value->columns()[1].type(), true},
          {"average", schema::LogicalType::create(schema::LogicalTypeKind::kFloat64).value(), true},
          {"maximum", schema_value->columns()[1].type(), true}}};
  const auto aggregate_request = [&](const std::uint64_t local_node) {
    return DistributedVectorAggregateWorkerRequestV2{.dispatch = std::cref(aggregate_dispatch),
                                                     .storage = std::cref(*storage),
                                                     .snapshot = std::cref(*snapshot),
                                                     .lineage = std::cref(lineage),
                                                     .placement = std::cref(placement),
                                                     .raft_group_id = group_id,
                                                     .local_node = local_node,
                                                     .local_linearizable_barrier =
                                                         raft::ReadBarrier{2U, 3U, 10U},
                                                     .limits = {}};
  };
  const auto bound_aggregate_definitions =
      bind_distributed_vector_aggregate_worker_definitions_v2(aggregate_request(11U));
  ASSERT_TRUE(bound_aggregate_definitions.has_value())
      << bound_aggregate_definitions.error().to_string();
  ASSERT_EQ(bound_aggregate_definitions->size(), 4U);
  EXPECT_EQ(bind_distributed_vector_aggregate_worker_definitions_v2(aggregate_request(12U))
                .error()
                .code(),
            common::StatusCode::kUnavailable);
  auto aggregate_result = execute_distributed_vector_aggregate_fragment_v2(aggregate_request(11U));
  ASSERT_TRUE(aggregate_result.has_value()) << aggregate_result.error().to_string();
  EXPECT_EQ(aggregate_result->input_rows, 1U);
  EXPECT_EQ(aggregate_result->definitions, *bound_aggregate_definitions);
  ASSERT_EQ(aggregate_result->definitions.size(), 4U);
  ASSERT_EQ(aggregate_result->messages.size(), aggregate_result->definitions.size());
  for (std::size_t ordinal = 0U; ordinal < aggregate_result->messages.size(); ++ordinal) {
    const DistributedVectorAggregateExchangeMessage& message = aggregate_result->messages[ordinal];
    EXPECT_EQ(message.query_id, aggregate_dispatch.dispatch.query_id);
    EXPECT_EQ(message.tablet_id, aggregate_dispatch.dispatch.tablet_id);
    EXPECT_EQ(message.sequence, ordinal + 1U);
    EXPECT_EQ(message.aggregate_ordinal, ordinal);
    EXPECT_EQ(message.terminal, ordinal + 1U == aggregate_result->messages.size());
    EXPECT_TRUE(
        encode_distributed_vector_aggregate_exchange_message(message, aggregate_result->definitions)
            .has_value());
  }
  auto count = std::move(aggregate_result->messages[0].state).take_result();
  auto sum = std::move(aggregate_result->messages[1].state).take_result();
  auto average = std::move(aggregate_result->messages[2].state).take_result();
  auto maximum = std::move(aggregate_result->messages[3].state).take_result();
  ASSERT_TRUE(count.has_value());
  ASSERT_TRUE(sum.has_value());
  ASSERT_TRUE(average.has_value());
  ASSERT_TRUE(maximum.has_value());
  EXPECT_EQ(std::get<std::int64_t>(count->storage()), 1);
  EXPECT_EQ(std::get<double>(sum->storage()), 2.5);
  EXPECT_EQ(std::get<double>(average->storage()), 2.5);
  EXPECT_EQ(std::get<double>(maximum->storage()), 2.5);

  OmittingPartLoader incomplete_aggregate_loader;
  EXPECT_EQ(execute_distributed_vector_aggregate_fragment_v2(aggregate_request(11U),
                                                             incomplete_aggregate_loader)
                .error()
                .code(),
            common::StatusCode::kCorruption);
  EXPECT_EQ(incomplete_aggregate_loader.calls(), 1U);
  OmittingPartLoader unavailable_aggregate_loader;
  EXPECT_EQ(execute_distributed_vector_aggregate_fragment_v2(aggregate_request(12U),
                                                             unavailable_aggregate_loader)
                .error()
                .code(),
            common::StatusCode::kUnavailable);
  EXPECT_EQ(unavailable_aggregate_loader.calls(), 0U);
  auto narrow_aggregate_request = aggregate_request(11U);
  narrow_aggregate_request.limits.maximum_aggregates = 3U;
  EXPECT_EQ(
      execute_distributed_vector_aggregate_fragment_v2(narrow_aggregate_request).error().code(),
      common::StatusCode::kResourceExhausted);

  OmittingPartLoader incomplete_vector_loader;
  Float64RowsConsumer incomplete_rows;
  EXPECT_EQ(execute_distributed_vector_rows_fragment_v2(vector_request(vector_dispatch, 11U),
                                                        incomplete_vector_loader, incomplete_rows)
                .error()
                .code(),
            common::StatusCode::kCorruption);
  EXPECT_EQ(incomplete_vector_loader.calls(), 1U);
  EXPECT_EQ(incomplete_rows.calls, 0U);

  OmittingPartLoader vector_omitting_loader;
  Float64RowsConsumer unavailable_rows;
  EXPECT_EQ(execute_distributed_vector_rows_fragment_v2(vector_request(vector_dispatch, 12U),
                                                        vector_omitting_loader, unavailable_rows)
                .error()
                .code(),
            common::StatusCode::kUnavailable);
  EXPECT_EQ(vector_omitting_loader.calls(), 0U);
  EXPECT_EQ(unavailable_rows.calls, 0U);

  OmittingPartLoader grouped_omitting_loader;
  EXPECT_EQ(
      execute_distributed_grouped_float64_fragment(grouped_request(12U), grouped_omitting_loader)
          .error()
          .code(),
      common::StatusCode::kUnavailable);
  EXPECT_EQ(grouped_omitting_loader.calls(), 0U);

  OmittingPartLoader omitting_loader;
  EXPECT_EQ(execute_distributed_aggregate_fragment(request(11U), omitting_loader).error().code(),
            common::StatusCode::kCorruption);
  EXPECT_EQ(omitting_loader.calls(), 1U);
  EXPECT_EQ(execute_distributed_aggregate_fragment(request(12U), omitting_loader).error().code(),
            common::StatusCode::kUnavailable);
  EXPECT_EQ(omitting_loader.calls(), 1U);

  auto stale_barrier = request(11U);
  stale_barrier.local_linearizable_barrier = raft::ReadBarrier{2U, 4U, 10U};
  EXPECT_EQ(execute_distributed_aggregate_fragment(stale_barrier).error().code(),
            common::StatusCode::kUnavailable);
  EXPECT_EQ(execute_distributed_aggregate_fragment(request(12U)).error().code(),
            common::StatusCode::kUnavailable);
  placement.placement_epoch = 13U;
  EXPECT_EQ(execute_distributed_aggregate_fragment(request(11U)).error().code(),
            common::StatusCode::kUnavailable);
}

} // namespace
} // namespace chronos::query
