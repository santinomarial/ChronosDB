#include "chronos/cluster/distributed_vector_grouped_aggregate_finalization_v2.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_collected_result_execution.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

template <typename Id> [[nodiscard]] Id id(const std::uint8_t seed) {
  return Id::from_uuid(uuid(seed)).value();
}

[[nodiscard]] schema::LogicalType string_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
}

[[nodiscard]] schema::LogicalType i64_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleAuthority authority() {
  const auto tablet = schema::TabletId::from_uuid(uuid(2U)).value();
  return DistributedVectorGroupedAggregateShuffleAuthority::create(
             uuid(1U), {{tablet, 2U}}, {{0U, 3U}, {1U, 4U}}, {{0U, string_type(), false}},
             {{query::VectorAggregateOperation::kCountStar, std::nullopt}})
      .value();
}

[[nodiscard]] query::DistributedVectorResultSchema result_schema() {
  return {.columns = {{"region", string_type(), false}, {"count", i64_type(), false}}};
}

[[nodiscard]] query::DistributedMutableVectorFragment fragment(const std::uint8_t tablet_seed,
                                                               const raft::NodeId node_id) {
  return {.query_id = uuid(1U),
          .database_id = id<manifest::DatabaseId>(8U),
          .table_id = id<schema::TableId>(9U),
          .tablet_id = id<schema::TabletId>(tablet_seed),
          .destination_schema_id = id<schema::SchemaId>(10U),
          .raft_group_id = uuid(static_cast<std::uint8_t>(tablet_seed + 20U)),
          .serving_node = node_id,
          .applied_position = 10U,
          .observed_leader_commit_position = 10U,
          .placement_epoch = 3U,
          .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
          .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
          .destination_column_ordinals = {0U},
          .plan = {.mode = query::DistributedVectorPlanMode::kGroupedAggregate,
                   .group_key_input_indices = {0U},
                   .aggregates = {{.operation = query::VectorAggregateOperation::kCountStar}},
                   .order_keys = {{.output_index = 1U,
                                   .direction = query::PhysicalSortDirection::kDescending}},
                   .limit = 1U},
          .result_schema = result_schema()};
}

[[nodiscard]] std::array<std::byte, sizeof(std::uint64_t)> encoded_u64(std::uint64_t value) {
  std::array<std::byte, sizeof(std::uint64_t)> bytes{};
  for (std::size_t index = 0U; index < bytes.size(); ++index) {
    bytes[index] = static_cast<std::byte>((value >> (index * 8U)) & std::uint64_t{0xffU});
  }
  return bytes;
}

[[nodiscard]] std::vector<std::byte> batch(const std::string& value, const std::uint64_t count) {
  const std::array columns{network::QueryResultColumn{"region", string_type(), false},
                           network::QueryResultColumn{"count", i64_type(), false}};
  const auto encoded_count = encoded_u64(count);
  const std::array cells{network::QueryResultCell{.value = std::as_bytes(std::span{value})},
                         network::QueryResultCell{.value = encoded_count}};
  return network::encode_query_result_batch(1U, columns, cells).value();
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleCompleteResultStream
complete(const DistributedVectorGroupedAggregateShuffleAuthority& expected,
         const query::DistributedVectorResultSchema& schema, const std::uint32_t partition_id,
         const std::string& value, const std::uint64_t count) {
  const auto source = expected.destination_node(partition_id).value();
  std::vector<std::vector<std::byte>> batches;
  if (!value.empty())
    batches.push_back(batch(value, count));
  auto sender = DistributedVectorGroupedAggregateShuffleResultStreamSender::create(
                    expected, schema, partition_id, source, 9U, batches)
                    .value();
  return {.query_id = expected.query_id(),
          .source_node_id = source,
          .target_node_id = 9U,
          .partition_id = partition_id,
          .encoded_result_batches = std::move(batches),
          .frame_count = static_cast<std::uint32_t>(sender.frame_count()),
          .encoded_bytes = sender.encoded_bytes()};
}

[[nodiscard]] std::vector<DistributedVectorGroupedAggregateShuffleCompleteResultStream>
streams(const DistributedVectorGroupedAggregateShuffleAuthority& expected,
        const query::DistributedVectorResultSchema& schema) {
  std::vector<DistributedVectorGroupedAggregateShuffleCompleteResultStream> result;
  result.push_back(complete(expected, schema, 0U, "east", 1U));
  result.push_back(complete(expected, schema, 1U, "west", 2U));
  return result;
}

TEST(DistributedVectorGroupedAggregateShuffleCollectedResultExecutionTest,
     MaterializesCanonicalPartitionsIntoAccountedChunks) {
  auto expected = authority();
  auto schema = result_schema();
  auto execution = DistributedVectorGroupedAggregateShuffleCollectedResultExecution::create(
      expected, schema, streams(expected, schema));
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();
  EXPECT_EQ(execution->authority(), &expected);
  ASSERT_TRUE(execution->output_resources().has_value());
  EXPECT_EQ(execution->key_definitions().size(), 1U);
  EXPECT_EQ(execution->aggregate_definitions().size(), 1U);

  for (const std::string wanted : {"east", "west"}) {
    auto step = execution->next();
    ASSERT_TRUE(step.has_value()) << step.error().to_string();
    ASSERT_EQ(step->kind(), query::PhysicalOperatorStepKind::kChunk);
    const auto* chunk = step->chunk();
    ASSERT_NE(chunk, nullptr);
    ASSERT_EQ(chunk->chunk().selected_row_count(), 1U);
    auto cell = chunk->chunk().cell({.column_ordinal = 0U, .selected_row = 0U});
    ASSERT_TRUE(cell.has_value());
    auto bytes = cell->bytes();
    ASSERT_TRUE(bytes.has_value());
    const auto wanted_bytes = std::as_bytes(std::span{wanted});
    EXPECT_TRUE(std::equal(bytes->begin(), bytes->end(), wanted_bytes.begin(), wanted_bytes.end()));
  }
  auto end = execution->next();
  ASSERT_TRUE(end.has_value());
  EXPECT_EQ(end->kind(), query::PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ(execution->next()->kind(), query::PhysicalOperatorStepKind::kEnd);
  const auto metrics = execution->metrics();
  EXPECT_EQ(metrics.total_partitions, 2U);
  EXPECT_EQ(metrics.completed_partitions, 2U);
  EXPECT_EQ(metrics.decoded_batches, 2U);
  EXPECT_EQ(metrics.decoded_rows, 2U);
  EXPECT_GT(metrics.decoded_batch_bytes, 0U);
}

TEST(DistributedVectorGroupedAggregateShuffleCollectedResultExecutionTest,
     RejectsCoverageSchemaAndStickyWorkingMemoryFailure) {
  auto expected = authority();
  auto schema = result_schema();
  auto reversed = streams(expected, schema);
  std::swap(reversed[0], reversed[1]);
  EXPECT_EQ(DistributedVectorGroupedAggregateShuffleCollectedResultExecution::create(
                expected, schema, std::move(reversed))
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  auto wrong_schema = schema;
  wrong_schema.columns.pop_back();
  EXPECT_EQ(DistributedVectorGroupedAggregateShuffleCollectedResultExecution::create(
                expected, wrong_schema, streams(expected, schema))
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  DistributedVectorGroupedAggregateShuffleCollectedResultExecutionLimits limits;
  limits.maximum_batch_working_bytes = 1U;
  auto execution = DistributedVectorGroupedAggregateShuffleCollectedResultExecution::create(
      expected, schema, streams(expected, schema), limits);
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();
  auto failed = execution->next();
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_EQ(execution->next().error(), failed.error());
}

TEST(DistributedVectorGroupedAggregateShuffleCollectedResultExecutionTest,
     RunsFragmentAuthorizedGlobalOrderAndLimitAcrossRemotePartitions) {
  std::vector fragments{fragment(2U, 3U), fragment(3U, 4U)};
  const std::array keys{query::VectorGroupKeyDefinition{0U, string_type(), false}};
  const std::array aggregates{
      query::VectorAggregateDefinition{query::VectorAggregateOperation::kCountStar, std::nullopt}};
  auto expected = DistributedVectorGroupedAggregateShuffleAuthority::create_from_mutable_fragments(
      fragments, keys, aggregates);
  ASSERT_TRUE(expected.has_value()) << expected.error().to_string();
  auto finalization =
      DistributedVectorGroupedAggregateShuffleFinalizationAuthorityV2::create(*expected, fragments);
  ASSERT_TRUE(finalization.has_value()) << finalization.error().to_string();

  auto copied_schema = finalization->result_schema();
  auto wrongly_borrowed = DistributedVectorGroupedAggregateShuffleCollectedResultExecution::create(
      *expected, copied_schema, streams(*expected, copied_schema));
  ASSERT_TRUE(wrongly_borrowed.has_value()) << wrongly_borrowed.error().to_string();
  EXPECT_EQ(
      finalize_distributed_vector_grouped_aggregate_shuffle_v2(*wrongly_borrowed, *finalization)
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);

  auto execution = DistributedVectorGroupedAggregateShuffleCollectedResultExecution::create(
      *expected, finalization->result_schema(), streams(*expected, finalization->result_schema()));
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();
  EXPECT_EQ(execution->result_schema(), &finalization->result_schema());
  auto finalized =
      finalize_distributed_vector_grouped_aggregate_shuffle_v2(*execution, *finalization);
  ASSERT_TRUE(finalized.has_value()) << finalized.error().to_string();
  ASSERT_EQ(finalized->row_count, 1U);
  ASSERT_EQ(finalized->encoded_batches.size(), 1U);
  auto decoded = network::decode_query_result_batch(finalized->encoded_batches.front());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  ASSERT_EQ(decoded->row_count(), 1U);
  const auto* region = decoded->cell(0U, 0U);
  const auto* count = decoded->cell(0U, 1U);
  ASSERT_NE(region, nullptr);
  ASSERT_NE(count, nullptr);
  const std::string wanted{"west"};
  const auto wanted_bytes = std::as_bytes(std::span{wanted});
  EXPECT_TRUE(std::equal(region->value.begin(), region->value.end(), wanted_bytes.begin(),
                         wanted_bytes.end()));
  ASSERT_EQ(count->value.size(), sizeof(std::int64_t));
  EXPECT_EQ(count->value.front(), std::byte{2U});
}

} // namespace
} // namespace chronos::cluster
