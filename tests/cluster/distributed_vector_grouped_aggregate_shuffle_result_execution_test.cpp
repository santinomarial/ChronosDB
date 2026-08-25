#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_execution.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_source_plan.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

[[nodiscard]] schema::TabletId tablet() {
  return schema::TabletId::from_uuid(uuid(2U)).value();
}

[[nodiscard]] schema::LogicalType string_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
}

[[nodiscard]] std::vector<query::VectorGroupKeyDefinition> keys() {
  return {{.column_ordinal = 0U, .type = string_type(), .nullable = false}};
}

[[nodiscard]] std::vector<query::VectorAggregateDefinition> aggregates() {
  return {{.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleAuthority authority() {
  return DistributedVectorGroupedAggregateShuffleAuthority::create(
             uuid(1U), {{.tablet_id = tablet(), .node_id = 2U}},
             {{.partition_id = 0U, .node_id = 2U}, {.partition_id = 1U, .node_id = 2U}}, keys(),
             aggregates())
      .value();
}

[[nodiscard]] std::string label_for_partition(const std::uint32_t partition) {
  const auto expected_keys = keys();
  for (std::size_t suffix = 0U;; ++suffix) {
    std::string label = "result-key-" + std::to_string(suffix);
    std::vector<query::ScalarValue> values;
    values.push_back(query::ScalarValue::text(string_type(), label).value());
    if (query::canonical_vector_group_key_hash_v1(expected_keys, values).value() % 2U == partition)
      return label;
  }
}

[[nodiscard]] std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> input() {
  std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> result;
  for (std::uint32_t ordinal = 0U; ordinal < 2U; ++ordinal) {
    auto state = query::MergeableVectorAggregateState::create(aggregates().front()).value();
    EXPECT_TRUE(state.accumulate_count_star().has_value());
    std::vector<query::ScalarValue> values;
    values.push_back(query::ScalarValue::text(string_type(), label_for_partition(ordinal)).value());
    std::vector<query::MergeableVectorAggregateState> states;
    states.push_back(std::move(state));
    result.push_back(query::encode_distributed_vector_grouped_aggregate_exchange_message(
                         {.query_id = uuid(1U),
                          .tablet_id = tablet(),
                          .sequence = static_cast<std::uint64_t>(ordinal) + 1U,
                          .group_ordinal = ordinal,
                          .group_count = 2U,
                          .terminal = ordinal == 1U,
                          .empty = false},
                         values, states, keys(), aggregates())
                         .value());
  }
  return result;
}

[[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleDestinationExecution>
complete_destination(const DistributedVectorGroupedAggregateShuffleAuthority& expected,
                     const query::QueryResourceContext& resources) {
  auto destination = DistributedVectorGroupedAggregateShuffleDestinationExecution::start(
      expected, {.local_node_id = 2U});
  if (!destination.has_value())
    return common::make_unexpected(destination.error());
  const auto messages = input();
  auto plan = DistributedVectorGroupedAggregateShuffleSourcePlan::create(expected, tablet(),
                                                                         messages, resources);
  if (!plan.has_value())
    return common::make_unexpected(plan.error());
  auto streams = plan->take_local_streams();
  for (auto stream = streams.rbegin(); stream != streams.rend(); ++stream) {
    const common::Status accepted = destination->accept_local_stream(*stream);
    if (!accepted.is_ok())
      return common::make_unexpected(accepted);
  }
  return destination;
}

[[nodiscard]] query::ScalarValue cell(const query::VectorChunk& chunk, const std::size_t column) {
  const columnar::PhysicalColumnView* physical = chunk.column(column);
  EXPECT_NE(physical, nullptr);
  return query::ScalarValue::from_column_cell(
             physical->type(), chunk.cell({.column_ordinal = column, .selected_row = 0U}).value())
      .value();
}

TEST(DistributedVectorGroupedAggregateShuffleResultExecutionTest,
     OwnsSealedDestinationsAndEmitsCanonicalPartitionOrder) {
  auto expected = authority();
  auto resources = query::QueryResourceContext::create(16U << 20U).value();
  auto destination = complete_destination(expected, resources);
  ASSERT_TRUE(destination.has_value()) << destination.error().to_string();
  ASSERT_EQ(destination->state(),
            DistributedVectorGroupedAggregateShuffleDestinationExecutionState::kComplete);
  std::vector<DistributedVectorGroupedAggregateShuffleDestinationExecution> destinations;
  destinations.push_back(std::move(*destination));
  auto result = DistributedVectorGroupedAggregateShuffleResultExecution::create(
      expected, std::move(destinations));
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_TRUE(result->output_resources().has_value());

  for (std::uint32_t partition = 0U; partition < 2U; ++partition) {
    auto step = result->next();
    ASSERT_TRUE(step.has_value()) << step.error().to_string();
    ASSERT_EQ(step->kind(), query::PhysicalOperatorStepKind::kChunk);
    EXPECT_EQ(std::get<std::string>(cell(step->chunk()->chunk(), 0U).storage()),
              label_for_partition(partition));
    EXPECT_EQ(std::get<std::int64_t>(cell(step->chunk()->chunk(), 1U).storage()), 1);
  }
  EXPECT_EQ(result->next()->kind(), query::PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ(result->next()->kind(), query::PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ(result->metrics().total_partitions, 2U);
  EXPECT_EQ(result->metrics().completed_partitions, 2U);
  EXPECT_EQ(result->metrics().emitted_chunks, 2U);
}

TEST(DistributedVectorGroupedAggregateShuffleResultExecutionTest,
     RejectsMissingConsumedAndInvalidWorkingAuthority) {
  auto expected = authority();
  auto resources = query::QueryResourceContext::create(16U << 20U).value();
  EXPECT_EQ(
      DistributedVectorGroupedAggregateShuffleResultExecution::create(expected, {}).error().code(),
      common::StatusCode::kInvalidArgument);
  auto consumed = complete_destination(expected, resources);
  ASSERT_TRUE(consumed.has_value());
  EXPECT_TRUE(consumed->next(0U).has_value());
  std::vector<DistributedVectorGroupedAggregateShuffleDestinationExecution> destinations;
  destinations.push_back(std::move(*consumed));
  EXPECT_EQ(DistributedVectorGroupedAggregateShuffleResultExecution::create(expected,
                                                                            std::move(destinations))
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  auto complete = complete_destination(expected, resources);
  ASSERT_TRUE(complete.has_value());
  destinations.clear();
  destinations.push_back(std::move(*complete));
  EXPECT_EQ(DistributedVectorGroupedAggregateShuffleResultExecution::create(
                expected, std::move(destinations), 0U)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::cluster
