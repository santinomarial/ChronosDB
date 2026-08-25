#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_reducer.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_source_plan.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
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

[[nodiscard]] schema::TabletId tablet(const std::uint8_t seed) {
  return schema::TabletId::from_uuid(uuid(seed)).value();
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
             uuid(1U), {{.tablet_id = tablet(2U), .node_id = 2U}},
             {{.partition_id = 0U, .node_id = 2U}, {.partition_id = 1U, .node_id = 3U}}, keys(),
             aggregates())
      .value();
}

[[nodiscard]] std::string label_for_partition(const std::uint32_t partition) {
  const auto expected_keys = keys();
  for (std::size_t suffix = 0U;; ++suffix) {
    std::string label = "source-key-" + std::to_string(suffix);
    std::vector<query::ScalarValue> values;
    values.push_back(query::ScalarValue::text(string_type(), label).value());
    if (query::canonical_vector_group_key_hash_v1(expected_keys, values).value() % 2U == partition)
      return label;
  }
}

[[nodiscard]] std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage>
input(const common::Uuid query_id = uuid(1U)) {
  const auto expected_keys = keys();
  const auto expected_aggregates = aggregates();
  std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> result;
  for (std::uint32_t ordinal = 0U; ordinal < 2U; ++ordinal) {
    std::vector<query::ScalarValue> values;
    values.push_back(query::ScalarValue::text(string_type(), label_for_partition(ordinal)).value());
    std::vector<query::MergeableVectorAggregateState> states;
    states.push_back(
        query::MergeableVectorAggregateState::create(expected_aggregates.front()).value());
    EXPECT_TRUE(states.front().accumulate_count_star().has_value());
    result.push_back(query::encode_distributed_vector_grouped_aggregate_exchange_message(
                         {.query_id = query_id,
                          .tablet_id = tablet(2U),
                          .sequence = static_cast<std::uint64_t>(ordinal) + 1U,
                          .group_ordinal = ordinal,
                          .group_count = 2U,
                          .terminal = ordinal == 1U,
                          .empty = false},
                         values, states, expected_keys, expected_aggregates)
                         .value());
  }
  return result;
}

[[nodiscard]] query::ScalarValue cell(const query::VectorChunk& chunk, const std::size_t column) {
  const columnar::PhysicalColumnView* physical = chunk.column(column);
  EXPECT_NE(physical, nullptr);
  return query::ScalarValue::from_column_cell(
             physical->type(), chunk.cell({.column_ordinal = column, .selected_row = 0U}).value())
      .value();
}

TEST(DistributedVectorGroupedAggregateShuffleSourcePlanTest,
     AtomicallyCreatesLocalDeliveryAndRemoteRetryForEveryPartition) {
  auto expected = authority();
  auto resources = query::QueryResourceContext::create(16U << 20U).value();
  const auto messages = input();
  auto plan = DistributedVectorGroupedAggregateShuffleSourcePlan::create(expected, tablet(2U),
                                                                         messages, resources);
  ASSERT_TRUE(plan.has_value()) << plan.error().to_string();
  EXPECT_EQ(plan->tablet_id(), tablet(2U));
  EXPECT_EQ(plan->source_node_id(), 2U);
  ASSERT_EQ(plan->local_streams().size(), 1U);
  ASSERT_EQ(plan->remote_retries().size(), 1U);
  EXPECT_EQ(plan->local_streams()[0].edge.partition_id, 0U);
  EXPECT_EQ(plan->local_streams()[0].edge.source_node_id, 2U);
  EXPECT_EQ(plan->local_streams()[0].edge.target_node_id, 2U);
  EXPECT_EQ(plan->remote_retries()[0].edge().partition_id, 1U);
  EXPECT_EQ(plan->remote_retries()[0].edge().target_node_id, 3U);
  EXPECT_EQ(plan->metrics().local_edges, 1U);
  EXPECT_EQ(plan->metrics().remote_edges, 1U);
  EXPECT_GT(plan->metrics().nested_encoded_bytes, 0U);
  EXPECT_GT(plan->metrics().outer_encoded_bytes, plan->metrics().nested_encoded_bytes);

  auto local = plan->take_local_streams();
  auto remote = plan->take_remote_retries();
  EXPECT_TRUE(plan->local_streams().empty());
  EXPECT_TRUE(plan->remote_retries().empty());
  ASSERT_EQ(local.size(), 1U);
  ASSERT_EQ(remote.size(), 1U);

  auto reducer = DistributedVectorGroupedAggregateShuffleReducer::create(expected, 0U, 2U).value();
  EXPECT_TRUE(reducer.accept_stream(local.front()).is_ok());
  EXPECT_TRUE(reducer.finish().is_ok());
  auto output = reducer.next();
  ASSERT_TRUE(output.has_value()) << output.error().to_string();
  ASSERT_EQ(output->kind(), query::PhysicalOperatorStepKind::kChunk);
  EXPECT_EQ(std::get<std::string>(cell(output->chunk()->chunk(), 0U).storage()),
            label_for_partition(0U));
  EXPECT_EQ(std::get<std::int64_t>(cell(output->chunk()->chunk(), 1U).storage()), 1);

  auto attempt =
      remote.front().begin_attempt(DistributedVectorGroupedAggregateShuffleRetry::TimePoint{});
  ASSERT_TRUE(attempt.has_value()) << attempt.error().to_string();
  EXPECT_EQ(attempt->target_node_id, 3U);
  EXPECT_EQ(attempt->stream.frame_count(), 1U);
  EXPECT_GT(attempt->stream.encoded_bytes(), 0U);
}

TEST(DistributedVectorGroupedAggregateShuffleSourcePlanTest,
     RejectsUnknownSourceAndPayloadAuthorityDriftWithoutPublishingEdges) {
  auto expected = authority();
  auto resources = query::QueryResourceContext::create(16U << 20U).value();
  const auto messages = input();
  EXPECT_EQ(DistributedVectorGroupedAggregateShuffleSourcePlan::create(expected, tablet(8U),
                                                                       messages, resources)
                .error()
                .code(),
            common::StatusCode::kNotFound);

  const auto wrong_query = input(uuid(9U));
  EXPECT_EQ(DistributedVectorGroupedAggregateShuffleSourcePlan::create(expected, tablet(2U),
                                                                       wrong_query, resources)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  DistributedVectorGroupedAggregateShuffleSourcePlanLimits limits;
  limits.maximum_total_outer_encoded_bytes = 1U;
  EXPECT_EQ(DistributedVectorGroupedAggregateShuffleSourcePlan::create(expected, tablet(2U),
                                                                       messages, resources, limits)
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
  limits.maximum_total_outer_encoded_bytes =
      kMaximumDistributedVectorGroupedAggregateShuffleSourcePlanOuterBytes + 1U;
  EXPECT_EQ(DistributedVectorGroupedAggregateShuffleSourcePlan::create(expected, tablet(2U),
                                                                       messages, resources, limits)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::cluster
