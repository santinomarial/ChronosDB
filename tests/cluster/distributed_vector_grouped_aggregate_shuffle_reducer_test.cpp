#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_reducer.hpp"

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
             uuid(1U),
             {{.tablet_id = tablet(2U), .node_id = 2U}, {.tablet_id = tablet(3U), .node_id = 4U}},
             {{.partition_id = 0U, .node_id = 3U}}, keys(), aggregates())
      .value();
}

[[nodiscard]] query::DistributedVectorGroupedAggregateExchangeMessage
message(const schema::TabletId& tablet_id, const std::size_t count) {
  auto state = query::MergeableVectorAggregateState::create(aggregates().front()).value();
  for (std::size_t row = 0U; row < count; ++row)
    EXPECT_TRUE(state.accumulate_count_star().has_value());
  std::vector<query::ScalarValue> values;
  values.push_back(query::ScalarValue::text(string_type(), "shared-key").value());
  std::vector<query::MergeableVectorAggregateState> states;
  states.push_back(std::move(state));
  return {{.query_id = uuid(1U),
           .tablet_id = tablet_id,
           .sequence = 1U,
           .group_ordinal = 0U,
           .group_count = 1U,
           .terminal = true,
           .empty = false},
          std::move(values),
          std::move(states)};
}

struct StreamInput {
  schema::TabletId tablet_id;
  raft::NodeId source_node{};
  std::size_t count{};
};

[[nodiscard]] DistributedVectorGroupedAggregateShuffleCompleteStream
stream(const DistributedVectorGroupedAggregateShuffleAuthority& expected,
       const StreamInput& input) {
  std::vector<query::DistributedVectorGroupedAggregateExchangeMessage> messages;
  messages.push_back(message(input.tablet_id, input.count));
  auto nested = query::encode_distributed_vector_grouped_aggregate_exchange_message(
                    messages.front(), expected.key_definitions(), expected.aggregate_definitions())
                    .value();
  return {.edge = {.tablet_id = input.tablet_id,
                   .partition_id = 0U,
                   .source_node_id = input.source_node,
                   .target_node_id = 3U,
                   .hash_version = expected.hash_version()},
          .messages = std::move(messages),
          .encoded_bytes = kDistributedVectorGroupedAggregateShuffleFrameV1HeaderSize +
                           nested.bytes().size() +
                           kDistributedVectorGroupedAggregateShuffleFrameV1TrailerSize};
}

[[nodiscard]] query::ScalarValue cell(const query::VectorChunk& chunk, const std::size_t column) {
  const columnar::PhysicalColumnView* physical = chunk.column(column);
  EXPECT_NE(physical, nullptr);
  return query::ScalarValue::from_column_cell(
             physical->type(), chunk.cell({.column_ordinal = column, .selected_row = 0U}).value())
      .value();
}

TEST(DistributedVectorGroupedAggregateShuffleReducerTest,
     SuppressesExactRetriesAndMergesEverySourceInAuthorityOrder) {
  auto expected = authority();
  auto reducer = DistributedVectorGroupedAggregateShuffleReducer::create(expected, 0U, 3U);
  ASSERT_TRUE(reducer.has_value()) << reducer.error().to_string();
  auto second = stream(expected, {.tablet_id = tablet(3U), .source_node = 4U, .count = 2U});
  auto first = stream(expected, {.tablet_id = tablet(2U), .source_node = 2U, .count = 1U});
  EXPECT_TRUE(reducer->accept_stream(second).is_ok());
  EXPECT_TRUE(reducer->accept_stream(second).is_ok());
  EXPECT_EQ(reducer->finish().code(), common::StatusCode::kUnavailable);
  EXPECT_TRUE(reducer->accept_stream(first).is_ok());
  EXPECT_EQ(reducer->metrics().accepted_sources, 2U);
  EXPECT_EQ(reducer->metrics().duplicate_streams, 1U);
  EXPECT_GT(reducer->metrics().retained_stream_bytes, 0U);
  EXPECT_TRUE(reducer->finish().is_ok());
  EXPECT_TRUE(reducer->ready());

  auto output = reducer->next();
  ASSERT_TRUE(output.has_value()) << output.error().to_string();
  ASSERT_EQ(output->kind(), query::PhysicalOperatorStepKind::kChunk);
  const query::VectorChunk& chunk = output->chunk()->chunk();
  EXPECT_EQ(std::get<std::string>(cell(chunk, 0U).storage()), "shared-key");
  EXPECT_EQ(std::get<std::int64_t>(cell(chunk, 1U).storage()), 3);
  EXPECT_EQ(reducer->next()->kind(), query::PhysicalOperatorStepKind::kEnd);
  EXPECT_TRUE(reducer->accept_stream(first).is_ok());
  EXPECT_EQ(reducer->metrics().duplicate_streams, 2U);
}

TEST(DistributedVectorGroupedAggregateShuffleReducerTest,
     RejectsConflictingRetryWrongRouteAndNoncanonicalExtent) {
  auto expected = authority();
  auto reducer = DistributedVectorGroupedAggregateShuffleReducer::create(expected, 0U, 3U).value();
  auto accepted = stream(expected, {.tablet_id = tablet(2U), .source_node = 2U, .count = 1U});
  EXPECT_TRUE(reducer.accept_stream(accepted).is_ok());
  auto conflicting = stream(expected, {.tablet_id = tablet(2U), .source_node = 2U, .count = 9U});
  EXPECT_EQ(reducer.accept_stream(conflicting).code(), common::StatusCode::kAlreadyExists);
  EXPECT_EQ(reducer.metrics().duplicate_streams, 0U);

  auto wrong_route = stream(expected, {.tablet_id = tablet(3U), .source_node = 4U, .count = 1U});
  wrong_route.edge.target_node_id = 5U;
  EXPECT_EQ(reducer.accept_stream(wrong_route).code(), common::StatusCode::kInvalidArgument);
  auto wrong_extent = stream(expected, {.tablet_id = tablet(3U), .source_node = 4U, .count = 1U});
  ++wrong_extent.encoded_bytes;
  EXPECT_EQ(reducer.accept_stream(wrong_extent).code(), common::StatusCode::kInvalidArgument);

  DistributedVectorGroupedAggregateShuffleReducerLimits invalid_limits;
  invalid_limits.maximum_total_stream_bytes = 1U;
  EXPECT_EQ(
      DistributedVectorGroupedAggregateShuffleReducer::create(expected, 0U, 3U, invalid_limits)
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);
  EXPECT_EQ(
      DistributedVectorGroupedAggregateShuffleReducer::create(expected, 0U, 4U).error().code(),
      common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::cluster
