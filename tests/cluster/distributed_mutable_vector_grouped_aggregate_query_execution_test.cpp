#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_query_execution.hpp"

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
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

template <typename Id> [[nodiscard]] Id id(const std::uint8_t seed) {
  return Id::from_uuid(uuid(seed)).value();
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

[[nodiscard]] query::DistributedMutableVectorFragment fragment(const std::uint8_t tablet_seed,
                                                               const raft::NodeId node) {
  const auto int64 = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  return {
      .query_id = uuid(1U),
      .database_id = id<manifest::DatabaseId>(2U),
      .table_id = id<schema::TableId>(3U),
      .tablet_id = id<schema::TabletId>(tablet_seed),
      .destination_schema_id = id<schema::SchemaId>(5U),
      .raft_group_id = uuid(tablet_seed + 10U),
      .serving_node = node,
      .applied_position = 10U,
      .observed_leader_commit_position = 10U,
      .placement_epoch = 8U,
      .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
      .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
      .destination_column_ordinals = {0U},
      .plan = {.mode = query::DistributedVectorPlanMode::kGroupedAggregate,
               .group_key_input_indices = {0U},
               .aggregates = {{.operation = query::VectorAggregateOperation::kCountStar}}},
      .result_schema = {.columns = {{"region", string_type(), false}, {"count", int64, false}}}};
}

[[nodiscard]] DistributedVectorGroupedAggregateQueryResponseV2
response(const query::DistributedMutableVectorFragment& fragment, const std::size_t count) {
  auto definitions = aggregates();
  auto state = query::MergeableVectorAggregateState::create(definitions.front()).value();
  for (std::size_t index = 0U; index < count; ++index)
    EXPECT_TRUE(state.accumulate_count_star().has_value());
  std::vector<query::ScalarValue> key_values;
  key_values.push_back(query::ScalarValue::text(string_type(), "east").value());
  std::vector<query::MergeableVectorAggregateState> states;
  states.push_back(std::move(state));
  return {.source_node_id = fragment.serving_node,
          .target_node_id = 1U,
          .query_id = fragment.query_id,
          .tablet_id = fragment.tablet_id,
          .status_code = common::StatusCode::kOk,
          .payload = query::DistributedVectorGroupedAggregateExchangeMessage{
              {.query_id = fragment.query_id,
               .tablet_id = fragment.tablet_id,
               .sequence = 1U,
               .group_ordinal = 0U,
               .group_count = 1U,
               .terminal = true,
               .empty = false},
              std::move(key_values),
              std::move(states)}};
}

TEST(DistributedMutableVectorGroupedAggregateQueryExecutionTest,
     OwnsExactSendersAndPublishesOnlyAfterAllTabletClosure) {
  std::vector fragments{fragment(4U, 11U), fragment(6U, 12U)};
  const auto first_fragment = fragments[0];
  const auto second_fragment = fragments[1];
  auto execution = DistributedMutableVectorGroupedAggregateQueryExecution::create(
      1U, std::move(fragments), keys(), aggregates(),
      {.coordinator = {.messages = {.maximum_messages_per_fragment = 1U,
                                    .maximum_total_messages = 2U},
                       .maximum_total_encoded_bytes = 1U << 20U},
       .maximum_decode_memory_bytes = 1U << 20U});
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();
  ASSERT_EQ(execution->targets().size(), 2U);
  EXPECT_EQ(execution->targets()[0].serving_node, 11U);
  ASSERT_EQ(execution->key_definitions().size(), 1U);
  EXPECT_EQ(execution->key_definitions().front().type, string_type());
  EXPECT_EQ(execution->finish().code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(execution->next().error().code(), common::StatusCode::kInvalidArgument);

  auto first_attempt = execution->begin_attempt(first_fragment.tablet_id, {});
  ASSERT_TRUE(first_attempt.has_value()) << first_attempt.error().to_string();
  const auto first_response = response(first_fragment, 1U);
  ASSERT_TRUE(
      execution->accept_responses(first_fragment.tablet_id, std::span{&first_response, 1U}, {})
          .is_ok());
  EXPECT_EQ(execution->finish().code(), common::StatusCode::kUnavailable);

  auto second_attempt = execution->begin_attempt(second_fragment.tablet_id, {});
  ASSERT_TRUE(second_attempt.has_value()) << second_attempt.error().to_string();
  const auto second_response = response(second_fragment, 2U);
  ASSERT_TRUE(
      execution->accept_responses(second_fragment.tablet_id, std::span{&second_response, 1U}, {})
          .is_ok());
  ASSERT_TRUE(execution->finish().is_ok());
  auto output = execution->next();
  ASSERT_TRUE(output.has_value()) << output.error().to_string();
  ASSERT_EQ(output->kind(), query::PhysicalOperatorStepKind::kChunk);
  EXPECT_EQ(output->chunk()->chunk().selected_row_count(), 1U);
  EXPECT_EQ(execution->next()->kind(), query::PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ(execution->finish().code(), common::StatusCode::kInvalidArgument);
}

TEST(DistributedMutableVectorGroupedAggregateQueryExecutionTest,
     RejectsMixedAuthorityAndMakesTerminalSenderFailureSticky) {
  std::vector mixed{fragment(4U, 11U), fragment(6U, 12U)};
  mixed[1].query_id = uuid(99U);
  EXPECT_EQ(DistributedMutableVectorGroupedAggregateQueryExecution::create(1U, std::move(mixed),
                                                                           keys(), aggregates())
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  auto only = fragment(4U, 11U);
  const auto tablet_id = only.tablet_id;
  std::vector fragments{std::move(only)};
  auto execution = DistributedMutableVectorGroupedAggregateQueryExecution::create(
      1U, std::move(fragments), keys(), aggregates(),
      {.sender = {.retry = {.maximum_attempts = 1U}}});
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();
  ASSERT_TRUE(execution->begin_attempt(tablet_id, {}).has_value());
  const auto failed =
      execution->record_transport_failure(tablet_id, common::StatusCode::kUnavailable, {});
  EXPECT_EQ(failed.code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(execution->finish(), failed);
  EXPECT_EQ(execution->record_transport_failure(tablet_id, common::StatusCode::kIoError, {}),
            failed);
}

} // namespace
} // namespace chronos::cluster
