#include "chronos/cluster/distributed_mutable_vector_query_execution.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <span>
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

[[nodiscard]] query::DistributedVectorResultSchema result_schema() {
  return {
      .columns = {
          {"value", schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(), false}}};
}

[[nodiscard]] query::DistributedMutableVectorFragment fragment(const std::uint8_t tablet_seed,
                                                               const raft::NodeId serving_node) {
  return {.query_id = uuid(1U),
          .database_id = id<manifest::DatabaseId>(2U),
          .table_id = id<schema::TableId>(3U),
          .tablet_id = id<schema::TabletId>(tablet_seed),
          .destination_schema_id = id<schema::SchemaId>(5U),
          .raft_group_id = uuid(static_cast<std::uint8_t>(tablet_seed + 20U)),
          .serving_node = serving_node,
          .applied_position = 8U,
          .observed_leader_commit_position = 8U,
          .placement_epoch = 9U,
          .read_policy = {.consistency = query::DistributedReadConsistency::kLocalEventual},
          .destination_column_ordinals = {0U},
          .plan = {.mode = query::DistributedVectorPlanMode::kRows, .row_output_indices = {0U}},
          .result_schema = result_schema()};
}

[[nodiscard]] std::vector<std::byte> zero_row_batch() {
  const auto schema_value = result_schema();
  const std::array columns{network::QueryResultColumn{schema_value.columns[0].name,
                                                      schema_value.columns[0].type,
                                                      schema_value.columns[0].nullable}};
  return network::encode_query_result_batch(0U, columns, {}).value();
}

[[nodiscard]] DistributedVectorQueryResponseV2
success(const query::DistributedMutableVectorFragment& value) {
  return {.source_node_id = value.serving_node,
          .target_node_id = 1U,
          .query_id = value.query_id,
          .tablet_id = value.tablet_id,
          .status_code = common::StatusCode::kOk,
          .payload =
              DistributedVectorResultExchangeMessage{.query_id = value.query_id,
                                                     .tablet_id = value.tablet_id,
                                                     .sequence = 1U,
                                                     .terminal = true,
                                                     .encoded_result_batch = zero_row_batch()}};
}

TEST(DistributedMutableVectorQueryExecutionTest,
     PublishesOnlyTheCompletePlanOrderedSchemaBoundResult) {
  std::vector fragments{fragment(4U, 7U), fragment(6U, 8U)};
  const std::array tablets{fragments[0].tablet_id, fragments[1].tablet_id};
  const auto first = success(fragments[0]);
  const auto second = success(fragments[1]);
  const auto expected_plan = fragments.front().plan;
  auto execution = DistributedMutableVectorQueryExecution::create(
      1U, std::move(fragments),
      {.coordinator = {
           .messages = {.maximum_messages_per_fragment = 2U, .maximum_total_messages = 4U}}});
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();
  EXPECT_EQ(execution->finish().error().code(), common::StatusCode::kUnavailable);
  const auto now = DistributedMutableVectorQueryExecution::TimePoint{};
  ASSERT_TRUE(execution->begin_attempt(tablets[0], now).has_value());
  ASSERT_TRUE(execution->begin_attempt(tablets[1], now).has_value());
  EXPECT_EQ(execution->begin_attempt(id<schema::TabletId>(99U), now).error().code(),
            common::StatusCode::kInvalidArgument);

  ASSERT_TRUE(execution->accept_responses(tablets[1], std::span{&second, 1U}, now).is_ok());
  EXPECT_EQ(execution->finish().error().code(), common::StatusCode::kUnavailable);
  ASSERT_TRUE(execution->accept_responses(tablets[0], std::span{&first, 1U}, now).is_ok());
  auto result = execution->finish();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_EQ(result->plan, expected_plan);
  EXPECT_EQ(result->result.result_schema, result_schema());
  ASSERT_EQ(result->result.messages.size(), 2U);
  EXPECT_EQ(result->result.messages[0].tablet_id, tablets[0]);
  EXPECT_EQ(result->result.messages[1].tablet_id, tablets[1]);
  EXPECT_EQ(execution->finish().error().code(), common::StatusCode::kInvalidArgument);
}

TEST(DistributedMutableVectorQueryExecutionTest,
     PreservesFreshAuthorityHintAndPoisonsOnlyAtTerminalFailure) {
  auto value = fragment(4U, 7U);
  const schema::TabletId tablet = value.tablet_id;
  auto execution = DistributedMutableVectorQueryExecution::create(
      1U, {value},
      {.sender = {.retry = {.maximum_attempts = 2U,
                            .initial_backoff = std::chrono::milliseconds{10},
                            .maximum_backoff = std::chrono::milliseconds{10}}}});
  ASSERT_TRUE(execution.has_value()) << execution.error().to_string();
  const auto now = DistributedMutableVectorQueryExecution::TimePoint{};
  ASSERT_TRUE(execution->begin_attempt(tablet, now).has_value());
  const DistributedVectorQueryResponseV2 unavailable{
      .source_node_id = value.serving_node,
      .target_node_id = 1U,
      .query_id = value.query_id,
      .tablet_id = tablet,
      .status_code = common::StatusCode::kUnavailable,
      .leader_hint = DistributedQueryLeaderHint{9U, 10U}};
  ASSERT_TRUE(execution->accept_responses(tablet, std::span{&unavailable, 1U}, now).is_ok());
  EXPECT_EQ(*execution->sender_state(tablet), DistributedQuerySenderState::kBackoff);
  EXPECT_EQ(*execution->suggested_leader(tablet), DistributedQueryLeaderHint(9U, 10U));
  EXPECT_EQ(*execution->next_attempt_not_before(tablet), now + std::chrono::milliseconds{10});
  EXPECT_EQ(execution->finish().error().code(), common::StatusCode::kUnavailable);

  ASSERT_TRUE(execution->begin_attempt(tablet, now + std::chrono::milliseconds{10}).has_value());
  ASSERT_TRUE(execution
                  ->record_transport_failure(tablet, common::StatusCode::kIoError,
                                             now + std::chrono::milliseconds{10})
                  .is_ok());
  EXPECT_EQ(*execution->sender_state(tablet), DistributedQuerySenderState::kFailed);
  EXPECT_EQ(execution->finish().error().code(), common::StatusCode::kIoError);
}

TEST(DistributedMutableVectorQueryExecutionTest, RejectsMixedOrDuplicateAuthorityBeforeAttempts) {
  EXPECT_EQ(DistributedMutableVectorQueryExecution::create(0U, {}).error().code(),
            common::StatusCode::kInvalidArgument);
  auto first = fragment(4U, 7U);
  auto mixed = fragment(6U, 8U);
  mixed.result_schema.columns[0].name = "different";
  EXPECT_EQ(DistributedMutableVectorQueryExecution::create(1U, {first, mixed}).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(DistributedMutableVectorQueryExecution::create(1U, {first, first}).error().code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::cluster
