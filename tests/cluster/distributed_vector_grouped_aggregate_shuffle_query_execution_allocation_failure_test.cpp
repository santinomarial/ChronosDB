#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_query_execution.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
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

[[nodiscard]] std::vector<query::VectorGroupKeyDefinition> keys() {
  return {{.column_ordinal = 0U, .type = string_type(), .nullable = false}};
}

[[nodiscard]] std::vector<query::VectorAggregateDefinition> aggregates() {
  return {{.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
}

[[nodiscard]] query::DistributedMutableVectorFragment fragment() {
  const auto int64 = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  return {
      .query_id = uuid(1U),
      .database_id = id<manifest::DatabaseId>(8U),
      .table_id = id<schema::TableId>(9U),
      .tablet_id = id<schema::TabletId>(2U),
      .destination_schema_id = id<schema::SchemaId>(10U),
      .raft_group_id = uuid(11U),
      .serving_node = 2U,
      .applied_position = 10U,
      .observed_leader_commit_position = 10U,
      .placement_epoch = 3U,
      .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
      .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
      .destination_column_ordinals = {0U},
      .plan = {.mode = query::DistributedVectorPlanMode::kGroupedAggregate,
               .group_key_input_indices = {0U},
               .aggregates = {{.operation = query::VectorAggregateOperation::kCountStar}}},
      .result_schema = {.columns = {{"region", string_type(), false}, {"count", int64, false}}}};
}

struct Inputs {
  DistributedVectorGroupedAggregateShuffleAuthority authority;
  std::vector<query::DistributedMutableVectorFragment> fragments;
  std::vector<DistributedVectorGroupedAggregateShuffleSourceInput> sources;
  DistributedVectorGroupedAggregateShuffleQueryExecutionConfig config;
};

[[nodiscard]] Inputs inputs() {
  std::vector fragments{fragment()};
  auto authority = DistributedVectorGroupedAggregateShuffleAuthority::create_from_mutable_fragments(
                       fragments, keys(), aggregates())
                       .value();
  auto state = query::MergeableVectorAggregateState::create(aggregates().front()).value();
  EXPECT_TRUE(state.accumulate_count_star().has_value());
  std::vector<query::ScalarValue> values;
  values.push_back(query::ScalarValue::text(string_type(), "allocation").value());
  std::vector<query::MergeableVectorAggregateState> states;
  states.push_back(std::move(state));
  std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> messages;
  messages.push_back(query::encode_distributed_vector_grouped_aggregate_exchange_message(
                         {.query_id = uuid(1U),
                          .tablet_id = id<schema::TabletId>(2U),
                          .sequence = 1U,
                          .group_ordinal = 0U,
                          .group_count = 1U,
                          .terminal = true,
                          .empty = false},
                         values, states, keys(), aggregates())
                         .value());
  std::vector<DistributedVectorGroupedAggregateShuffleSourceInput> sources;
  sources.push_back({.tablet_id = id<schema::TabletId>(2U), .messages = std::move(messages)});
  DistributedVectorGroupedAggregateShuffleQueryExecutionConfig config;
  config.destinations.push_back({.local_node_id = 2U});
  return {std::move(authority), std::move(fragments), std::move(sources), std::move(config)};
}

template <typename Operation>
[[nodiscard]] auto run_failure(std::size_t fail_after, Operation&& operation) {
  using Result = decltype(operation());
  std::optional<Result> result;
  {
    ::chronos::test::ScopedAllocationFailure failure{fail_after};
    result.emplace(operation());
    failure.disable();
  }
  return std::move(*result);
}

TEST(DistributedVectorGroupedAggregateShuffleQueryExecutionAllocationFailureTest,
     ClassifiesConstructionAndFinalizationAllocations) {
  bool construction_succeeded{};
  for (std::size_t fail_after = 0U; fail_after < 2048U; ++fail_after) {
    Inputs value = inputs();
    auto result = run_failure(fail_after, [&] {
      return DistributedVectorGroupedAggregateShuffleQueryExecution::create(
          std::move(value.authority), std::move(value.fragments), std::move(value.sources),
          std::move(value.config));
    });
    if (result.has_value()) {
      construction_succeeded = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted)
        << result.error().to_string();
  }
  EXPECT_TRUE(construction_succeeded);
  bool finalization_succeeded{};
  for (std::size_t fail_after = 0U; fail_after < 2048U; ++fail_after) {
    Inputs value = inputs();
    auto execution = DistributedVectorGroupedAggregateShuffleQueryExecution::create(
                         std::move(value.authority), std::move(value.fragments),
                         std::move(value.sources), std::move(value.config))
                         .value();
    const common::Status status =
        run_failure(fail_after, [&] { return execution.poll_once(std::chrono::milliseconds{0}); });
    if (status.is_ok()) {
      finalization_succeeded = true;
      EXPECT_EQ(execution.state(),
                DistributedVectorGroupedAggregateShuffleQueryExecutionState::kComplete);
      break;
    }
    EXPECT_EQ(status.code(), common::StatusCode::kResourceExhausted) << status.to_string();
  }
  EXPECT_TRUE(finalization_succeeded);
}

} // namespace
} // namespace chronos::cluster
