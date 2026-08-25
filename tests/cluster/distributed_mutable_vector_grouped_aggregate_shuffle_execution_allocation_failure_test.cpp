#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_shuffle_execution.hpp"
#include "support/failing_allocator.hpp"

#include <chrono>
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
      .database_id = id<manifest::DatabaseId>(2U),
      .table_id = id<schema::TableId>(3U),
      .tablet_id = id<schema::TabletId>(4U),
      .destination_schema_id = id<schema::SchemaId>(5U),
      .raft_group_id = uuid(6U),
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

class Authenticator final : public network::ConnectionAuthenticator {
public:
  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest&) override {
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = 9U};
  }
};

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(std::uint64_t, raft::NodeId) const override {
    return true;
  }
};

class Worker final : public DistributedMutableVectorGroupedAggregateQueryWorkerService {
public:
  common::Result<query::DistributedVectorGroupedAggregateAuthority>
  bind_authority(const query::DistributedMutableVectorFragment&) override {
    try {
      return query::DistributedVectorGroupedAggregateAuthority{keys(), aggregates()};
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                    "test worker authority allocation failed"});
    }
  }

  common::Result<query::DistributedVectorGroupedAggregateWorkerResultV2>
  execute(const query::DistributedMutableVectorFragment& received) override {
    try {
      auto state = query::MergeableVectorAggregateState::create(aggregates().front());
      if (!state.has_value())
        return common::make_unexpected(state.error());
      auto accumulated = state->accumulate_count_star();
      if (!accumulated.has_value())
        return common::make_unexpected(accumulated.error());
      std::vector<query::ScalarValue> values;
      auto text = query::ScalarValue::text(string_type(), "allocation");
      if (!text.has_value())
        return common::make_unexpected(text.error());
      values.push_back(std::move(*text));
      std::vector<query::MergeableVectorAggregateState> states;
      states.push_back(std::move(*state));
      auto encoded = query::encode_distributed_vector_grouped_aggregate_exchange_message(
          {.query_id = received.query_id,
           .tablet_id = received.tablet_id,
           .sequence = 1U,
           .group_ordinal = 0U,
           .group_count = 1U,
           .terminal = true,
           .empty = false},
          values, states, keys(), aggregates());
      if (!encoded.has_value())
        return common::make_unexpected(encoded.error());
      query::DistributedVectorGroupedAggregateWorkerResultV2 result{
          .authority = {.keys = keys(), .aggregates = aggregates()},
          .input_rows = 1U,
          .group_count = 1U,
          .encoded_bytes = encoded->bytes().size()};
      result.messages.push_back(std::move(*encoded));
      return result;
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kResourceExhausted, "test worker allocation failed"});
    }
  }
};

[[nodiscard]] DistributedMutableVectorGroupedAggregateShuffleExecutionConfig
config(Worker& worker, Authenticator& authenticator, const Authorizer& authorizer) {
  DistributedMutableVectorGroupedAggregateShuffleExecutionConfig value;
  value.worker_execution.sender.retry.maximum_attempts = 1U;
  value.worker_execution.sender.maximum_response_frames = 1U;
  value.worker_execution.coordinator.messages.maximum_messages_per_fragment = 1U;
  value.worker_execution.coordinator.messages.maximum_total_messages = 1U;
  value.worker_transport.authenticator = &authenticator;
  value.worker_transport.node_authorizer = &authorizer;
  value.worker_transport.local_node_id = 2U;
  value.worker_transport.local_worker = &worker;
  value.shuffle.destinations.push_back({.local_node_id = 2U});
  return value;
}

template <typename Operation>
[[nodiscard]] auto run_failure(const std::size_t fail_after, Operation&& operation) {
  using Result = decltype(operation());
  std::optional<Result> result;
  {
    ::chronos::test::ScopedAllocationFailure failure{fail_after};
    result.emplace(operation());
    failure.disable();
  }
  return std::move(*result);
}

TEST(DistributedMutableVectorGroupedAggregateShuffleExecutionAllocationFailureTest,
     ClassifiesConstructionAndWorkerToShuffleHandoffAllocations) {
  Worker worker;
  Authenticator authenticator;
  Authorizer authorizer;
  bool construction_succeeded{};
  for (std::size_t fail_after = 0U; fail_after < 4096U; ++fail_after) {
    auto fragments = std::vector{fragment()};
    auto key_definitions = keys();
    auto aggregate_definitions = aggregates();
    auto execution_config = config(worker, authenticator, authorizer);
    auto result = run_failure(fail_after, [&] {
      return DistributedMutableVectorGroupedAggregateShuffleExecution::create(
          2U, std::move(fragments), std::move(key_definitions), std::move(aggregate_definitions),
          std::move(execution_config));
    });
    if (result.has_value()) {
      construction_succeeded = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted)
        << result.error().to_string();
  }
  EXPECT_TRUE(construction_succeeded);

  bool polling_succeeded{};
  for (std::size_t fail_after = 0U; fail_after < 4096U; ++fail_after) {
    auto owner = DistributedMutableVectorGroupedAggregateShuffleExecution::create(
                     2U, std::vector{fragment()}, keys(), aggregates(),
                     config(worker, authenticator, authorizer))
                     .value();
    const common::Status status =
        run_failure(fail_after, [&] { return owner.poll_once(std::chrono::milliseconds{0}); });
    if (status.is_ok()) {
      polling_succeeded = true;
      EXPECT_EQ(owner.state(),
                DistributedMutableVectorGroupedAggregateShuffleExecutionState::kComplete);
      break;
    }
    EXPECT_EQ(status.code(), common::StatusCode::kResourceExhausted) << status.to_string();
  }
  EXPECT_TRUE(polling_succeeded);
}

} // namespace
} // namespace chronos::cluster
