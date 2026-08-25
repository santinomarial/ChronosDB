#include "chronos/cluster/distributed_mutable_vector_grouped_aggregate_shuffle_execution.hpp"
#include "chronos/common/byte_reader.hpp"
#include "chronos/network/messages.hpp"

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
    ++bind_calls;
    return query::DistributedVectorGroupedAggregateAuthority{keys(), aggregates()};
  }

  common::Result<query::DistributedVectorGroupedAggregateWorkerResultV2>
  execute(const query::DistributedMutableVectorFragment& received) override {
    ++execute_calls;
    auto state = query::MergeableVectorAggregateState::create(aggregates().front()).value();
    EXPECT_TRUE(state.accumulate_count_star().has_value());
    EXPECT_TRUE(state.accumulate_count_star().has_value());
    EXPECT_TRUE(state.accumulate_count_star().has_value());
    std::vector<query::ScalarValue> values;
    values.push_back(query::ScalarValue::text(string_type(), "east").value());
    std::vector<query::MergeableVectorAggregateState> states;
    states.push_back(std::move(state));
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
        .input_rows = 3U,
        .group_count = 1U,
        .encoded_bytes = encoded->bytes().size()};
    result.messages.push_back(std::move(*encoded));
    return result;
  }

  std::size_t bind_calls{};
  std::size_t execute_calls{};
};

[[nodiscard]] common::Result<DistributedMutableVectorGroupedAggregateShuffleExecution>
execution(Worker& worker, Authenticator& authenticator, const Authorizer& authorizer) {
  DistributedMutableVectorGroupedAggregateShuffleExecutionConfig config;
  config.worker_execution.sender.retry.maximum_attempts = 1U;
  config.worker_execution.sender.maximum_response_frames = 1U;
  config.worker_execution.coordinator.messages.maximum_messages_per_fragment = 1U;
  config.worker_execution.coordinator.messages.maximum_total_messages = 1U;
  config.worker_transport.authenticator = &authenticator;
  config.worker_transport.node_authorizer = &authorizer;
  config.worker_transport.local_node_id = 2U;
  config.worker_transport.local_worker = &worker;
  config.shuffle.destinations.push_back({.local_node_id = 2U});
  return DistributedMutableVectorGroupedAggregateShuffleExecution::create(
      2U, std::vector{fragment()}, keys(), aggregates(), std::move(config));
}

TEST(DistributedMutableVectorGroupedAggregateShuffleExecutionTest,
     PublishesWorkerStreamsOnlyThroughShuffleNativeFinalization) {
  Worker worker;
  Authenticator authenticator;
  Authorizer authorizer;
  auto owner = execution(worker, authenticator, authorizer);
  ASSERT_TRUE(owner.has_value()) << owner.error().to_string();
  EXPECT_EQ(owner->state(),
            DistributedMutableVectorGroupedAggregateShuffleExecutionState::kCollectingSources);
  EXPECT_FALSE(owner->result().has_value());
  ASSERT_TRUE(owner->poll_once(std::chrono::milliseconds{0}).is_ok());
  EXPECT_EQ(owner->state(),
            DistributedMutableVectorGroupedAggregateShuffleExecutionState::kComplete);
  ASSERT_TRUE(owner->result().has_value());
  EXPECT_EQ(worker.bind_calls, 1U);
  EXPECT_EQ(worker.execute_calls, 1U);
  EXPECT_EQ(owner->metrics().workers.local_completed_attempts, 1U);
  EXPECT_EQ(owner->metrics().shuffle.local_edges, 1U);

  ASSERT_EQ(owner->result()->encoded_batches.size(), 1U);
  auto decoded = network::decode_query_result_batch(owner->result()->encoded_batches.front());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  ASSERT_EQ(decoded->row_count(), 1U);
  auto region =
      query::decode_canonical_scalar_value(string_type(), false, decoded->cell(0U, 0U)->value);
  ASSERT_TRUE(region.has_value()) << region.error().to_string();
  EXPECT_EQ(std::get<std::string>(region->storage()), "east");
  common::ByteReader count{decoded->cell(0U, 1U)->value};
  EXPECT_EQ(count.read_i64_le().value(), 3);
  auto transferred = owner->take_result();
  ASSERT_TRUE(transferred.has_value()) << transferred.error().to_string();
  EXPECT_EQ(transferred->row_count, 1U);
  EXPECT_FALSE(owner->result().has_value());
  EXPECT_EQ(owner->take_result().error().code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(owner->metrics().shuffle.local_edges, 1U);
}

TEST(DistributedMutableVectorGroupedAggregateShuffleExecutionTest,
     CancellationWithholdsTheResultAcrossBothPhases) {
  Worker worker;
  Authenticator authenticator;
  Authorizer authorizer;
  auto owner = execution(worker, authenticator, authorizer);
  ASSERT_TRUE(owner.has_value()) << owner.error().to_string();
  EXPECT_EQ(owner->cancel().code(), common::StatusCode::kCancelled);
  EXPECT_EQ(owner->state(),
            DistributedMutableVectorGroupedAggregateShuffleExecutionState::kCancelled);
  EXPECT_FALSE(owner->result().has_value());
  EXPECT_EQ(worker.execute_calls, 0U);
}

} // namespace
} // namespace chronos::cluster
