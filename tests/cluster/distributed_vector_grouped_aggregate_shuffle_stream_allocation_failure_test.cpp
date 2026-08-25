#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_stream.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

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

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

[[nodiscard]] schema::LogicalType string_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
}

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal_id,
                                      const raft::NodeId node_id) const override {
    return common::Result<bool>{principal_id == 91U && node_id == 2U};
  }
};

TEST(DistributedVectorGroupedAggregateShuffleStreamAllocationFailureTest,
     ClassifiesSenderReceiverAndDecodeAllocationsWithoutPublishingOrLeakingCredit) {
  const auto tablet = schema::TabletId::from_uuid(uuid(2U)).value();
  const std::vector<query::VectorGroupKeyDefinition> keys{
      {.column_ordinal = 0U, .type = string_type(), .nullable = false}};
  const std::vector<query::VectorAggregateDefinition> aggregates{
      {.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
  auto authority = DistributedVectorGroupedAggregateShuffleAuthority::create(
                       uuid(1U), {{.tablet_id = tablet, .node_id = 2U}},
                       {{.partition_id = 0U, .node_id = 3U}}, keys, aggregates)
                       .value();
  const DistributedVectorGroupedAggregateShuffleEdge edge{.tablet_id = tablet,
                                                          .partition_id = 0U,
                                                          .source_node_id = 2U,
                                                          .target_node_id = 3U,
                                                          .hash_version = authority.hash_version()};
  auto state = query::MergeableVectorAggregateState::create(aggregates.front()).value();
  ASSERT_TRUE(state.accumulate_count_star().has_value());
  std::vector<query::ScalarValue> values;
  values.push_back(
      query::ScalarValue::text(string_type(), "allocation-key-larger-than-SSO").value());
  std::vector<query::MergeableVectorAggregateState> states;
  states.push_back(std::move(state));
  query::DistributedVectorGroupedAggregateExchangeMessage message{{.query_id = uuid(1U),
                                                                   .tablet_id = tablet,
                                                                   .sequence = 1U,
                                                                   .group_ordinal = 0U,
                                                                   .group_count = 1U,
                                                                   .terminal = true,
                                                                   .empty = false},
                                                                  std::move(values),
                                                                  std::move(states)};
  std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> encoded;
  encoded.push_back(
      query::encode_distributed_vector_grouped_aggregate_exchange_message(message, keys, aggregates)
          .value());
  query::QueryResourceContext resources = query::QueryResourceContext::create(4U << 20U).value();

  bool sender_success{};
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      return DistributedVectorGroupedAggregateShuffleStreamSender::create(authority, edge, encoded,
                                                                          resources);
    });
    if (result.has_value()) {
      sender_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  EXPECT_TRUE(sender_success);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);

  auto sender = DistributedVectorGroupedAggregateShuffleStreamSender::create(authority, edge,
                                                                             encoded, resources)
                    .value();
  std::vector<std::byte> stream_bytes(sender.pending_write().begin(), sender.pending_write().end());
  Authorizer authorizer;

  bool create_success{};
  for (std::size_t fail_after = 0U; fail_after < 32U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      return DistributedVectorGroupedAggregateShuffleStreamReceiver::create(
          authority, 3U, authorizer, {.authorized = true, .principal_id = 91U}, resources);
    });
    if (result.has_value()) {
      create_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(create_success);

  bool consume_success{};
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    {
      auto receiver =
          DistributedVectorGroupedAggregateShuffleStreamReceiver::create(
              authority, 3U, authorizer, {.authorized = true, .principal_id = 91U}, resources)
              .value();
      auto result = run_failure(fail_after, [&] { return receiver.consume(stream_bytes); });
      if (result.has_value()) {
        ASSERT_TRUE(result->complete);
        consume_success = true;
        break;
      }
      EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
      EXPECT_TRUE(receiver.failed());
      EXPECT_EQ(receiver.accepted_frames(), 0U);
    }
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  EXPECT_TRUE(consume_success);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

} // namespace
} // namespace chronos::cluster
