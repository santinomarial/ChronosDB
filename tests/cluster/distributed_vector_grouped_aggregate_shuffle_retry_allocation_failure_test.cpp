#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_retry.hpp"
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

[[nodiscard]] std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage>
messages(const schema::TabletId& tablet, const schema::LogicalType& type,
         const std::vector<query::VectorGroupKeyDefinition>& keys,
         const std::vector<query::VectorAggregateDefinition>& aggregates) {
  auto state = query::MergeableVectorAggregateState::create(aggregates.front()).value();
  EXPECT_TRUE(state.accumulate_count_star().has_value());
  std::vector<query::ScalarValue> values;
  values.push_back(query::ScalarValue::text(type, "allocation-key-larger-than-SSO").value());
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
  std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> result;
  result.push_back(
      query::encode_distributed_vector_grouped_aggregate_exchange_message(message, keys, aggregates)
          .value());
  return result;
}

TEST(DistributedVectorGroupedAggregateShuffleRetryAllocationFailureTest,
     ClassifiesRetainedAndAttemptAllocationsWithoutConsumingRetryBudget) {
  const auto tablet = schema::TabletId::from_uuid(uuid(2U)).value();
  const auto type = schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const std::vector<query::VectorGroupKeyDefinition> keys{
      {.column_ordinal = 0U, .type = type, .nullable = false}};
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
  auto resources = query::QueryResourceContext::create(4U << 20U).value();

  bool create_success{};
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    auto encoded = messages(tablet, type, keys, aggregates);
    auto result = run_failure(fail_after, [&] {
      return DistributedVectorGroupedAggregateShuffleRetry::create(authority, edge,
                                                                   std::move(encoded), resources);
    });
    if (result.has_value()) {
      create_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  EXPECT_TRUE(create_success);

  bool attempt_success{};
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    auto retry = DistributedVectorGroupedAggregateShuffleRetry::create(
                     authority, edge, messages(tablet, type, keys, aggregates), resources)
                     .value();
    auto result = run_failure(fail_after, [&] { return retry.begin_attempt({}); });
    if (result.has_value()) {
      attempt_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(retry.state(), DistributedVectorGroupedAggregateShuffleRetryState::kReady);
    EXPECT_EQ(retry.attempts_started(), 0U);
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  EXPECT_TRUE(attempt_success);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

} // namespace
} // namespace chronos::cluster
