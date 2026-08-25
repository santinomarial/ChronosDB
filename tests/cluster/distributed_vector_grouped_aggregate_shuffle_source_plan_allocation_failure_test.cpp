#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_source_plan.hpp"
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

[[nodiscard]] schema::TabletId tablet(const std::uint8_t seed) {
  return schema::TabletId::from_uuid(uuid(seed)).value();
}

[[nodiscard]] schema::LogicalType string_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
}

TEST(DistributedVectorGroupedAggregateShuffleSourcePlanAllocationFailureTest,
     ClassifiesEveryAtomicPartitionAndEdgeAllocation) {
  const std::vector<query::VectorGroupKeyDefinition> keys{
      {.column_ordinal = 0U, .type = string_type(), .nullable = false}};
  const std::vector<query::VectorAggregateDefinition> aggregates{
      {.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
  auto authority = DistributedVectorGroupedAggregateShuffleAuthority::create(
                       uuid(1U), {{.tablet_id = tablet(2U), .node_id = 2U}},
                       {{.partition_id = 0U, .node_id = 2U}, {.partition_id = 1U, .node_id = 3U}},
                       keys, aggregates)
                       .value();
  std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> input;
  for (std::uint32_t ordinal = 0U; ordinal < 8U; ++ordinal) {
    std::vector<query::ScalarValue> values;
    values.push_back(
        query::ScalarValue::text(string_type(), "allocation-source-" + std::to_string(ordinal))
            .value());
    std::vector<query::MergeableVectorAggregateState> states;
    states.push_back(query::MergeableVectorAggregateState::create(aggregates.front()).value());
    EXPECT_TRUE(states.front().accumulate_count_star().has_value());
    input.push_back(query::encode_distributed_vector_grouped_aggregate_exchange_message(
                        {.query_id = uuid(1U),
                         .tablet_id = tablet(2U),
                         .sequence = static_cast<std::uint64_t>(ordinal) + 1U,
                         .group_ordinal = ordinal,
                         .group_count = 8U,
                         .terminal = ordinal == 7U,
                         .empty = false},
                        values, states, keys, aggregates)
                        .value());
  }
  auto resources = query::QueryResourceContext::create(32U << 20U).value();
  bool saw_failure{};
  bool succeeded{};
  for (std::size_t fail_after = 0U; fail_after < 512U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      return DistributedVectorGroupedAggregateShuffleSourcePlan::create(authority, tablet(2U),
                                                                        input, resources);
    });
    if (result.has_value()) {
      EXPECT_EQ(result->metrics().local_edges + result->metrics().remote_edges, 2U);
      succeeded = true;
      break;
    }
    saw_failure = true;
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted)
        << result.error().to_string();
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  EXPECT_TRUE(saw_failure);
  EXPECT_TRUE(succeeded);
}

} // namespace
} // namespace chronos::cluster
