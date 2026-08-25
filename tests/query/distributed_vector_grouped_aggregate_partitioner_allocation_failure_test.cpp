#include "chronos/query/distributed_vector_grouped_aggregate_partitioner.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace chronos::query {
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

TEST(DistributedVectorGroupedAggregatePartitionerAllocationFailureTest,
     ClassifiesEveryPartitionAllocationAndRemainsRetryable) {
  const std::vector<VectorGroupKeyDefinition> keys{
      {.column_ordinal = 0U, .type = string_type(), .nullable = false}};
  const std::vector<VectorAggregateDefinition> aggregates{
      {.operation = VectorAggregateOperation::kCountStar, .input = std::nullopt}};
  auto state = MergeableVectorAggregateState::create(aggregates[0]).value();
  ASSERT_TRUE(state.accumulate_count_star().has_value());
  std::vector<MergeableVectorAggregateState> states;
  states.push_back(std::move(state));
  std::vector<ScalarValue> values;
  values.push_back(ScalarValue::text(string_type(), "partition-key-larger-than-SSO").value());
  std::vector<EncodedDistributedVectorGroupedAggregateExchangeMessage> input;
  input.push_back(encode_distributed_vector_grouped_aggregate_exchange_message(
                      {.query_id = uuid(1U),
                       .tablet_id = schema::TabletId::from_uuid(uuid(2U)).value(),
                       .sequence = 1U,
                       .group_ordinal = 0U,
                       .group_count = 1U,
                       .terminal = true,
                       .empty = false},
                      values, states, keys, aggregates)
                      .value());
  QueryResourceContext resources = QueryResourceContext::create(8U << 20U).value();
  auto partitioner =
      DistributedVectorGroupedAggregatePartitioner::create(keys, aggregates, resources, 8U).value();

  bool succeeded{};
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    auto result = run_failure(fail_after, [&] { return partitioner.partition(input); });
    if (result.has_value()) {
      EXPECT_EQ(result->size(), 8U);
      succeeded = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
    auto retry = partitioner.partition(input);
    ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
    EXPECT_EQ(retry->size(), 8U);
  }
  EXPECT_TRUE(succeeded);
}

} // namespace
} // namespace chronos::query
