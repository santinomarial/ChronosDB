#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_authority.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
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

TEST(DistributedVectorGroupedAggregateShuffleAuthorityAllocationFailureTest,
     ClassifiesEveryRetainedSourceIndexAllocation) {
  const auto string_type = schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  bool succeeded{};
  for (std::size_t fail_after = 0U; fail_after < 32U; ++fail_after) {
    std::vector<DistributedVectorGroupedAggregateShuffleSource> sources{
        {.tablet_id = schema::TabletId::from_uuid(uuid(1U)).value(), .node_id = 10U},
        {.tablet_id = schema::TabletId::from_uuid(uuid(2U)).value(), .node_id = 20U}};
    std::vector<DistributedVectorGroupedAggregateShuffleDestination> destinations{
        {.partition_id = 0U, .node_id = 30U}, {.partition_id = 1U, .node_id = 40U}};
    std::vector<query::VectorGroupKeyDefinition> keys{
        {.column_ordinal = 0U, .type = string_type, .nullable = false}};
    std::vector<query::VectorAggregateDefinition> aggregates{
        {.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
    auto result = run_failure(fail_after, [&] {
      return DistributedVectorGroupedAggregateShuffleAuthority::create(
          uuid(9U), std::move(sources), std::move(destinations), std::move(keys),
          std::move(aggregates));
    });
    if (result.has_value()) {
      EXPECT_EQ(result->sources().size(), 2U);
      succeeded = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(succeeded);
}

} // namespace
} // namespace chronos::cluster
