#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_retry.hpp"
#include "support/failing_allocator.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <span>
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
    test::ScopedAllocationFailure failure{fail_after};
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

TEST(DistributedVectorGroupedAggregateShuffleResultRetryAllocationFailureTest,
     ClassifiesPrevalidationAndAttemptReconstruction) {
  const auto string = schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const auto tablet = schema::TabletId::from_uuid(uuid(2U)).value();
  auto authority = DistributedVectorGroupedAggregateShuffleAuthority::create(
                       uuid(1U), {{tablet, 2U}}, {{0U, 3U}}, {{0U, string, false}},
                       {{query::VectorAggregateOperation::kCountStar, std::nullopt}})
                       .value();
  const query::DistributedVectorResultSchema schema{.columns = {{"region", string, false}}};
  const std::string label = "allocation-owned-result";
  const std::array columns{network::QueryResultColumn{"region", string, false}};
  const std::array cells{network::QueryResultCell{.value = std::as_bytes(std::span{label})}};
  const std::vector<std::vector<std::byte>> batches{
      network::encode_query_result_batch(1U, columns, cells).value()};

  bool create_success{};
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    auto retained = batches;
    auto result = run_failure(fail_after, [&] {
      return DistributedVectorGroupedAggregateShuffleResultRetry::create(
          authority, schema, {.partition_id = 0U, .source_node_id = 3U, .coordinator_node_id = 9U},
          std::move(retained));
    });
    if (result.has_value()) {
      create_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(create_success);

  auto retry = DistributedVectorGroupedAggregateShuffleResultRetry::create(
                   authority, schema,
                   {.partition_id = 0U, .source_node_id = 3U, .coordinator_node_id = 9U}, batches)
                   .value();
  bool attempt_success{};
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    auto result = run_failure(fail_after, [&] { return retry.begin_attempt({}); });
    if (result.has_value()) {
      attempt_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(retry.attempts_started(), 0U);
  }
  EXPECT_TRUE(attempt_success);
}

} // namespace
} // namespace chronos::cluster
