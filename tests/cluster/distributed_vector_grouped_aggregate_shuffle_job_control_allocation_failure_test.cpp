#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_control.hpp"
#include "support/failing_allocator.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <utility>

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

[[nodiscard]] DistributedVectorGroupedAggregateShuffleJobPrepare prepare() {
  const auto string = schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const auto count = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  return {.coordinator_node_id = 9U,
          .target_node_id = 7U,
          .coordinator_result_endpoint = {{127U, 0U, 0U, 1U}, 8137U},
          .execution_timeout = std::chrono::milliseconds{30'000},
          .authority = DistributedVectorGroupedAggregateShuffleAuthority::create(
                           uuid(1U), {{schema::TabletId::from_uuid(uuid(2U)).value(), 3U}},
                           {{0U, 7U}}, {{0U, string, false}},
                           {{query::VectorAggregateOperation::kCountStar, std::nullopt}})
                           .value(),
          .result_schema = {.columns = {{"region", string, false}, {"count", count, false}}}};
}

TEST(DistributedVectorGroupedAggregateShuffleJobControlAllocationFailureTest,
     ClassifiesPrepareEncodingAndCompleteDecodeAllocations) {
  auto expected = prepare();
  auto encode_failure = run_failure(0U, [&] {
    return encode_distributed_vector_grouped_aggregate_shuffle_job_prepare_v1(expected);
  });
  ASSERT_FALSE(encode_failure.has_value());
  EXPECT_EQ(encode_failure.error().code(), common::StatusCode::kResourceExhausted);

  auto encoded =
      encode_distributed_vector_grouped_aggregate_shuffle_job_prepare_v1(expected).value();
  bool saw_failure{};
  bool saw_success{};
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    auto decoded = run_failure(fail_after, [&] {
      return decode_distributed_vector_grouped_aggregate_shuffle_job_control_request_v1_exact(
          encoded.bytes());
    });
    if (!decoded.has_value()) {
      saw_failure = true;
      EXPECT_EQ(decoded.error().code(), common::StatusCode::kResourceExhausted)
          << decoded.error().to_string();
      continue;
    }
    saw_success = true;
    break;
  }
  EXPECT_TRUE(saw_failure);
  EXPECT_TRUE(saw_success);
}

} // namespace
} // namespace chronos::cluster
