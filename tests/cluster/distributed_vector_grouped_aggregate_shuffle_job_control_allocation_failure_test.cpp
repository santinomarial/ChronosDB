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

  const DistributedVectorGroupedAggregateShuffleJobControlResponse response{
      .action = DistributedVectorGroupedAggregateShuffleJobControlAction::kPrepare,
      .status_code = common::StatusCode::kOk,
      .query_id = uuid(1U),
      .coordinator_node_id = 9U,
      .target_node_id = 7U,
      .reducer_shuffle_endpoint = network::Ipv4Endpoint{{127U, 0U, 0U, 1U}, 9123U}};
  auto response_failure = run_failure(0U, [&] {
    return encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v1(response);
  });
  ASSERT_FALSE(response_failure.has_value());
  EXPECT_EQ(response_failure.error().code(), common::StatusCode::kResourceExhausted);
}

TEST(DistributedVectorGroupedAggregateShuffleJobControlAllocationFailureTest,
     ClassifiesVersionTwoRouteEncodingAndDecodeAllocations) {
  const DistributedVectorGroupedAggregateShuffleJobInstallRoutes routes{
      .query_id = uuid(1U),
      .coordinator_node_id = 9U,
      .target_node_id = 7U,
      .routes = {{.node_id = 7U, .endpoint = {{127U, 0U, 0U, 1U}, 9123U}},
                 {.node_id = 8U, .endpoint = {{127U, 0U, 0U, 2U}, 9124U}}}};
  auto encode_failure = run_failure(0U, [&] {
    return encode_distributed_vector_grouped_aggregate_shuffle_job_install_routes_v2(routes);
  });
  ASSERT_FALSE(encode_failure.has_value());
  EXPECT_EQ(encode_failure.error().code(), common::StatusCode::kResourceExhausted);

  auto encoded =
      encode_distributed_vector_grouped_aggregate_shuffle_job_install_routes_v2(routes).value();
  bool saw_failure{};
  bool saw_success{};
  for (std::size_t fail_after = 0U; fail_after < 16U; ++fail_after) {
    auto decoded = run_failure(fail_after, [&] {
      return decode_distributed_vector_grouped_aggregate_shuffle_job_control_request_v2_exact(
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

TEST(DistributedVectorGroupedAggregateShuffleJobControlAllocationFailureTest,
     ClassifiesVersionThreeCancellationEncodingAllocations) {
  const DistributedVectorGroupedAggregateShuffleJobCancel cancel{
      .query_id = uuid(1U), .coordinator_node_id = 9U, .target_node_id = 7U};
  auto request_failure = run_failure(0U, [&] {
    return encode_distributed_vector_grouped_aggregate_shuffle_job_cancel_v3(cancel);
  });
  ASSERT_FALSE(request_failure.has_value());
  EXPECT_EQ(request_failure.error().code(), common::StatusCode::kResourceExhausted);

  const DistributedVectorGroupedAggregateShuffleJobControlResponse response{
      .action = DistributedVectorGroupedAggregateShuffleJobControlAction::kCancel,
      .status_code = common::StatusCode::kOk,
      .query_id = uuid(1U),
      .coordinator_node_id = 9U,
      .target_node_id = 7U};
  auto response_failure = run_failure(0U, [&] {
    return encode_distributed_vector_grouped_aggregate_shuffle_job_control_response_v3(response);
  });
  ASSERT_FALSE(response_failure.has_value());
  EXPECT_EQ(response_failure.error().code(), common::StatusCode::kResourceExhausted);
}

} // namespace
} // namespace chronos::cluster
