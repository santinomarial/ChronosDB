#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_control_transport.hpp"
#include "support/failing_allocator.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>

namespace chronos::cluster {
namespace {

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

TEST(DistributedVectorGroupedAggregateShuffleJobControlTransportAllocationFailureTest,
     ClassifiesHeaderGatedFrameAllocationAndMakesFailureSticky) {
  auto encoded =
      encode_distributed_vector_grouped_aggregate_shuffle_job_prepare_v1(prepare()).value();
  auto reader = DistributedVectorGroupedAggregateShuffleJobControlRequestReader::create().value();
  constexpr std::size_t kHeaderLength =
      distributed_vector_grouped_aggregate_shuffle_job_control_format::kHeaderLength;
  ::chronos::test::ScopedAllocationFailure failure{0U};
  auto consumed = reader.consume(encoded.bytes().first(kHeaderLength));
  failure.disable();
  ASSERT_FALSE(consumed.has_value());
  EXPECT_EQ(consumed.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_TRUE(reader.failed());
  EXPECT_EQ(reader.consume({}).error().code(), common::StatusCode::kResourceExhausted);
}

} // namespace
} // namespace chronos::cluster
