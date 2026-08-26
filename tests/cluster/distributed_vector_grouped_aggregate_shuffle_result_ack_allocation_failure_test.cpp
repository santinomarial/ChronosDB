#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_ack.hpp"
#include "support/failing_allocator.hpp"

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

TEST(DistributedVectorGroupedAggregateShuffleResultAckAllocationFailureTest,
     ClassifiesEncodingAndCursorAllocations) {
  const auto string = schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const auto tablet = schema::TabletId::from_uuid(uuid(2U)).value();
  auto authority = DistributedVectorGroupedAggregateShuffleAuthority::create(
                       uuid(1U), {{tablet, 2U}}, {{0U, 3U}}, {{0U, string, false}},
                       {{query::VectorAggregateOperation::kCountStar, std::nullopt}})
                       .value();
  const query::DistributedVectorResultSchema schema{.columns = {{"region", string, false}}};
  const DistributedVectorGroupedAggregateShuffleResultAckV1 ack{
      .query_id = uuid(1U),
      .source_node_id = 3U,
      .target_node_id = 9U,
      .partition_id = 0U,
      .partition_count = 1U,
      .hash_version = kDistributedVectorGroupedAggregateShuffleHashVersionV1,
      .accepted_frames = 1U,
      .accepted_bytes = 132U};

  bool encode_success{};
  for (std::size_t fail_after = 0U; fail_after < 16U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      return encode_distributed_vector_grouped_aggregate_shuffle_result_ack_v1(ack, authority,
                                                                               schema, 9U);
    });
    if (result.has_value()) {
      encode_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(encode_success);

  bool cursor_success{};
  for (std::size_t fail_after = 0U; fail_after < 16U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      return DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor::create(ack, authority,
                                                                                    schema, 9U);
    });
    if (result.has_value()) {
      cursor_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(cursor_success);
}

} // namespace
} // namespace chronos::cluster
