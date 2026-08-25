#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_ack.hpp"
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

TEST(DistributedVectorGroupedAggregateShuffleAckAllocationFailureTest,
     ClassifiesEncodeAndCursorAllocationFailure) {
  const auto tablet = schema::TabletId::from_uuid(uuid(2U)).value();
  const auto string = schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  auto authority =
      DistributedVectorGroupedAggregateShuffleAuthority::create(
          uuid(1U), {{.tablet_id = tablet, .node_id = 2U}}, {{.partition_id = 0U, .node_id = 3U}},
          {{.column_ordinal = 0U, .type = string, .nullable = false}}, {})
          .value();
  const DistributedVectorGroupedAggregateShuffleAckV1 ack{
      .query_id = uuid(1U),
      .edge = {.tablet_id = tablet,
               .partition_id = 0U,
               .source_node_id = 2U,
               .target_node_id = 3U,
               .hash_version = authority.hash_version()},
      .partition_count = 1U,
      .accepted_frames = 1U,
      .accepted_bytes = 512U};

  bool encode_success{};
  for (std::size_t fail_after = 0U; fail_after < 8U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      return encode_distributed_vector_grouped_aggregate_shuffle_ack_v1(ack, authority);
    });
    if (result.has_value()) {
      encode_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(encode_success);

  bool cursor_success{};
  for (std::size_t fail_after = 0U; fail_after < 8U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      return DistributedVectorGroupedAggregateShuffleAckV1WriteCursor::create(ack, authority);
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
