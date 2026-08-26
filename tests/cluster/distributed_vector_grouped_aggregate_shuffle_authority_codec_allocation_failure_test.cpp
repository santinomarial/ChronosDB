#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_authority_codec.hpp"
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

[[nodiscard]] DistributedVectorGroupedAggregateShuffleAuthority authority() {
  const auto type = schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  return DistributedVectorGroupedAggregateShuffleAuthority::create(
             uuid(1U), {{schema::TabletId::from_uuid(uuid(2U)).value(), 3U}}, {{0U, 3U}},
             {{0U, type, false}}, {{query::VectorAggregateOperation::kCountStar, std::nullopt}})
      .value();
}

TEST(DistributedVectorGroupedAggregateShuffleAuthorityCodecAllocationFailureTest,
     ClassifiesEncodingAndCompleteDecodeAllocations) {
  auto expected = authority();
  auto encode_failure = run_failure(
      0U, [&] { return encode_distributed_vector_grouped_aggregate_shuffle_authority(expected); });
  ASSERT_FALSE(encode_failure.has_value());
  EXPECT_EQ(encode_failure.error().code(), common::StatusCode::kResourceExhausted);

  auto encoded = encode_distributed_vector_grouped_aggregate_shuffle_authority(expected).value();
  bool saw_failure{};
  bool saw_success{};
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    auto decoded = run_failure(fail_after, [&] {
      return decode_distributed_vector_grouped_aggregate_shuffle_authority_exact(encoded.bytes());
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
