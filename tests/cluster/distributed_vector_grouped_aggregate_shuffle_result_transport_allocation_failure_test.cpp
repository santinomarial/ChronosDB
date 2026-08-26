#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_transport.hpp"
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

TEST(DistributedVectorGroupedAggregateShuffleResultTransportAllocationFailureTest,
     ClassifiesEncodeDecodeAndHeaderFirstReaderAllocations) {
  const auto string = schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const auto int64 = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  const auto tablet = schema::TabletId::from_uuid(uuid(2U)).value();
  auto authority =
      DistributedVectorGroupedAggregateShuffleAuthority::create(
          uuid(1U), {{.tablet_id = tablet, .node_id = 2U}}, {{.partition_id = 0U, .node_id = 3U}},
          {{.column_ordinal = 0U, .type = string, .nullable = false}},
          {{.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}})
          .value();
  const query::DistributedVectorResultSchema result_schema{
      .columns = {{"region", string, false}, {"count", int64, false}}};
  const std::array columns{network::QueryResultColumn{"region", string, false},
                           network::QueryResultColumn{"count", int64, false}};
  const std::string label = "allocation-owned-partition-result";
  const std::array<std::byte, 8U> count{std::byte{1U}};
  const std::array cells{network::QueryResultCell{.value = std::as_bytes(std::span{label})},
                         network::QueryResultCell{.value = count}};
  const DistributedVectorGroupedAggregateShuffleResultFrame frame{
      .query_id = uuid(1U),
      .source_node_id = 3U,
      .target_node_id = 9U,
      .partition_id = 0U,
      .partition_count = 1U,
      .hash_version = authority.hash_version(),
      .sequence = 1U,
      .terminal = true,
      .encoded_result_batch = network::encode_query_result_batch(1U, columns, cells).value()};

  bool encode_success{};
  for (std::size_t fail_after = 0U; fail_after < 32U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      return encode_distributed_vector_grouped_aggregate_shuffle_result_frame(frame, authority,
                                                                              result_schema, 9U);
    });
    if (result.has_value()) {
      encode_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(encode_success);
  const auto encoded = encode_distributed_vector_grouped_aggregate_shuffle_result_frame(
                           frame, authority, result_schema, 9U)
                           .value();

  bool decode_success{};
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      return decode_distributed_vector_grouped_aggregate_shuffle_result_frame_exact(
          encoded, authority, result_schema, 9U);
    });
    if (result.has_value()) {
      decode_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(decode_success);

  bool reader_success{};
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    DistributedVectorGroupedAggregateShuffleResultReader reader{authority, result_schema, 9U};
    auto result = run_failure(fail_after, [&] { return reader.consume(encoded); });
    if (result.has_value()) {
      ASSERT_TRUE(result->frame.has_value());
      reader_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_TRUE(reader.failed());
  }
  EXPECT_TRUE(reader_success);
}

} // namespace
} // namespace chronos::cluster
