#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_ack.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <span>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

[[nodiscard]] schema::TabletId tablet() {
  return schema::TabletId::from_uuid(uuid(2U)).value();
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleAuthority authority() {
  const auto string = schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  return DistributedVectorGroupedAggregateShuffleAuthority::create(
             uuid(1U), {{.tablet_id = tablet(), .node_id = 2U}},
             {{.partition_id = 0U, .node_id = 3U}},
             {{.column_ordinal = 0U, .type = string, .nullable = false}},
             {{.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}})
      .value();
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleAckV1 ack() {
  return {.query_id = uuid(1U),
          .edge = {.tablet_id = tablet(),
                   .partition_id = 0U,
                   .source_node_id = 2U,
                   .target_node_id = 3U,
                   .hash_version = kDistributedVectorGroupedAggregateShuffleHashVersionV1},
          .partition_count = 1U,
          .accepted_frames = 2U,
          .accepted_bytes = 1024U};
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
}

TEST(DistributedVectorGroupedAggregateShuffleAckTest,
     RoundTripsReverseRouteAndOwnsEveryReadAndWriteFragment) {
  auto expected = authority();
  const auto value = ack();
  auto encoded = encode_distributed_vector_grouped_aggregate_shuffle_ack_v1(value, expected);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  ASSERT_EQ(encoded->size(), kDistributedVectorGroupedAggregateShuffleAckV1Size);
  auto decoded =
      decode_distributed_vector_grouped_aggregate_shuffle_ack_v1_exact(*encoded, expected);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->edge.source_node_id, 2U);
  EXPECT_EQ(decoded->edge.target_node_id, 3U);
  EXPECT_EQ(decoded->accepted_frames, 2U);
  EXPECT_EQ(decoded->accepted_bytes, 1024U);

  for (std::size_t split = 0U; split < encoded->size(); ++split) {
    DistributedVectorGroupedAggregateShuffleAckV1Reader reader{expected};
    auto first = reader.consume(std::span{*encoded}.first(split));
    ASSERT_TRUE(first.has_value()) << first.error().to_string();
    EXPECT_FALSE(first->ack.has_value());
    auto second = reader.consume(std::span{*encoded}.subspan(split));
    ASSERT_TRUE(second.has_value()) << second.error().to_string();
    const auto& decoded_ack = second->ack;
    if (!decoded_ack.has_value()) {
      ADD_FAILURE() << "complete shuffle acknowledgement split produced no value";
      return;
    }
    EXPECT_EQ(decoded_ack->accepted_bytes, 1024U);
  }

  auto cursor =
      DistributedVectorGroupedAggregateShuffleAckV1WriteCursor::create(value, expected).value();
  while (!cursor.complete()) {
    const std::size_t count = std::min<std::size_t>(7U, cursor.pending_write().size());
    ASSERT_TRUE(cursor.consume_written(count).is_ok());
  }
  EXPECT_EQ(cursor.written_bytes(), encoded->size());
  auto moved = std::move(cursor);
  EXPECT_TRUE(moved.complete());
}

TEST(DistributedVectorGroupedAggregateShuffleAckTest,
     RejectsAuthorityDriftDamageUnknownVersionAndInvalidExtent) {
  auto expected = authority();
  auto value = ack();
  value.accepted_frames = 0U;
  EXPECT_EQ(
      encode_distributed_vector_grouped_aggregate_shuffle_ack_v1(value, expected).error().code(),
      common::StatusCode::kInvalidArgument);
  auto bytes = encode_distributed_vector_grouped_aggregate_shuffle_ack_v1(ack(), expected).value();
  bytes[32U] ^= std::byte{1U};
  EXPECT_EQ(decode_distributed_vector_grouped_aggregate_shuffle_ack_v1_exact(bytes, expected)
                .error()
                .code(),
            common::StatusCode::kCorruption);

  auto unknown =
      encode_distributed_vector_grouped_aggregate_shuffle_ack_v1(ack(), expected).value();
  unknown[8U] = std::byte{2U};
  store_u32(unknown, 124U, common::crc32c(std::span{unknown}.first(124U)));
  store_u32(unknown, 128U, common::crc32c(std::span{unknown}.first(128U)));
  EXPECT_EQ(decode_distributed_vector_grouped_aggregate_shuffle_ack_v1_exact(unknown, expected)
                .error()
                .code(),
            common::StatusCode::kNotSupported);
}

} // namespace
} // namespace chronos::cluster
