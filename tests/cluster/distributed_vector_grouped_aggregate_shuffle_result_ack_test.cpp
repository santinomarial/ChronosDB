#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_ack.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <span>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleAuthority authority() {
  const auto string = schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const auto tablet = schema::TabletId::from_uuid(uuid(2U)).value();
  return DistributedVectorGroupedAggregateShuffleAuthority::create(
             uuid(1U), {{tablet, 2U}}, {{0U, 3U}}, {{0U, string, false}},
             {{query::VectorAggregateOperation::kCountStar, std::nullopt}})
      .value();
}

[[nodiscard]] query::DistributedVectorResultSchema schema_value() {
  const auto string = schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  return {.columns = {{"region", string, false}}};
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleResultAckV1 ack() {
  return {.query_id = uuid(1U),
          .source_node_id = 3U,
          .target_node_id = 9U,
          .partition_id = 0U,
          .partition_count = 1U,
          .hash_version = kDistributedVectorGroupedAggregateShuffleHashVersionV1,
          .accepted_frames = 2U,
          .accepted_bytes = 1024U};
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
}

TEST(DistributedVectorGroupedAggregateShuffleResultAckTest,
     RoundTripsReverseRouteSchemaAndEveryPartialIoSplit) {
  auto expected = authority();
  const auto schema = schema_value();
  auto encoded = encode_distributed_vector_grouped_aggregate_shuffle_result_ack_v1(ack(), expected,
                                                                                   schema, 9U);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  ASSERT_EQ(encoded->size(), kDistributedVectorGroupedAggregateShuffleResultAckV1Size);
  auto decoded = decode_distributed_vector_grouped_aggregate_shuffle_result_ack_v1_exact(
      *encoded, expected, schema, 9U);
  if (!decoded.has_value()) {
    FAIL() << decoded.error().to_string();
  }
  const auto& decoded_ack = decoded.value();
  EXPECT_EQ(decoded_ack.source_node_id, 3U);
  EXPECT_EQ(decoded_ack.target_node_id, 9U);
  EXPECT_EQ(decoded_ack.accepted_frames, 2U);
  EXPECT_EQ(decoded_ack.accepted_bytes, 1024U);

  for (std::size_t split = 0U; split < encoded->size(); ++split) {
    DistributedVectorGroupedAggregateShuffleResultAckV1Reader reader{expected, schema, 9U};
    auto first = reader.consume(std::span{*encoded}.first(split));
    ASSERT_TRUE(first.has_value()) << first.error().to_string();
    EXPECT_FALSE(first->ack.has_value());
    auto second = reader.consume(std::span{*encoded}.subspan(split));
    ASSERT_TRUE(second.has_value()) << second.error().to_string();
    const auto& split_ack = second->ack;
    if (!split_ack.has_value()) {
      ADD_FAILURE() << "complete result acknowledgement split produced no value";
      return;
    }
    EXPECT_EQ(split_ack->accepted_bytes, 1024U);
    auto repeated = reader.consume({});
    ASSERT_TRUE(repeated.has_value());
    EXPECT_FALSE(repeated->ack.has_value());
  }

  auto cursor = DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor::create(
                    ack(), expected, schema, 9U)
                    .value();
  while (!cursor.complete()) {
    const std::size_t count = std::min<std::size_t>(7U, cursor.pending_write().size());
    ASSERT_TRUE(cursor.consume_written(count).is_ok());
  }
  auto moved = std::move(cursor);
  EXPECT_TRUE(moved.complete());
}

TEST(DistributedVectorGroupedAggregateShuffleResultAckTest,
     RejectsAuthoritySchemaDamageUnknownVersionAndInvalidExtent) {
  auto expected = authority();
  const auto schema = schema_value();
  auto invalid = ack();
  invalid.accepted_frames = 0U;
  EXPECT_EQ(encode_distributed_vector_grouped_aggregate_shuffle_result_ack_v1(invalid, expected,
                                                                              schema, 9U)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  auto damaged =
      encode_distributed_vector_grouped_aggregate_shuffle_result_ack_v1(ack(), expected, schema, 9U)
          .value();
  damaged[32U] ^= std::byte{1U};
  EXPECT_EQ(decode_distributed_vector_grouped_aggregate_shuffle_result_ack_v1_exact(
                damaged, expected, schema, 9U)
                .error()
                .code(),
            common::StatusCode::kCorruption);

  auto unknown =
      encode_distributed_vector_grouped_aggregate_shuffle_result_ack_v1(ack(), expected, schema, 9U)
          .value();
  unknown[8U] = std::byte{2U};
  store_u32(unknown, 124U, common::crc32c(std::span{unknown}.first(124U)));
  store_u32(unknown, 128U, common::crc32c(std::span{unknown}.first(128U)));
  EXPECT_EQ(decode_distributed_vector_grouped_aggregate_shuffle_result_ack_v1_exact(
                unknown, expected, schema, 9U)
                .error()
                .code(),
            common::StatusCode::kNotSupported);

  auto other_schema = schema;
  other_schema.columns[0].name = "other";
  EXPECT_EQ(decode_distributed_vector_grouped_aggregate_shuffle_result_ack_v1_exact(
                encode_distributed_vector_grouped_aggregate_shuffle_result_ack_v1(ack(), expected,
                                                                                  schema, 9U)
                    .value(),
                expected, other_schema, 9U)
                .error()
                .code(),
            common::StatusCode::kCorruption);
}

} // namespace
} // namespace chronos::cluster
