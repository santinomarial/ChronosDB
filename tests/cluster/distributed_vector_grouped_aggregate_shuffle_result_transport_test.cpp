#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_transport.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

[[nodiscard]] schema::LogicalType string_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
}

[[nodiscard]] schema::LogicalType int64_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleAuthority authority() {
  const auto tablet = schema::TabletId::from_uuid(uuid(2U)).value();
  return DistributedVectorGroupedAggregateShuffleAuthority::create(
             uuid(1U), {{.tablet_id = tablet, .node_id = 2U}},
             {{.partition_id = 0U, .node_id = 3U}},
             {{.column_ordinal = 0U, .type = string_type(), .nullable = false}},
             {{.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}})
      .value();
}

[[nodiscard]] query::DistributedVectorResultSchema result_schema() {
  return {.columns = {{"region", string_type(), false}, {"count", int64_type(), false}}};
}

[[nodiscard]] std::vector<std::byte> batch() {
  const auto schema_value = result_schema();
  const std::array columns{
      network::QueryResultColumn{schema_value.columns[0].name, schema_value.columns[0].type, false},
      network::QueryResultColumn{schema_value.columns[1].name, schema_value.columns[1].type,
                                 false}};
  const std::string label = "remote-partition-result";
  std::array<std::byte, sizeof(std::int64_t)> count{};
  common::ByteWriter writer{count};
  EXPECT_TRUE(writer.write_i64_le(7).is_ok());
  const std::array cells{network::QueryResultCell{.value = std::as_bytes(std::span{label})},
                         network::QueryResultCell{.value = count}};
  return network::encode_query_result_batch(1U, columns, cells).value();
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleResultFrame frame() {
  return {.query_id = uuid(1U),
          .source_node_id = 3U,
          .target_node_id = 9U,
          .partition_id = 0U,
          .partition_count = 1U,
          .hash_version = kDistributedVectorGroupedAggregateShuffleHashVersionV1,
          .sequence = 1U,
          .terminal = true,
          .encoded_result_batch = batch()};
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
}

TEST(DistributedVectorGroupedAggregateShuffleResultTransportTest,
     RoundTripsProofBoundBatchAndCanonicalEmptyTerminal) {
  auto expected = authority();
  const auto schema_value = result_schema();
  const auto value = frame();
  auto encoded = encode_distributed_vector_grouped_aggregate_shuffle_result_frame(value, expected,
                                                                                  schema_value, 9U);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  const std::array<std::byte, 8U> magic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                        std::byte{'V'}, std::byte{'G'}, std::byte{'R'},
                                        std::byte{'R'}, std::byte{'1'}};
  EXPECT_TRUE(std::ranges::equal(std::span{*encoded}.first(magic.size()), magic));
  auto decoded = decode_distributed_vector_grouped_aggregate_shuffle_result_frame_exact(
      *encoded, expected, schema_value, 9U);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, value);
  auto decoded_batch = network::decode_query_result_batch(decoded->encoded_result_batch);
  ASSERT_TRUE(decoded_batch.has_value());
  EXPECT_EQ(decoded_batch->row_count(), 1U);
  EXPECT_EQ(decoded_batch->columns().size(), 2U);

  auto empty = value;
  empty.sequence = 2U;
  empty.encoded_result_batch.clear();
  auto encoded_empty = encode_distributed_vector_grouped_aggregate_shuffle_result_frame(
      empty, expected, schema_value, 9U);
  ASSERT_TRUE(encoded_empty.has_value());
  EXPECT_TRUE(decode_distributed_vector_grouped_aggregate_shuffle_result_frame_exact(
                  *encoded_empty, expected, schema_value, 9U)
                  ->encoded_result_batch.empty());
  auto drifted_schema = schema_value;
  drifted_schema.columns[0].name = "different";
  EXPECT_EQ(decode_distributed_vector_grouped_aggregate_shuffle_result_frame_exact(
                *encoded_empty, expected, drifted_schema, 9U)
                .error()
                .code(),
            common::StatusCode::kCorruption);

  empty.terminal = false;
  EXPECT_EQ(encode_distributed_vector_grouped_aggregate_shuffle_result_frame(empty, expected,
                                                                             schema_value, 9U)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedVectorGroupedAggregateShuffleResultTransportTest,
     RejectsAuthoritySchemaDamageVersionAndNonexactBytes) {
  auto expected = authority();
  auto schema_value = result_schema();
  auto value = frame();
  value.source_node_id = 4U;
  EXPECT_EQ(encode_distributed_vector_grouped_aggregate_shuffle_result_frame(value, expected,
                                                                             schema_value, 9U)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  value = frame();
  const auto encoded = encode_distributed_vector_grouped_aggregate_shuffle_result_frame(
                           value, expected, schema_value, 9U)
                           .value();
  EXPECT_EQ(decode_distributed_vector_grouped_aggregate_shuffle_result_frame_exact(
                encoded, expected, schema_value, 8U)
                .error()
                .code(),
            common::StatusCode::kCorruption);
  schema_value.columns[1].name = "different";
  EXPECT_EQ(decode_distributed_vector_grouped_aggregate_shuffle_result_frame_exact(
                encoded, expected, schema_value, 9U)
                .error()
                .code(),
            common::StatusCode::kCorruption);
  schema_value = result_schema();
  auto damaged = encoded;
  damaged[distributed_vector_grouped_aggregate_shuffle_result_format::kHeaderLength] ^=
      std::byte{1U};
  EXPECT_EQ(decode_distributed_vector_grouped_aggregate_shuffle_result_frame_exact(
                damaged, expected, schema_value, 9U)
                .error()
                .code(),
            common::StatusCode::kCorruption);
  auto unknown = encoded;
  unknown[8U] = std::byte{2U};
  store_u32(unknown, 124U, common::crc32c(std::span{unknown}.first(124U)));
  EXPECT_EQ(decode_distributed_vector_grouped_aggregate_shuffle_result_frame_exact(
                unknown, expected, schema_value, 9U)
                .error()
                .code(),
            common::StatusCode::kNotSupported);
  EXPECT_EQ(decode_distributed_vector_grouped_aggregate_shuffle_result_frame_exact(
                std::span{encoded}.first(encoded.size() - 1U), expected, schema_value, 9U)
                .error()
                .code(),
            common::StatusCode::kCorruption);
}

TEST(DistributedVectorGroupedAggregateShuffleResultTransportTest,
     ReaderAndCursorOwnEverySplitAndCoalescedSuccessor) {
  auto expected = authority();
  const auto schema_value = result_schema();
  const auto value = frame();
  const auto encoded = encode_distributed_vector_grouped_aggregate_shuffle_result_frame(
                           value, expected, schema_value, 9U)
                           .value();
  for (std::size_t split = 0U; split <= encoded.size(); ++split) {
    DistributedVectorGroupedAggregateShuffleResultReader reader{expected, schema_value, 9U};
    auto first = reader.consume(std::span{encoded}.first(split));
    ASSERT_TRUE(first.has_value()) << first.error().to_string();
    EXPECT_EQ(first->consumed_bytes, split);
    if (split != encoded.size()) {
      EXPECT_FALSE(first->frame.has_value());
    }
    auto second = reader.consume(std::span{encoded}.subspan(split));
    ASSERT_TRUE(second.has_value()) << second.error().to_string();
    ASSERT_TRUE((first->frame.has_value() || second->frame.has_value()));
  }

  std::vector<std::byte> coalesced = encoded;
  coalesced.insert(coalesced.end(), encoded.begin(), encoded.end());
  DistributedVectorGroupedAggregateShuffleResultReader reader{expected, schema_value, 9U};
  auto first = reader.consume(coalesced);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(first->frame.has_value());
  EXPECT_EQ(first->consumed_bytes, encoded.size());
  auto second = reader.consume(std::span{coalesced}.subspan(first->consumed_bytes));
  ASSERT_TRUE(second.has_value());
  ASSERT_TRUE(second->frame.has_value());

  auto cursor = DistributedVectorGroupedAggregateShuffleResultWriteCursor::create(value, expected,
                                                                                  schema_value, 9U);
  ASSERT_TRUE(cursor.has_value());
  const std::size_t total = cursor->pending_write().size();
  ASSERT_TRUE(cursor->consume_written(total / 2U).is_ok());
  EXPECT_EQ(cursor->written_bytes(), total / 2U);
  EXPECT_FALSE(cursor->complete());
  EXPECT_EQ(cursor->consume_written(total).code(), common::StatusCode::kInvalidArgument);
  ASSERT_TRUE(cursor->consume_written(cursor->pending_write().size()).is_ok());
  EXPECT_TRUE(cursor->complete());
  auto moved = std::move(*cursor);
  EXPECT_TRUE(cursor->complete());
  EXPECT_TRUE(cursor->pending_write().empty());
  EXPECT_TRUE(moved.complete());
}

} // namespace
} // namespace chronos::cluster
