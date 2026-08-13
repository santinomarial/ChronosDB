#include "chronos/cluster/distributed_vector_result_exchange.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/query/distributed_vector_exchange.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <ranges>
#include <string>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

[[nodiscard]] schema::TabletId tablet(const std::uint8_t seed) {
  return schema::TabletId::from_uuid(uuid(seed)).value();
}

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

[[nodiscard]] query::DistributedVectorResultSchema result_schema() {
  return {.columns = {{"value", type(schema::LogicalTypeKind::kInt64), false},
                      {"value", type(schema::LogicalTypeKind::kString), true}}};
}

[[nodiscard]] std::vector<std::byte> result_batch() {
  const query::DistributedVectorResultSchema schema = result_schema();
  const std::array<network::QueryResultColumn, 2U> columns{
      network::QueryResultColumn{schema.columns[0].name, schema.columns[0].type,
                                 schema.columns[0].nullable},
      network::QueryResultColumn{schema.columns[1].name, schema.columns[1].type,
                                 schema.columns[1].nullable}};
  const std::array<std::byte, 8U> one{std::byte{1U}};
  const std::array<std::byte, 5U> alpha{std::byte{'a'}, std::byte{'l'}, std::byte{'p'},
                                        std::byte{'h'}, std::byte{'a'}};
  const std::array<std::byte, 8U> two{std::byte{2U}};
  const std::array<network::QueryResultCell, 4U> cells{
      network::QueryResultCell{.value = one}, network::QueryResultCell{.value = alpha},
      network::QueryResultCell{.value = two},
      network::QueryResultCell{.is_null = true, .value = {}}};
  return network::encode_query_result_batch(2U, columns, cells).value();
}

void store_u32_le(std::vector<std::byte>& bytes, const std::size_t offset,
                  const std::uint32_t value) {
  for (std::size_t index = 0U; index < 4U; ++index)
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
}

void repair_exchange_checksums(std::vector<std::byte>& bytes) {
  store_u32_le(bytes, 72U, common::crc32c(common::ByteView{bytes}.first(72U)));
  store_u32_le(bytes, bytes.size() - 4U,
               common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

TEST(DistributedVectorResultExchangeTest, RoundTripsSchemaLightRowsAndTerminalOnlyStream) {
  const query::DistributedVectorResultSchema schema = result_schema();
  const std::vector<std::byte> batch = result_batch();
  const DistributedVectorResultExchangeMessage message{.query_id = uuid(1U),
                                                       .tablet_id = tablet(2U),
                                                       .sequence = 3U,
                                                       .terminal = true,
                                                       .encoded_result_batch = batch};
  const auto encoded = encode_distributed_vector_result_exchange_message_v2(message, schema);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  EXPECT_EQ(encoded->bytes().size(), 80U + batch.size() + 4U);
  const auto decoded =
      decode_distributed_vector_result_exchange_message_v2_exact(encoded->bytes(), schema);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->query_id, message.query_id);
  EXPECT_EQ(decoded->tablet_id, message.tablet_id);
  EXPECT_EQ(decoded->sequence, 3U);
  EXPECT_TRUE(decoded->terminal);
  EXPECT_EQ(decoded->encoded_result_batch, batch);
  const auto nested = network::decode_query_result_batch(decoded->encoded_result_batch);
  ASSERT_TRUE(nested.has_value());
  EXPECT_EQ(nested->row_count(), 2U);
  ASSERT_EQ(nested->columns().size(), 2U);
  EXPECT_EQ(nested->columns()[0].name, "value");
  EXPECT_EQ(nested->columns()[1].name, "value");
  ASSERT_NE(nested->cell(1U, 1U), nullptr);
  EXPECT_TRUE(nested->cell(1U, 1U)->is_null);

  const std::array<network::QueryResultColumn, 2U> columns{
      network::QueryResultColumn{schema.columns[0].name, schema.columns[0].type,
                                 schema.columns[0].nullable},
      network::QueryResultColumn{schema.columns[1].name, schema.columns[1].type,
                                 schema.columns[1].nullable}};
  const auto zero_row_batch = network::encode_query_result_batch(0U, columns, {});
  ASSERT_TRUE(zero_row_batch.has_value());
  const auto zero_row = encode_distributed_vector_result_exchange_message_v2(
      {.query_id = uuid(1U),
       .tablet_id = tablet(2U),
       .sequence = 4U,
       .encoded_result_batch = *zero_row_batch},
      schema);
  ASSERT_TRUE(zero_row.has_value());
  const auto decoded_zero_row =
      decode_distributed_vector_result_exchange_message_v2_exact(zero_row->bytes(), schema);
  ASSERT_TRUE(decoded_zero_row.has_value());
  EXPECT_EQ(network::decode_query_result_batch(decoded_zero_row->encoded_result_batch)->row_count(),
            0U);

  const auto terminal = encode_distributed_vector_result_exchange_message_v2(
      {.query_id = uuid(1U), .tablet_id = tablet(2U), .sequence = 5U, .terminal = true}, schema);
  ASSERT_TRUE(terminal.has_value());
  const auto decoded_terminal =
      decode_distributed_vector_result_exchange_message_v2_exact(terminal->bytes(), schema);
  ASSERT_TRUE(decoded_terminal.has_value());
  EXPECT_TRUE(decoded_terminal->terminal);
  EXPECT_TRUE(decoded_terminal->encoded_result_batch.empty());
}

TEST(DistributedVectorResultExchangeTest, RejectsSchemaMismatchDamageBoundsAndV1Confusion) {
  const query::DistributedVectorResultSchema schema = result_schema();
  const std::vector<std::byte> batch = result_batch();
  const auto encoded =
      encode_distributed_vector_result_exchange_message_v2({.query_id = uuid(1U),
                                                            .tablet_id = tablet(2U),
                                                            .sequence = 3U,
                                                            .encoded_result_batch = batch},
                                                           schema);
  ASSERT_TRUE(encoded.has_value());

  query::DistributedVectorResultSchema mismatch = schema;
  mismatch.columns.front().name = "other";
  EXPECT_EQ(encode_distributed_vector_result_exchange_message_v2({.query_id = uuid(1U),
                                                                  .tablet_id = tablet(2U),
                                                                  .sequence = 3U,
                                                                  .encoded_result_batch = batch},
                                                                 mismatch)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(decode_distributed_vector_result_exchange_message_v2_exact(encoded->bytes(), mismatch)
                .error()
                .code(),
            common::StatusCode::kCorruption);

  EXPECT_EQ(decode_distributed_vector_result_exchange_message_v2_exact(
                encoded->bytes().first(encoded->bytes().size() - 1U), schema)
                .error()
                .code(),
            common::StatusCode::kCorruption);
  EXPECT_EQ(decode_distributed_vector_result_exchange_message_v2_exact(
                encoded->bytes(), schema, {.maximum_frame_length = encoded->bytes().size() - 1U})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(decode_distributed_vector_result_exchange_message_v2_exact(
                encoded->bytes(), schema,
                {.result_batch = {.protocol = {.maximum_payload_size =
                                                   static_cast<std::uint32_t>(batch.size() - 1U)}}})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);

  std::vector<std::byte> nested_damage(encoded->bytes().begin(), encoded->bytes().end());
  nested_damage[80U] ^= std::byte{1U};
  repair_exchange_checksums(nested_damage);
  EXPECT_EQ(decode_distributed_vector_result_exchange_message_v2_exact(nested_damage, schema)
                .error()
                .code(),
            common::StatusCode::kCorruption);
  std::vector<std::byte> future(encoded->bytes().begin(), encoded->bytes().end());
  future[8U] = std::byte{3U};
  repair_exchange_checksums(future);
  EXPECT_EQ(
      decode_distributed_vector_result_exchange_message_v2_exact(future, schema).error().code(),
      common::StatusCode::kNotSupported);

  const auto v1 = query::encode_distributed_vector_exchange_message(
      {.query_id = uuid(1U), .tablet_id = tablet(2U), .sequence = 3U, .terminal = true});
  ASSERT_TRUE(v1.has_value());
  EXPECT_EQ(decode_distributed_vector_result_exchange_message_v2_exact(v1->bytes(), schema)
                .error()
                .code(),
            common::StatusCode::kCorruption);
  EXPECT_EQ(encode_distributed_vector_result_exchange_message_v2(
                {.query_id = uuid(1U), .tablet_id = tablet(2U), .sequence = 3U}, schema)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedVectorResultExchangeTest, OwnsBoundedPartialReadsAndShortWriteProgress) {
  const query::DistributedVectorResultSchema schema = result_schema();
  const std::vector<std::byte> batch = result_batch();
  const DistributedVectorResultExchangeMessage first_message{.query_id = uuid(0x11U),
                                                             .tablet_id = tablet(0x12U),
                                                             .sequence = 1U,
                                                             .encoded_result_batch = batch};
  const DistributedVectorResultExchangeMessage second_message{
      .query_id = uuid(0x11U), .tablet_id = tablet(0x12U), .sequence = 2U, .terminal = true};
  const auto first_encoded =
      encode_distributed_vector_result_exchange_message_v2(first_message, schema);
  const auto second_encoded =
      encode_distributed_vector_result_exchange_message_v2(second_message, schema);
  ASSERT_TRUE(first_encoded.has_value());
  ASSERT_TRUE(second_encoded.has_value());

  for (std::size_t split = 0U; split <= first_encoded->bytes().size(); ++split) {
    DistributedVectorResultExchangeReader reader{result_schema()};
    const auto prefix = reader.consume(first_encoded->bytes().first(split));
    ASSERT_TRUE(prefix.has_value()) << "split=" << split;
    EXPECT_EQ(prefix->consumed_bytes, split) << "split=" << split;
    EXPECT_EQ(prefix->message.has_value(), split == first_encoded->bytes().size())
        << "split=" << split;
    const auto suffix = reader.consume(first_encoded->bytes().subspan(split));
    ASSERT_TRUE(suffix.has_value()) << "split=" << split;
    const DistributedVectorResultExchangeMessage* decoded =
        prefix->message.has_value() ? &*prefix->message : &*suffix->message;
    EXPECT_EQ(decoded->sequence, 1U) << "split=" << split;
    EXPECT_EQ(decoded->encoded_result_batch, batch) << "split=" << split;
    EXPECT_EQ(reader.buffered_bytes(), 0U) << "split=" << split;
  }

  std::vector<std::byte> coalesced(first_encoded->bytes().begin(), first_encoded->bytes().end());
  coalesced.insert(coalesced.end(), second_encoded->bytes().begin(), second_encoded->bytes().end());
  DistributedVectorResultExchangeReader coalesced_reader{result_schema()};
  const auto first = coalesced_reader.consume(coalesced);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(first->message.has_value());
  EXPECT_EQ(first->consumed_bytes, first_encoded->bytes().size());
  const auto second =
      coalesced_reader.consume(common::ByteView{coalesced}.subspan(first->consumed_bytes));
  ASSERT_TRUE(second.has_value());
  ASSERT_TRUE(second->message.has_value());
  EXPECT_TRUE(second->message->terminal);

  std::vector<std::byte> corrupt(first_encoded->bytes().begin(), first_encoded->bytes().end());
  corrupt.front() ^= std::byte{1U};
  DistributedVectorResultExchangeReader failed_reader{result_schema()};
  const auto rejected = failed_reader.consume(corrupt);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_TRUE(failed_reader.failed());
  EXPECT_EQ(failed_reader.consume(first_encoded->bytes()).error(), rejected.error());

  DistributedVectorResultExchangeReader limited_reader{
      result_schema(),
      {.result_batch = {
           .protocol = {.maximum_payload_size = static_cast<std::uint32_t>(batch.size() - 1U)}}}};
  EXPECT_EQ(limited_reader.consume(first_encoded->bytes()).error().code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_TRUE(limited_reader.failed());

  auto cursor = DistributedVectorResultExchangeWriteCursor::create(first_message, schema);
  ASSERT_TRUE(cursor.has_value());
  EXPECT_TRUE(std::ranges::equal(cursor->pending_write(), first_encoded->bytes()));
  ASSERT_TRUE(cursor->consume_written(17U).is_ok());
  EXPECT_EQ(cursor->written_bytes(), 17U);
  EXPECT_EQ(cursor->consume_written(cursor->pending_write().size() + 1U).code(),
            common::StatusCode::kInvalidArgument);
  DistributedVectorResultExchangeWriteCursor moved = std::move(*cursor);
  EXPECT_TRUE(cursor->complete());
  EXPECT_TRUE(cursor->pending_write().empty());
  ASSERT_TRUE(moved.consume_written(moved.pending_write().size()).is_ok());
  EXPECT_TRUE(moved.complete());
}

} // namespace
} // namespace chronos::cluster
