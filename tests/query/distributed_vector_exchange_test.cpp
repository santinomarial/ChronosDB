#include "chronos/common/crc32c.hpp"
#include "chronos/query/distributed_vector_exchange.hpp"
#include "columnar/columnar_test_support.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <ranges>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

[[nodiscard]] schema::TabletId tablet(const std::uint8_t seed) {
  return schema::TabletId::from_uuid(uuid(seed)).value();
}

void store_u32_le(std::vector<std::byte>& bytes, const std::size_t offset,
                  const std::uint32_t value) {
  for (std::size_t index = 0U; index < 4U; ++index)
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
}

TEST(DistributedVectorExchangeTest, WrapsCanonicalAllTypeBatchAndEmptyTerminal) {
  const auto batch = columnar::OwnedColumnarBatch::create(columnar::test::batch_schema(),
                                                          columnar::test::batch_columns());
  ASSERT_TRUE(batch.has_value());
  const auto encoded_batch = columnar::encode_columnar_batch_v1(*batch);
  ASSERT_TRUE(encoded_batch.has_value());
  const DistributedVectorExchangeMessage message{
      .query_id = uuid(1U),
      .tablet_id = tablet(2U),
      .sequence = 3U,
      .terminal = true,
      .encoded_batch = {encoded_batch->bytes().begin(), encoded_batch->bytes().end()}};
  const auto encoded = encode_distributed_vector_exchange_message(message);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  EXPECT_EQ(encoded->bytes().size(), 80U + encoded_batch->size() + 4U);
  const auto decoded = decode_distributed_vector_exchange_message_exact(encoded->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->query_id, message.query_id);
  EXPECT_EQ(decoded->tablet_id, message.tablet_id);
  EXPECT_EQ(decoded->sequence, 3U);
  EXPECT_TRUE(decoded->terminal);
  EXPECT_TRUE(std::ranges::equal(decoded->encoded_batch, encoded_batch->bytes()));
  const auto nested = columnar::decode_columnar_batch_v1_exact(decoded->encoded_batch);
  ASSERT_TRUE(nested.has_value()) << nested.error().status().to_string();
  EXPECT_EQ(nested->row_count(), 2U);
  EXPECT_EQ(nested->columns().size(), 3U);

  const auto terminal = encode_distributed_vector_exchange_message(
      {.query_id = uuid(1U), .tablet_id = tablet(2U), .sequence = 4U, .terminal = true});
  ASSERT_TRUE(terminal.has_value());
  const auto decoded_terminal = decode_distributed_vector_exchange_message_exact(terminal->bytes());
  ASSERT_TRUE(decoded_terminal.has_value());
  EXPECT_TRUE(decoded_terminal->terminal);
  EXPECT_TRUE(decoded_terminal->encoded_batch.empty());
}

TEST(DistributedVectorExchangeTest, RejectsDamageBoundsAndNoncanonicalFrames) {
  const auto batch = columnar::OwnedColumnarBatch::create(columnar::test::batch_schema(),
                                                          columnar::test::batch_columns());
  ASSERT_TRUE(batch.has_value());
  const auto nested = columnar::encode_columnar_batch_v1(*batch);
  ASSERT_TRUE(nested.has_value());
  const auto encoded = encode_distributed_vector_exchange_message(
      {.query_id = uuid(1U),
       .tablet_id = tablet(2U),
       .sequence = 3U,
       .encoded_batch = {nested->bytes().begin(), nested->bytes().end()}});
  ASSERT_TRUE(encoded.has_value());
  std::vector<std::byte> bytes(encoded->bytes().begin(), encoded->bytes().end());
  EXPECT_EQ(decode_distributed_vector_exchange_message_exact(
                common::ByteView{bytes}.first(bytes.size() - 1U))
                .error()
                .code(),
            common::StatusCode::kCorruption);
  bytes[80U] ^= std::byte{1U};
  store_u32_le(bytes, bytes.size() - 4U,
               common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
  EXPECT_EQ(decode_distributed_vector_exchange_message_exact(bytes).error().code(),
            common::StatusCode::kCorruption);
  EXPECT_EQ(decode_distributed_vector_exchange_message_exact(
                encoded->bytes(), {.maximum_frame_length = encoded->bytes().size() - 1U})
                .error()
                .code(),
            common::StatusCode::kCorruption);
  EXPECT_EQ(encode_distributed_vector_exchange_message(
                {.query_id = uuid(1U), .tablet_id = tablet(2U), .sequence = 3U})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedVectorExchangeTest, OwnsBoundedPartialReadsAndShortWriteProgress) {
  const auto batch = columnar::OwnedColumnarBatch::create(columnar::test::batch_schema(),
                                                          columnar::test::batch_columns());
  ASSERT_TRUE(batch.has_value());
  const auto nested = columnar::encode_columnar_batch_v1(*batch);
  ASSERT_TRUE(nested.has_value());
  const DistributedVectorExchangeMessage first_message{
      .query_id = uuid(0x11U),
      .tablet_id = tablet(0x12U),
      .sequence = 1U,
      .encoded_batch = {nested->bytes().begin(), nested->bytes().end()}};
  const DistributedVectorExchangeMessage second_message{
      .query_id = uuid(0x11U), .tablet_id = tablet(0x12U), .sequence = 2U, .terminal = true};
  const auto first_encoded = encode_distributed_vector_exchange_message(first_message);
  const auto second_encoded = encode_distributed_vector_exchange_message(second_message);
  ASSERT_TRUE(first_encoded.has_value());
  ASSERT_TRUE(second_encoded.has_value());

  for (std::size_t split = 0U; split <= first_encoded->bytes().size(); ++split) {
    DistributedVectorExchangeReader reader;
    const auto prefix = reader.consume(first_encoded->bytes().first(split));
    ASSERT_TRUE(prefix.has_value()) << "split=" << split;
    EXPECT_EQ(prefix->consumed_bytes, split) << "split=" << split;
    EXPECT_EQ(prefix->message.has_value(), split == first_encoded->bytes().size())
        << "split=" << split;
    const auto suffix = reader.consume(first_encoded->bytes().subspan(split));
    ASSERT_TRUE(suffix.has_value()) << "split=" << split;
    EXPECT_EQ(suffix->consumed_bytes, first_encoded->bytes().size() - split) << "split=" << split;
    const DistributedVectorExchangeMessage* decoded =
        prefix->message.has_value() ? &*prefix->message : &*suffix->message;
    EXPECT_EQ(decoded->sequence, 1U) << "split=" << split;
    EXPECT_TRUE(std::ranges::equal(decoded->encoded_batch, nested->bytes())) << "split=" << split;
    EXPECT_EQ(reader.buffered_bytes(), 0U) << "split=" << split;
  }

  std::vector<std::byte> coalesced(first_encoded->bytes().begin(), first_encoded->bytes().end());
  coalesced.insert(coalesced.end(), second_encoded->bytes().begin(), second_encoded->bytes().end());
  DistributedVectorExchangeReader coalesced_reader;
  const auto first = coalesced_reader.consume(coalesced);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(first->message.has_value());
  EXPECT_EQ(first->consumed_bytes, first_encoded->bytes().size());
  const auto second =
      coalesced_reader.consume(common::ByteView{coalesced}.subspan(first->consumed_bytes));
  ASSERT_TRUE(second.has_value());
  ASSERT_TRUE(second->message.has_value());
  EXPECT_EQ(second->consumed_bytes, second_encoded->bytes().size());
  EXPECT_EQ(second->message->sequence, 2U);
  EXPECT_TRUE(second->message->terminal);

  std::vector<std::byte> corrupt(first_encoded->bytes().begin(), first_encoded->bytes().end());
  corrupt.front() ^= std::byte{1U};
  DistributedVectorExchangeReader failed_reader;
  const auto rejected = failed_reader.consume(corrupt);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_TRUE(failed_reader.failed());
  EXPECT_EQ(failed_reader.buffered_bytes(), distributed_vector_exchange_format::kHeaderLength);
  const auto repeated = failed_reader.consume(first_encoded->bytes());
  ASSERT_FALSE(repeated.has_value());
  EXPECT_EQ(repeated.error(), rejected.error());

  std::vector<std::byte> unsupported(first_encoded->bytes().begin(), first_encoded->bytes().end());
  unsupported[8U] = std::byte{2U};
  store_u32_le(unsupported, 72U, common::crc32c(common::ByteView{unsupported}.first(72U)));
  DistributedVectorExchangeReader unsupported_reader;
  EXPECT_EQ(unsupported_reader.consume(unsupported).error().code(),
            common::StatusCode::kNotSupported);

  DistributedVectorExchangeReader batch_limited_reader(
      {.batch = {.max_batch_length = nested->bytes().size() - 1U}});
  EXPECT_EQ(batch_limited_reader.consume(first_encoded->bytes()).error().code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(batch_limited_reader.buffered_bytes(),
            distributed_vector_exchange_format::kHeaderLength);
  DistributedVectorExchangeReader invalid_limits_reader({.maximum_frame_length = 1U});
  EXPECT_EQ(invalid_limits_reader.consume({}).error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_FALSE(invalid_limits_reader.failed());

  auto cursor = DistributedVectorExchangeWriteCursor::create(first_message);
  ASSERT_TRUE(cursor.has_value());
  EXPECT_TRUE(std::ranges::equal(cursor->pending_write(), first_encoded->bytes()));
  ASSERT_TRUE(cursor->consume_written(17U).is_ok());
  EXPECT_EQ(cursor->written_bytes(), 17U);
  EXPECT_TRUE(std::ranges::equal(cursor->pending_write(), first_encoded->bytes().subspan(17U)));
  EXPECT_EQ(cursor->consume_written(cursor->pending_write().size() + 1U).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(cursor->written_bytes(), 17U);
  DistributedVectorExchangeWriteCursor moved = std::move(*cursor);
  EXPECT_TRUE(cursor->complete());
  EXPECT_TRUE(cursor->pending_write().empty());
  ASSERT_TRUE(moved.consume_written(moved.pending_write().size()).is_ok());
  EXPECT_TRUE(moved.complete());
  EXPECT_TRUE(moved.consume_written(0U).is_ok());
}

} // namespace
} // namespace chronos::query
