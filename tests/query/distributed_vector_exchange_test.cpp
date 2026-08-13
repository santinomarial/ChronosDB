#include "chronos/common/crc32c.hpp"
#include "chronos/query/distributed_vector_exchange.hpp"
#include "columnar/columnar_test_support.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <ranges>
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

} // namespace
} // namespace chronos::query
