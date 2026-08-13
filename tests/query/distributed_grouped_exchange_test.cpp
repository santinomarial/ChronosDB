#include "chronos/common/byte_reader.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/query/distributed_grouped_exchange.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <limits>
#include <optional>
#include <ranges>
#include <vector>

namespace chronos::query {
namespace {

using Frame = std::array<std::byte, grouped_float64_exchange_format::kFrameLength>;

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

[[nodiscard]] schema::TabletId tablet(const std::uint8_t seed) {
  return schema::TabletId::from_uuid(uuid(seed)).value();
}

void store_u16_le(Frame& bytes, const std::size_t offset, const std::uint16_t value) {
  bytes[offset] = static_cast<std::byte>(value & 0xffU);
  bytes[offset + 1U] = static_cast<std::byte>((value >> 8U) & 0xffU);
}

void store_u32_le(Frame& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < 4U; ++index)
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
}

void store_u64_le(Frame& bytes, const std::size_t offset, const std::uint64_t value) {
  for (std::size_t index = 0U; index < 8U; ++index)
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
}

void rewrite_crc(Frame& bytes) {
  store_u32_le(bytes, bytes.size() - 4U,
               common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

[[nodiscard]] Frame copy_encoded(const EncodedGroupedFloat64ExchangeMessage& encoded) {
  Frame bytes{};
  std::ranges::copy(encoded.bytes(), bytes.begin());
  return bytes;
}

[[nodiscard]] GroupedFloat64ExchangeMessage message(std::optional<double> key) {
  return {
      .query_id = uuid(0x11U),
      .tablet_id = tablet(0x22U),
      .sequence = 0x0102'0304'0506'0708ULL,
      .group_key = key,
      .partial = {.count = 2U, .sum = 3.0, .minimum = 1.0, .maximum = 2.0, .mean = 1.5, .m2 = 0.5},
      .terminal = true};
}

TEST(DistributedGroupedExchangeTest, FreezesLayoutAndCanonicalizesFloatGroupingKeys) {
  const auto encoded = encode_grouped_float64_exchange_message(message(-0.0));
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  common::ByteReader reader{encoded->bytes()};
  const std::array<std::byte, 8U> magic{std::byte{'C'}, std::byte{'H'}, std::byte{'D'},
                                        std::byte{'X'}, std::byte{'G'}, std::byte{'R'},
                                        std::byte{'P'}, std::byte{'1'}};
  EXPECT_TRUE(std::ranges::equal(reader.read_exact(8U).value(), magic));
  EXPECT_EQ(reader.read_u16_le().value(), grouped_float64_exchange_format::kMajor);
  EXPECT_EQ(reader.read_u16_le().value(), grouped_float64_exchange_format::kMinor);
  EXPECT_EQ(reader.read_u32_le().value(), grouped_float64_exchange_format::kFrameLength);
  EXPECT_EQ(reader.read_exact(16U).value().front(), std::byte{0x11U});
  EXPECT_EQ(reader.read_exact(16U).value().front(), std::byte{0x22U});
  EXPECT_EQ(reader.read_u64_le().value(), 0x0102'0304'0506'0708ULL);
  EXPECT_EQ(std::bit_cast<std::uint64_t>(reader.read_float64_le().value()), 0U);
  EXPECT_EQ(reader.read_u64_le().value(), 2U);
  EXPECT_EQ(reader.read_float64_le().value(), 3.0);
  EXPECT_EQ(reader.read_float64_le().value(), 1.0);
  EXPECT_EQ(reader.read_float64_le().value(), 2.0);
  EXPECT_EQ(reader.read_float64_le().value(), 1.5);
  EXPECT_EQ(reader.read_float64_le().value(), 0.5);
  EXPECT_EQ(reader.read_u32_le().value(), 0x0fU);
  const auto reserved = reader.read_exact(16U);
  ASSERT_TRUE(reserved.has_value());
  EXPECT_TRUE(
      std::ranges::all_of(*reserved, [](const std::byte value) { return value == std::byte{0U}; }));
  const auto stored_crc = reader.read_u32_le();
  ASSERT_TRUE(stored_crc.has_value());
  EXPECT_EQ(*stored_crc, common::crc32c(encoded->bytes().first(encoded->bytes().size() - 4U)));
  EXPECT_TRUE(reader.empty());

  auto decoded = decode_grouped_float64_exchange_message_exact(encoded->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  ASSERT_TRUE(decoded->group_key.has_value());
  EXPECT_EQ(std::bit_cast<std::uint64_t>(*decoded->group_key), 0U);
  EXPECT_EQ(decoded->partial.count, 2U);
  EXPECT_EQ(decoded->partial.sum, 3.0);
  EXPECT_TRUE(decoded->terminal);

  const double payload_nan = std::bit_cast<double>(0xfff0'0000'0000'0001ULL);
  const auto nan_encoded = encode_grouped_float64_exchange_message(message(payload_nan));
  ASSERT_TRUE(nan_encoded.has_value());
  const auto nan_decoded = decode_grouped_float64_exchange_message_exact(nan_encoded->bytes());
  ASSERT_TRUE(nan_decoded.has_value());
  ASSERT_TRUE(nan_decoded->group_key.has_value());
  EXPECT_EQ(std::bit_cast<std::uint64_t>(*nan_decoded->group_key),
            grouped_float64_exchange_format::kCanonicalQuietNanBits);
  const double second_nan = std::bit_cast<double>(0x7ff8'0000'0000'1234ULL);
  const auto second_nan_encoded = encode_grouped_float64_exchange_message(message(second_nan));
  ASSERT_TRUE(second_nan_encoded.has_value());
  EXPECT_TRUE(std::ranges::equal(nan_encoded->bytes(), second_nan_encoded->bytes()));

  const auto null_encoded = encode_grouped_float64_exchange_message(message(std::nullopt));
  ASSERT_TRUE(null_encoded.has_value());
  const auto null_decoded = decode_grouped_float64_exchange_message_exact(null_encoded->bytes());
  ASSERT_TRUE(null_decoded.has_value());
  EXPECT_FALSE(null_decoded->group_key.has_value());
}

TEST(DistributedGroupedExchangeTest, RejectsDamageVersionsAndNoncanonicalRepresentations) {
  const auto encoded = encode_grouped_float64_exchange_message(message(1.0));
  ASSERT_TRUE(encoded.has_value());
  const Frame canonical = copy_encoded(*encoded);
  EXPECT_EQ(decode_grouped_float64_exchange_message_exact(
                common::ByteView{canonical}.first(canonical.size() - 1U))
                .error()
                .code(),
            common::StatusCode::kCorruption);
  std::vector<std::byte> trailing(canonical.begin(), canonical.end());
  trailing.push_back(std::byte{0U});
  EXPECT_EQ(decode_grouped_float64_exchange_message_exact(trailing).error().code(),
            common::StatusCode::kCorruption);

  Frame damaged = canonical;
  damaged[64U] ^= std::byte{1U};
  EXPECT_EQ(decode_grouped_float64_exchange_message_exact(damaged).error().code(),
            common::StatusCode::kCorruption);

  Frame future = canonical;
  store_u16_le(future, 8U, grouped_float64_exchange_format::kMajor + 1U);
  rewrite_crc(future);
  EXPECT_EQ(decode_grouped_float64_exchange_message_exact(future).error().code(),
            common::StatusCode::kNotSupported);

  Frame negative_zero = canonical;
  for (std::size_t index = 56U; index < 64U; ++index)
    negative_zero[index] = std::byte{0U};
  negative_zero[63U] = std::byte{0x80U};
  rewrite_crc(negative_zero);
  EXPECT_EQ(decode_grouped_float64_exchange_message_exact(negative_zero).error().code(),
            common::StatusCode::kCorruption);

  Frame noncanonical_nan = canonical;
  store_u64_le(noncanonical_nan, 56U, 0x7ff8'0000'0000'1234ULL);
  rewrite_crc(noncanonical_nan);
  EXPECT_EQ(decode_grouped_float64_exchange_message_exact(noncanonical_nan).error().code(),
            common::StatusCode::kCorruption);

  const auto null_encoded = encode_grouped_float64_exchange_message(message(std::nullopt));
  ASSERT_TRUE(null_encoded.has_value());
  Frame null_nonzero = copy_encoded(*null_encoded);
  null_nonzero[56U] = std::byte{1U};
  rewrite_crc(null_nonzero);
  EXPECT_EQ(decode_grouped_float64_exchange_message_exact(null_nonzero).error().code(),
            common::StatusCode::kCorruption);

  Frame reserved = canonical;
  reserved[116U] = std::byte{1U};
  rewrite_crc(reserved);
  EXPECT_EQ(decode_grouped_float64_exchange_message_exact(reserved).error().code(),
            common::StatusCode::kCorruption);

  GroupedFloat64ExchangeMessage invalid = message(1.0);
  invalid.partial = {.count = 0U, .sum = -0.0};
  EXPECT_EQ(encode_grouped_float64_exchange_message(invalid).error().code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::query
