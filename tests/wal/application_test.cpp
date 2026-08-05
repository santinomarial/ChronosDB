#include "chronos/wal/application.hpp"
#include "chronos/wal/types.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <gtest/gtest.h>
#include <vector>

namespace chronos::wal {
namespace {

TEST(WalApplicationEnvelopeTest, MatchesTheFrozenLittleEndianEnvelopeAndBorrowsBody) {
  const std::array<std::byte, 3U> body{std::byte{0xaa}, std::byte{0xbb}, std::byte{0xcc}};
  const auto encoded = encode_application_payload(
      ApplicationEnvelopeInput{.application_format = 1U,
                               .application_kind = 2U,
                               .application_flags = 0x0102030405060708ULL,
                               .application_body = body});
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  constexpr std::array<std::byte, 19U> expected{
      std::byte{0x01}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x02},
      std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x08}, std::byte{0x07},
      std::byte{0x06}, std::byte{0x05}, std::byte{0x04}, std::byte{0x03}, std::byte{0x02},
      std::byte{0x01}, std::byte{0xaa}, std::byte{0xbb}, std::byte{0xcc}};
  EXPECT_TRUE(std::ranges::equal(encoded->bytes(), expected));

  const auto decoded = decode_application_payload(encoded->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->application_format, 1U);
  EXPECT_EQ(decoded->application_kind, 2U);
  EXPECT_EQ(decoded->application_flags, 0x0102030405060708ULL);
  EXPECT_EQ(decoded->application_body.data(), encoded->bytes().data() + 16U);
  EXPECT_TRUE(std::ranges::equal(decoded->application_body, body));
}

TEST(WalApplicationEnvelopeTest, RejectsTruncationInvalidIdentitiesAndOversizeBodies) {
  const std::array<std::byte, 16U> header{};
  for (std::size_t size = 0U; size < header.size(); ++size) {
    const auto decoded = decode_application_payload(common::ByteView{header}.first(size));
    ASSERT_FALSE(decoded.has_value());
    EXPECT_EQ(decoded.error().code(), common::StatusCode::kCorruption);
  }
  EXPECT_EQ(decode_application_payload(header).error().code(), common::StatusCode::kCorruption);

  const std::array<std::byte, 1U> body{};
  EXPECT_EQ(encode_application_payload({0U, 1U, 0U, body}).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(encode_application_payload({1U, 0U, 0U, body}).error().code(),
            common::StatusCode::kInvalidArgument);

  std::vector<std::byte> maximum_body(kMaximumPayloadLength - kApplicationEnvelopeSize);
  EXPECT_TRUE(encode_application_payload({1U, 1U, 0U, maximum_body}).has_value());
  maximum_body.push_back(std::byte{0});
  EXPECT_EQ(encode_application_payload({1U, 1U, 0U, maximum_body}).error().code(),
            common::StatusCode::kOutOfRange);
}

} // namespace
} // namespace chronos::wal
