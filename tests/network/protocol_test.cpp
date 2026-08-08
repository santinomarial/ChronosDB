#include "chronos/common/crc32c.hpp"
#include "chronos/network/protocol.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <vector>

namespace chronos::network {
namespace {

TEST(ProtocolFrameTest, EncodesTheAcceptedGoldenFrameAndRoundTrips) {
  const std::array<std::byte, 3> payload{std::byte{0x61}, std::byte{0x62}, std::byte{0x63}};
  const auto encoded = encode_frame({.message_type = MessageType::kQueryResult,
                                     .flags = kFrameFlagEndStream,
                                     .request_id = 0x0102030405060708ULL},
                                    payload);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  const std::array<std::byte, 43> expected{
      std::byte{0x43}, std::byte{0x44}, std::byte{0x42}, std::byte{0x31}, std::byte{0x01},
      std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x28}, std::byte{0x00},
      std::byte{0x15}, std::byte{0x00}, std::byte{0x01}, std::byte{0x00}, std::byte{0x00},
      std::byte{0x00}, std::byte{0x08}, std::byte{0x07}, std::byte{0x06}, std::byte{0x05},
      std::byte{0x04}, std::byte{0x03}, std::byte{0x02}, std::byte{0x01}, std::byte{0x03},
      std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0xb7}, std::byte{0x3f},
      std::byte{0x4b}, std::byte{0x36}, std::byte{0x00}, std::byte{0x00}, std::byte{0x00},
      std::byte{0x00}, std::byte{0x0f}, std::byte{0xd7}, std::byte{0x20}, std::byte{0x0b},
      std::byte{0x61}, std::byte{0x62}, std::byte{0x63}};
  EXPECT_EQ(*encoded, std::vector<std::byte>(expected.begin(), expected.end()));
  const auto decoded = decode_frame(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->header.message_type, MessageType::kQueryResult);
  EXPECT_EQ(decoded->header.flags, kFrameFlagEndStream);
  EXPECT_EQ(decoded->header.request_id, 0x0102030405060708ULL);
  EXPECT_EQ(decoded->payload, std::vector<std::byte>(payload.begin(), payload.end()));
}

TEST(ProtocolFrameTest, RejectsEveryTruncatedAndTrailingBoundary) {
  const std::array<std::byte, 4> payload{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};
  const std::vector<std::byte> encoded =
      *encode_frame({.message_type = MessageType::kIngestRequest, .request_id = 7U}, payload);
  for (std::size_t size = 0U; size < encoded.size(); ++size) {
    const auto decoded = decode_frame(common::ByteView{encoded}.first(size));
    EXPECT_FALSE(decoded.has_value()) << size;
    EXPECT_EQ(decoded.error().code(), common::StatusCode::kCorruption) << size;
  }
  std::vector<std::byte> trailing = encoded;
  trailing.push_back(std::byte{0});
  EXPECT_FALSE(decode_frame(trailing).has_value());
}

TEST(ProtocolFrameTest, RejectsHeaderAndPayloadCorruption) {
  const std::array<std::byte, 2> payload{std::byte{0xaa}, std::byte{0xbb}};
  const std::vector<std::byte> encoded =
      *encode_frame({.message_type = MessageType::kQueryRequest, .request_id = 11U}, payload);
  for (std::size_t byte = 0U; byte < kFrameHeaderSize; ++byte) {
    std::vector<std::byte> corrupted = encoded;
    corrupted[byte] ^= std::byte{1};
    EXPECT_FALSE(decode_frame(corrupted).has_value()) << byte;
  }
  std::vector<std::byte> corrupted = encoded;
  corrupted.back() ^= std::byte{1};
  EXPECT_FALSE(decode_frame(corrupted).has_value());
}

TEST(ProtocolFrameTest, EnforcesLimitsTypesAndFlagsBeforeAllocation) {
  EXPECT_FALSE(validate_protocol_limits({.maximum_payload_size = 0U}).is_ok());
  EXPECT_FALSE(
      validate_protocol_limits({.maximum_payload_size = kDefaultMaximumPayloadSize + 1U}).is_ok());
  const std::array<std::byte, 5> payload{};
  EXPECT_FALSE(
      encode_frame({.message_type = MessageType::kPing}, payload, {.maximum_payload_size = 4U})
          .has_value());
  EXPECT_FALSE(encode_frame({.message_type = MessageType::kPing, .flags = kFrameFlagEndStream}, {})
                   .has_value());
  EXPECT_FALSE(
      encode_frame({.message_type = MessageType::kPing, .flags = 1U << 31U}, {}).has_value());
}

TEST(ProtocolFrameTest, EmptyPayloadHasAnExactBoundedFrame) {
  const auto encoded = encode_frame({.message_type = MessageType::kPing}, {});
  ASSERT_TRUE(encoded.has_value());
  EXPECT_EQ(encoded->size(), kFrameHeaderSize);
  const auto decoded = decode_frame(*encoded);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_TRUE(decoded->payload.empty());
  EXPECT_EQ(decoded->header.payload_crc32c, common::crc32c({}));
}

} // namespace
} // namespace chronos::network
