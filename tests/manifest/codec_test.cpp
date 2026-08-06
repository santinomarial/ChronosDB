#include "chronos/common/status.hpp"
#include "chronos/manifest/codec.hpp"
#include "chronos/manifest/format.hpp"
#include "manifest/manifest_test_support.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <string_view>
#include <vector>

namespace chronos::manifest {
namespace {

[[nodiscard]] std::uint8_t hex_nibble(const char value) {
  if (value >= '0' && value <= '9') {
    return static_cast<std::uint8_t>(value - '0');
  }
  return static_cast<std::uint8_t>(value - 'a' + 10);
}

[[nodiscard]] std::vector<std::byte> decode_hex(const std::string_view hex) {
  std::vector<std::byte> bytes;
  bytes.reserve(hex.size() / 2U);
  for (std::size_t offset = 0U; offset < hex.size(); offset += 2U) {
    bytes.push_back(static_cast<std::byte>(
        static_cast<std::uint8_t>((hex_nibble(hex[offset]) << 4U) | hex_nibble(hex[offset + 1U]))));
  }
  return bytes;
}

TEST(ManifestCodecTest, MatchesTheIndependentlyConstructedEmptyGenerationGolden) {
  // Constructed directly from docs/formats/manifest-v1.md with an independent CRC32C script.
  constexpr std::string_view golden_hex =
      "4348524e4d465354010000000001000000000000000000000801000000000000"
      "0100000000000000000000000000000000000000000000000000000000000000"
      "0000000000000000000000000000000000000000000000010000000000000000"
      "0000000000000002000000000000000001000000000000004000000000000000"
      "0001000000000000000100000000000000010000000000000001000000000000"
      "0000000000000000000000000000000000000000000000000000000000000000"
      "0000000000000000000000000000000000000000000000000000000000000000"
      "000000000000000000000000000000000000000000000000ad2ba17500000000"
      "000000005db5602b";
  const std::vector<std::byte> golden = decode_hex(golden_hex);
  ASSERT_EQ(golden.size(), 264U);
  const common::Result<EncodedManifest> encoded = encode_manifest_v1({
      .generation = 1U,
      .database_id = test::make_id<DatabaseId>(1U),
      .wal_id = test::make_wal_id(2U),
      .reclaim_checkpoint = {.record_sequence = 0U,
                             .segment_number = wal::kFirstSegmentNumber,
                             .byte_offset = wal::kSegmentHeaderSize},
      .tablets = {},
      .parts = {},
      .retries = {},
  });
  ASSERT_TRUE(encoded.has_value());
  EXPECT_TRUE(std::ranges::equal(encoded->bytes(), golden));
  const ManifestDecodeResult decoded = decode_manifest_v1_exact(golden);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->generation(), 1U);
}

TEST(ManifestCodecTest, RoundTripsCanonicalBorrowedDescriptorViewsDeterministically) {
  const test::ManifestFixture fixture;
  const EncodedManifest first = test::encode_fixture(fixture);
  const EncodedManifest second = test::encode_fixture(fixture);
  ASSERT_TRUE(std::ranges::equal(first.bytes(), second.bytes()));
  ASSERT_EQ(first.size(), 616U);

  const ManifestDecodeResult decoded = decode_manifest_v1_exact(first.bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().status().message();
  EXPECT_EQ(decoded->generation(), 2U);
  EXPECT_EQ(decoded->previous_generation(), 1U);
  EXPECT_EQ(decoded->database_id(), fixture.database_id);
  EXPECT_EQ(decoded->wal_id(), fixture.wal_id);
  EXPECT_EQ(decoded->reclaim_checkpoint(),
            (WalCheckpoint{.record_sequence = 5U, .segment_number = 1U, .byte_offset = 128U}));
  EXPECT_TRUE(std::ranges::equal(decoded->tablets(), fixture.tablets));
  EXPECT_TRUE(std::ranges::equal(decoded->parts(), fixture.parts));
  EXPECT_TRUE(std::ranges::equal(decoded->retries(), fixture.retries));
  EXPECT_EQ(decoded->encoded_bytes().data(), first.bytes().data());
  EXPECT_EQ(decoded->encoded_bytes().size(), first.size());
}

TEST(ManifestCodecTest, PrefixAndExactDecodingDistinguishTruncationAndTrailingBytes) {
  const test::ManifestFixture fixture;
  const EncodedManifest encoded = test::encode_fixture(fixture);
  for (std::size_t length = 0U; length < encoded.size(); ++length) {
    const ManifestDecodeResult decoded = decode_manifest_v1_prefix(encoded.bytes().first(length));
    ASSERT_FALSE(decoded.has_value()) << length;
    EXPECT_EQ(decoded.error().kind(), ManifestDecodeErrorKind::kIncomplete) << length;
    EXPECT_GT(decoded.error().required_size(), length) << length;
  }

  std::vector<std::byte> followed(encoded.bytes().begin(), encoded.bytes().end());
  followed.push_back(std::byte{0xa5});
  const ManifestDecodeResult prefix = decode_manifest_v1_prefix(followed);
  ASSERT_TRUE(prefix.has_value());
  EXPECT_EQ(prefix->encoded_bytes().size(), encoded.size());
  const ManifestDecodeResult exact = decode_manifest_v1_exact(followed);
  ASSERT_FALSE(exact.has_value());
  EXPECT_EQ(exact.error().kind(), ManifestDecodeErrorKind::kCorruption);
}

TEST(ManifestCodecTest, EmptyGenerationUsesTheExactInitialCheckpointAndMinimumLayout) {
  const DatabaseId database_id = test::make_id<DatabaseId>(1U);
  const wal::WalId wal_id = test::make_wal_id(2U);
  const common::Result<EncodedManifest> encoded = encode_manifest_v1({
      .generation = 1U,
      .database_id = database_id,
      .wal_id = wal_id,
      .reclaim_checkpoint = {.record_sequence = 0U,
                             .segment_number = wal::kFirstSegmentNumber,
                             .byte_offset = wal::kSegmentHeaderSize},
      .tablets = {},
      .parts = {},
      .retries = {},
  });
  ASSERT_TRUE(encoded.has_value());
  EXPECT_EQ(encoded->size(), 264U);
  const ManifestDecodeResult decoded = decode_manifest_v1_exact(encoded->bytes());
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->generation(), 1U);
  EXPECT_EQ(decoded->previous_generation(), 0U);
  EXPECT_TRUE(decoded->tablets().empty());
  EXPECT_TRUE(decoded->parts().empty());
  EXPECT_TRUE(decoded->retries().empty());
}

TEST(ManifestCodecTest, EnforcesRuntimeLimitsBeforeDescriptorAllocation) {
  const test::ManifestFixture fixture;
  const EncodedManifest encoded = test::encode_fixture(fixture);
  for (const ManifestDecodeLimits limits :
       {ManifestDecodeLimits{.max_file_length = encoded.size() - 1U},
        ManifestDecodeLimits{.max_tablets = 0U}, ManifestDecodeLimits{.max_parts = 0U},
        ManifestDecodeLimits{.max_retries = 0U}}) {
    const ManifestDecodeResult decoded = decode_manifest_v1_prefix(encoded.bytes(), limits);
    ASSERT_FALSE(decoded.has_value());
    EXPECT_EQ(decoded.error().kind(), ManifestDecodeErrorKind::kResourceLimit);
  }

  const ManifestDecodeResult invalid_limits = decode_manifest_v1_prefix(
      encoded.bytes(), {.max_file_length = format::kMaximumFileLength + 1U});
  ASSERT_FALSE(invalid_limits.has_value());
  EXPECT_EQ(invalid_limits.error().kind(), ManifestDecodeErrorKind::kResourceLimit);
}

TEST(ManifestCodecTest, EncoderRejectsNoncanonicalCrossDescriptorState) {
  test::ManifestFixture fixture;
  fixture.parts.front().tablet_id = test::make_id<schema::TabletId>(0xfeU);
  const common::Result<EncodedManifest> encoded = encode_manifest_v1(fixture.input());
  ASSERT_FALSE(encoded.has_value());
  EXPECT_EQ(encoded.error().code(), common::StatusCode::kInvalidArgument);
}

TEST(ManifestCodecTest, EncoderRejectsGlobalCoverageBeyondEveryTabletBoundary) {
  test::ManifestFixture fixture;
  ManifestEncodeInput input = fixture.input();
  input.reclaim_checkpoint.record_sequence = fixture.tablets.front().durable_record_sequence + 1U;
  const common::Result<EncodedManifest> encoded = encode_manifest_v1(input);
  ASSERT_FALSE(encoded.has_value());
  EXPECT_EQ(encoded.error().code(), common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::manifest
