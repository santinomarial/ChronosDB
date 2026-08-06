#include "chronos/common/crc32c.hpp"
#include "chronos/manifest/codec.hpp"
#include "chronos/manifest/format.hpp"
#include "manifest/manifest_test_support.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <vector>

namespace chronos::manifest {
namespace {

void store_u16(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint16_t value) {
  bytes[offset] = static_cast<std::byte>(value & 0xffU);
  bytes[offset + 1U] = static_cast<std::byte>(value >> 8U);
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
  }
}

void store_u64(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint64_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
  }
}

void repair_file_crc(std::vector<std::byte>& bytes) {
  const std::size_t offset = bytes.size() - format::kFileCrc32cLength;
  store_u32(bytes, offset, common::crc32c(common::ByteView{bytes}.first(offset)));
}

void repair_header_and_file_crc(std::vector<std::byte>& bytes) {
  store_u32(bytes, format::kHeaderCrc32cOffset,
            common::crc32c(common::ByteView{bytes}.first(format::kHeaderCrc32cOffset)));
  repair_file_crc(bytes);
}

[[nodiscard]] std::vector<std::byte> fixture_bytes() {
  const test::ManifestFixture fixture;
  const EncodedManifest encoded = test::encode_fixture(fixture);
  return {encoded.bytes().begin(), encoded.bytes().end()};
}

void expect_kind(const std::vector<std::byte>& bytes, const ManifestDecodeErrorKind kind) {
  const ManifestDecodeResult decoded = decode_manifest_v1_exact(bytes);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().kind(), kind) << decoded.error().status().message();
}

TEST(ManifestCodecCorruptionTest, RejectsDamageBeforeTrustingHeaderLengths) {
  std::vector<std::byte> bytes = fixture_bytes();
  bytes[format::kTotalLengthOffset] ^= std::byte{1U};
  expect_kind(bytes, ManifestDecodeErrorKind::kCorruption);

  bytes = fixture_bytes();
  bytes[format::kHeaderCrc32cOffset] ^= std::byte{1U};
  expect_kind(bytes, ManifestDecodeErrorKind::kCorruption);
}

TEST(ManifestCodecCorruptionTest, DistinguishesChecksumValidUnsupportedSemantics) {
  std::vector<std::byte> bytes = fixture_bytes();
  store_u16(bytes, format::kFormatMajorOffset, 2U);
  repair_header_and_file_crc(bytes);
  expect_kind(bytes, ManifestDecodeErrorKind::kUnsupported);

  bytes = fixture_bytes();
  store_u32(bytes, format::kFileFlagsOffset, 1U);
  repair_header_and_file_crc(bytes);
  expect_kind(bytes, ManifestDecodeErrorKind::kUnsupported);

  bytes = fixture_bytes();
  store_u32(bytes, format::kTabletsOffset + format::kTabletFlagsOffset, 1U);
  repair_file_crc(bytes);
  expect_kind(bytes, ManifestDecodeErrorKind::kUnsupported);

  bytes = fixture_bytes();
  constexpr std::size_t retry_offset =
      format::kFileHeaderLength + format::kTabletDescriptorLength + format::kPartDescriptorLength;
  store_u32(bytes, retry_offset + format::kRetryFlagsOffset, 1U);
  repair_file_crc(bytes);
  expect_kind(bytes, ManifestDecodeErrorKind::kUnsupported);
}

TEST(ManifestCodecCorruptionTest, RejectsChecksumValidHeaderContradictions) {
  for (const auto mutation : {0U, 1U, 2U, 3U, 4U}) {
    std::vector<std::byte> bytes = fixture_bytes();
    switch (mutation) {
    case 0U:
      store_u64(bytes, format::kGenerationOffset, 0U);
      break;
    case 1U:
      store_u64(bytes, format::kPreviousGenerationOffset, 9U);
      break;
    case 2U:
      store_u64(bytes, format::kPartsOffsetFieldOffset, 0U);
      break;
    case 3U:
      store_u64(bytes, format::kReclaimByteOffsetOffset, 65U);
      break;
    case 4U:
      bytes[format::kDatabaseIdOffset + 15U] = std::byte{0U};
      break;
    default:
      FAIL() << "unreachable mutation";
    }
    repair_header_and_file_crc(bytes);
    expect_kind(bytes, ManifestDecodeErrorKind::kCorruption);
  }
}

TEST(ManifestCodecCorruptionTest, RejectsChecksumValidDescriptorContradictions) {
  constexpr std::size_t tablet_offset = format::kFileHeaderLength;
  constexpr std::size_t part_offset = tablet_offset + format::kTabletDescriptorLength;
  constexpr std::size_t retry_offset = part_offset + format::kPartDescriptorLength;
  for (const auto mutation : {0U, 1U, 2U, 3U, 4U, 5U}) {
    std::vector<std::byte> bytes = fixture_bytes();
    switch (mutation) {
    case 0U:
      store_u32(bytes, tablet_offset + format::kTabletReservedOffset, 1U);
      break;
    case 1U:
      store_u64(bytes, tablet_offset + format::kTabletPartCountOffset, 0U);
      break;
    case 2U:
      store_u64(bytes, part_offset + format::kPartFileLengthOffset, 1U);
      break;
    case 3U:
      store_u64(bytes, part_offset + format::kPartMaximumRecordSequenceOffset, 1'000U);
      break;
    case 4U:
      bytes[retry_offset + format::kRetryWalIdOffset + 15U] ^= std::byte{1U};
      break;
    case 5U:
      store_u32(bytes, retry_offset + format::kRetryAppliedRowCountOffset, 0U);
      break;
    default:
      FAIL() << "unreachable mutation";
    }
    repair_file_crc(bytes);
    expect_kind(bytes, ManifestDecodeErrorKind::kCorruption);
  }
}

} // namespace
} // namespace chronos::manifest
