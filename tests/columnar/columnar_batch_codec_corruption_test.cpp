#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/common/crc32c.hpp"
#include "columnar/columnar_test_support.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <vector>

namespace chronos::columnar {
namespace {

void store_u16_le(const common::MutableByteView bytes, const std::size_t offset,
                  const std::uint16_t value) {
  bytes[offset] = static_cast<std::byte>(value & 0xffU);
  bytes[offset + 1U] = static_cast<std::byte>((value >> 8U) & 0xffU);
}

void store_u32_le(const common::MutableByteView bytes, const std::size_t offset,
                  const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
  }
}

void store_u64_le(const common::MutableByteView bytes, const std::size_t offset,
                  const std::uint64_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
  }
}

[[nodiscard]] std::vector<std::byte> valid_batch() {
  const OwnedColumnarBatch batch =
      OwnedColumnarBatch::create(test::batch_schema(), test::batch_columns()).value();
  const EncodedColumnarBatch encoded = encode_columnar_batch_v1(batch).value();
  return {encoded.bytes().begin(), encoded.bytes().end()};
}

void refresh_header_crc(const common::MutableByteView bytes) {
  store_u32_le(bytes, format::kHeaderCrc32cOffset,
               common::crc32c(common::ByteView{bytes}.first(format::kHeaderCrc32cOffset)));
}

void refresh_batch_crc(const common::MutableByteView bytes) {
  store_u32_le(
      bytes, bytes.size() - format::kBatchTrailerLength,
      common::crc32c(common::ByteView{bytes}.first(bytes.size() - format::kBatchTrailerLength)));
}

void refresh_all(const common::MutableByteView bytes) {
  refresh_header_crc(bytes);
  refresh_batch_crc(bytes);
}

void expect_invalid(const common::ByteView bytes) {
  const auto decoded = decode_columnar_batch_v1_exact(bytes);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().kind(), ColumnarBatchDecodeErrorKind::kInvalid)
      << decoded.error().status().to_string();
}

TEST(ColumnarBatchCodecCorruptionTest, RejectsDamagedIntegrityBeforeTrustingLengths) {
  std::vector<std::byte> bytes = valid_batch();
  bytes[0] ^= std::byte{1U};
  expect_invalid(bytes);

  bytes = valid_batch();
  bytes[format::kHeaderCrc32cOffset] ^= std::byte{1U};
  expect_invalid(bytes);

  bytes = valid_batch();
  store_u64_le(bytes, 32U, format::kMaximumEmbeddedBatchLength);
  expect_invalid(common::ByteView{bytes}.first(format::kBatchHeaderLength));

  bytes = valid_batch();
  bytes.back() ^= std::byte{1U};
  expect_invalid(bytes);
}

TEST(ColumnarBatchCodecCorruptionTest, DistinguishesChecksumValidUnsupportedFeatures) {
  std::vector<std::byte> bytes = valid_batch();
  store_u16_le(bytes, 8U, 2U);
  refresh_all(bytes);
  auto decoded = decode_columnar_batch_v1_exact(bytes);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().kind(), ColumnarBatchDecodeErrorKind::kUnsupported);

  bytes = valid_batch();
  store_u16_le(bytes, format::kDescriptorsOffset + 16U, 19U);
  refresh_batch_crc(bytes);
  decoded = decode_columnar_batch_v1_exact(bytes);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().kind(), ColumnarBatchDecodeErrorKind::kUnsupported);

  bytes = valid_batch();
  store_u16_le(bytes, format::kDescriptorsOffset + 18U, 2U);
  refresh_batch_crc(bytes);
  decoded = decode_columnar_batch_v1_exact(bytes);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().kind(), ColumnarBatchDecodeErrorKind::kUnsupported);

  bytes = valid_batch();
  store_u32_le(bytes, format::kDescriptorsOffset + 24U, 2U);
  refresh_batch_crc(bytes);
  decoded = decode_columnar_batch_v1_exact(bytes);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().kind(), ColumnarBatchDecodeErrorKind::kUnsupported);
}

TEST(ColumnarBatchCodecCorruptionTest, RejectsEveryCanonicalLayoutContradiction) {
  std::vector<std::byte> bytes = valid_batch();
  store_u64_le(bytes, format::kDescriptorsOffset + 64U, 344U);
  refresh_batch_crc(bytes);
  expect_invalid(bytes);

  bytes = valid_batch();
  bytes[353] = std::byte{1U};
  refresh_batch_crc(bytes);
  expect_invalid(bytes);

  bytes = valid_batch();
  bytes[392] = std::byte{1U};
  refresh_batch_crc(bytes);
  expect_invalid(bytes);

  bytes = valid_batch();
  std::copy_n(bytes.begin() + static_cast<std::ptrdiff_t>(format::kDescriptorsOffset), 16U,
              bytes.begin() + static_cast<std::ptrdiff_t>(format::kDescriptorsOffset +
                                                          format::kColumnDescriptorLength));
  refresh_batch_crc(bytes);
  expect_invalid(bytes);

  bytes = valid_batch();
  store_u64_le(bytes, format::kDescriptorsOffset + format::kColumnDescriptorLength + 48U, 368U);
  refresh_batch_crc(bytes);
  expect_invalid(bytes);
}

TEST(ColumnarBatchCodecCorruptionTest, RejectsChecksumValidHostileValueDomains) {
  std::vector<std::byte> bytes = valid_batch();
  const std::size_t string_descriptor =
      format::kDescriptorsOffset + format::kColumnDescriptorLength;
  store_u32_le(bytes, string_descriptor + 28U, 0U);
  refresh_batch_crc(bytes);
  expect_invalid(bytes);

  bytes = valid_batch();
  bytes[376] = std::byte{0x80U};
  refresh_batch_crc(bytes);
  expect_invalid(bytes);

  bytes = valid_batch();
  bytes[384] = std::byte{0x80U};
  refresh_batch_crc(bytes);
  expect_invalid(bytes);

  bytes = valid_batch();
  std::fill_n(bytes.begin() + 40, 16U, std::byte{0U});
  refresh_all(bytes);
  expect_invalid(bytes);
}

TEST(ColumnarBatchCodecCorruptionTest, RejectsAllStructuralHeaderContradictionsWithValidChecksums) {
  using Mutation = void (*)(common::MutableByteView);
  constexpr std::array<Mutation, 7> kMutations{
      [](const common::MutableByteView bytes) { store_u32_le(bytes, 12U, 95U); },
      [](const common::MutableByteView bytes) { store_u32_le(bytes, 20U, 0U); },
      [](const common::MutableByteView bytes) { store_u32_le(bytes, 24U, 0U); },
      [](const common::MutableByteView bytes) { store_u32_le(bytes, 28U, 79U); },
      [](const common::MutableByteView bytes) { store_u64_le(bytes, 32U, 392U); },
      [](const common::MutableByteView bytes) { store_u64_le(bytes, 80U, 104U); },
      [](const common::MutableByteView bytes) { store_u32_le(bytes, 92U, 1U); },
  };
  for (const Mutation mutate : kMutations) {
    std::vector<std::byte> bytes = valid_batch();
    mutate(bytes);
    refresh_all(bytes);
    expect_invalid(bytes);
  }
}

} // namespace
} // namespace chronos::columnar
