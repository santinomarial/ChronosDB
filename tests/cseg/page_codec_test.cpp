#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/common/status.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/cseg/page_codec.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/logical_type.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace chronos::cseg {
namespace {

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

[[nodiscard]] CsegColumnDescriptor column(const schema::LogicalType logical_type,
                                          const bool nullable) {
  common::Uuid::Bytes bytes{};
  bytes.back() = std::byte{1U};
  return {.column_id = schema::ColumnId::from_bytes(bytes).value(),
          .storage_kind = StorageKind::kUser,
          .logical_type = logical_type,
          .nullable = nullable,
          .event_time = false,
          .schema_ordinal = 0U,
          .ordering_ordinal = std::nullopt};
}

void append_u32(std::vector<std::byte>& bytes, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes.push_back(std::byte{static_cast<std::uint8_t>(value >> (index * 8U))});
  }
}

[[nodiscard]] CsegPageDescriptor descriptor(const EncodedCsegPage& encoded) {
  const CsegPageMetadataInput metadata = encoded.metadata();
  return {.granule_ordinal = 0U,
          .stored_column_ordinal = 0U,
          .compression = metadata.compression,
          .row_count = metadata.row_count,
          .null_count = metadata.null_count,
          .page_offset = 0U,
          .stored_length = metadata.stored_length,
          .uncompressed_length = metadata.uncompressed_length,
          .validity_length = metadata.validity_length,
          .offsets_length = metadata.offsets_length,
          .values_length = metadata.values_length,
          .page_crc32c = metadata.page_crc32c};
}

TEST(CsegPageCodecTest, RawGoldenComputesStoredCrcAndDecodesAsAnExactBorrowedView) {
  const std::array<std::byte, 1U> validity{std::byte{0x05U}};
  const std::array<std::byte, 16U> offsets{
      std::byte{0U}, std::byte{0U}, std::byte{0U}, std::byte{0U}, std::byte{1U}, std::byte{0U},
      std::byte{0U}, std::byte{0U}, std::byte{1U}, std::byte{0U}, std::byte{0U}, std::byte{0U},
      std::byte{3U}, std::byte{0U}, std::byte{0U}, std::byte{0U},
  };
  const std::array<std::byte, 3U> values{std::byte{'a'}, std::byte{'b'}, std::byte{'c'}};
  const schema::LogicalType logical_type = type(schema::LogicalTypeKind::kString);
  const auto physical = columnar::PhysicalColumnView::create(
      {.type = logical_type, .nullable = true, .row_count = 3U, .null_count = 1U},
      {.validity = validity, .offsets = offsets, .values = values});
  ASSERT_TRUE(physical.has_value());
  const auto encoded = encode_cseg_v1_page(*physical, PageCompression::kNone);
  ASSERT_TRUE(encoded.has_value());
  EXPECT_EQ(encoded->compression(), PageCompression::kNone);
  EXPECT_EQ(encoded->size(), 20U);
  // Independently generated with a bit-at-a-time reflected Castagnoli implementation over the
  // literal validity/offset/value bytes above.
  EXPECT_EQ(encoded->page_crc32c(), 0x485c9c22U);
  EXPECT_EQ(encoded->metadata().validity_length, 1U);
  EXPECT_EQ(encoded->metadata().offsets_length, 16U);
  EXPECT_EQ(encoded->metadata().values_length, 3U);

  auto decoded =
      decode_cseg_v1_page(encoded->bytes(), column(logical_type, true), descriptor(*encoded));
  ASSERT_TRUE(decoded.has_value());
  EXPECT_FALSE(decoded->owns_uncompressed_bytes());
  EXPECT_EQ(decoded->uncompressed_bytes().data(), encoded->bytes().data());
  EXPECT_EQ(decoded->physical().values().data(), encoded->bytes().data() + 17U);
  EXPECT_TRUE(decoded->physical().cell(1U)->is_null());
  EXPECT_EQ(decoded->physical().cell(2U)->bytes()->size(), 2U);

  const CsegPageDescriptor page = descriptor(*encoded);
  for (std::size_t size = 0U; size < encoded->size(); ++size) {
    const auto truncated =
        decode_cseg_v1_page(encoded->bytes().first(size), column(logical_type, true), page);
    ASSERT_FALSE(truncated.has_value()) << size;
    EXPECT_EQ(truncated.error().code(), common::StatusCode::kCorruption) << size;
  }
  std::vector<std::byte> suffixed(encoded->bytes().begin(), encoded->bytes().end());
  suffixed.push_back(std::byte{0U});
  const auto trailing = decode_cseg_v1_page(suffixed, column(logical_type, true), page);
  ASSERT_FALSE(trailing.has_value());
  EXPECT_EQ(trailing.error().code(), common::StatusCode::kCorruption);
}

TEST(CsegPageCodecTest, ZstdIsDeterministicAndDecodedOutputOwnsStablePhysicalBytes) {
  std::vector<std::byte> offsets;
  append_u32(offsets, 0U);
  append_u32(offsets, 16'384U);
  const std::vector<std::byte> values(16'384U, std::byte{'x'});
  const schema::LogicalType logical_type = type(schema::LogicalTypeKind::kString);
  const auto physical = columnar::PhysicalColumnView::create(
      {.type = logical_type, .nullable = false, .row_count = 1U, .null_count = 0U},
      {.validity = {}, .offsets = offsets, .values = values});
  ASSERT_TRUE(physical.has_value());
  const auto first = encode_cseg_v1_page(*physical, PageCompression::kZstd);
  const auto second = encode_cseg_v1_page(*physical, PageCompression::kZstd);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  ASSERT_EQ(first->compression(), PageCompression::kZstd);
  EXPECT_TRUE(std::ranges::equal(first->bytes(), second->bytes()));
  EXPECT_EQ(first->page_crc32c(), second->page_crc32c());

  auto decoded =
      decode_cseg_v1_page(first->bytes(), column(logical_type, false), descriptor(*first));
  ASSERT_TRUE(decoded.has_value());
  ASSERT_TRUE(decoded->owns_uncompressed_bytes());
  EXPECT_EQ(decoded->uncompressed_bytes().size(), values.size() + offsets.size());
  EXPECT_NE(decoded->uncompressed_bytes().data(), first->bytes().data());
  const common::ByteView before_move = decoded->physical().values();
  DecodedCsegPage moved = std::move(*decoded);
  EXPECT_EQ(moved.physical().values().data(), before_move.data());
  EXPECT_TRUE(std::ranges::equal(moved.physical().values(), values));
}

TEST(CsegPageCodecTest, RejectsCorruptionAndUnsupportedCompressionBeforeInterpretation) {
  const std::array<std::byte, 1U> values{std::byte{1U}};
  const schema::LogicalType logical_type = type(schema::LogicalTypeKind::kUInt8);
  const auto physical = columnar::PhysicalColumnView::create(
      {.type = logical_type, .nullable = false, .row_count = 1U, .null_count = 0U},
      {.validity = {}, .offsets = {}, .values = values});
  ASSERT_TRUE(physical.has_value());
  const auto encoded = encode_cseg_v1_page(*physical, PageCompression::kNone);
  ASSERT_TRUE(encoded.has_value());
  CsegPageDescriptor page = descriptor(*encoded);
  std::array<std::byte, 1U> corrupt{std::byte{0U}};
  const auto bad_crc = decode_cseg_v1_page(corrupt, column(logical_type, false), page);
  ASSERT_FALSE(bad_crc.has_value());
  EXPECT_EQ(bad_crc.error().code(), common::StatusCode::kCorruption);
  EXPECT_NE(bad_crc.error().message().find("CRC32C"), std::string::npos);

  // Exercise the defensive typed-input check that follows metadata parsing in normal use.
  // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
  page.compression = static_cast<PageCompression>(0U);
  const auto zero = decode_cseg_v1_page(encoded->bytes(), column(logical_type, false), page);
  ASSERT_FALSE(zero.has_value());
  EXPECT_EQ(zero.error().code(), common::StatusCode::kCorruption);

  // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
  page.compression = static_cast<PageCompression>(3U);
  const auto unsupported = decode_cseg_v1_page(encoded->bytes(), column(logical_type, false), page);
  ASSERT_FALSE(unsupported.has_value());
  EXPECT_EQ(unsupported.error().code(), common::StatusCode::kNotSupported);

  page = descriptor(*encoded);
  page.compression = PageCompression::kZstd;
  page.page_crc32c = common::crc32c(encoded->bytes());
  const auto malformed = decode_cseg_v1_page(encoded->bytes(), column(logical_type, false), page);
  ASSERT_FALSE(malformed.has_value());
  EXPECT_EQ(malformed.error().code(), common::StatusCode::kCorruption);
}

TEST(CsegPageCodecPropertyTest, EveryStoredByteMutationAndChecksumValidDirtyPayloadFailsClosed) {
  std::vector<std::byte> values(257U);
  for (std::size_t index = 0U; index < values.size(); ++index) {
    values[index] = std::byte{static_cast<std::uint8_t>((index * 29U) & 0xffU)};
  }
  const schema::LogicalType logical_type = type(schema::LogicalTypeKind::kUInt8);
  const auto physical =
      columnar::PhysicalColumnView::create({.type = logical_type,
                                            .nullable = false,
                                            .row_count = static_cast<std::uint32_t>(values.size()),
                                            .null_count = 0U},
                                           {.validity = {}, .offsets = {}, .values = values});
  ASSERT_TRUE(physical.has_value());
  const auto encoded = encode_cseg_v1_page(*physical, PageCompression::kNone);
  ASSERT_TRUE(encoded.has_value());
  for (std::size_t index = 0U; index < encoded->size(); ++index) {
    std::vector<std::byte> mutated(encoded->bytes().begin(), encoded->bytes().end());
    mutated[index] ^= std::byte{1U};
    const auto decoded =
        decode_cseg_v1_page(mutated, column(logical_type, false), descriptor(*encoded));
    ASSERT_FALSE(decoded.has_value()) << index;
    EXPECT_EQ(decoded.error().code(), common::StatusCode::kCorruption) << index;
  }

  const schema::LogicalType bool_type = type(schema::LogicalTypeKind::kBool);
  const std::array<std::byte, 2U> dirty_bool{std::byte{0U}, std::byte{1U}};
  CsegPageDescriptor dirty_page{.granule_ordinal = 0U,
                                .stored_column_ordinal = 0U,
                                .compression = PageCompression::kNone,
                                .row_count = 1U,
                                .null_count = 1U,
                                .page_offset = 0U,
                                .stored_length = dirty_bool.size(),
                                .uncompressed_length = dirty_bool.size(),
                                .validity_length = 1U,
                                .offsets_length = 0U,
                                .values_length = 1U,
                                .page_crc32c = common::crc32c(dirty_bool)};
  const auto dirty = decode_cseg_v1_page(dirty_bool, column(bool_type, true), dirty_page);
  ASSERT_FALSE(dirty.has_value());
  EXPECT_EQ(dirty.error().code(), common::StatusCode::kCorruption);
}

TEST(CsegPageCodecPropertyTest, GeneratedRowsAndBothPoliciesRoundTripDeterministically) {
  const schema::LogicalType logical_type = type(schema::LogicalTypeKind::kUInt8);
  for (std::uint32_t rows = 1U; rows <= 257U; rows += 16U) {
    std::vector<std::byte> values(rows);
    for (std::uint32_t row = 0U; row < rows; ++row) {
      values[row] = std::byte{static_cast<std::uint8_t>((row * 17U + rows) & 0xffU)};
    }
    const auto physical = columnar::PhysicalColumnView::create(
        {.type = logical_type, .nullable = false, .row_count = rows, .null_count = 0U},
        {.validity = {}, .offsets = {}, .values = values});
    ASSERT_TRUE(physical.has_value()) << rows;
    for (const PageCompression policy : {PageCompression::kNone, PageCompression::kZstd}) {
      const auto first = encode_cseg_v1_page(*physical, policy);
      const auto second = encode_cseg_v1_page(*physical, policy);
      ASSERT_TRUE(first.has_value()) << rows;
      ASSERT_TRUE(second.has_value()) << rows;
      EXPECT_TRUE(std::ranges::equal(first->bytes(), second->bytes())) << rows;
      EXPECT_EQ(first->metadata(), second->metadata()) << rows;
      const auto decoded =
          decode_cseg_v1_page(first->bytes(), column(logical_type, false), descriptor(*first));
      ASSERT_TRUE(decoded.has_value()) << rows;
      EXPECT_TRUE(std::ranges::equal(decoded->physical().values(), values)) << rows;
      EXPECT_EQ(decoded->owns_uncompressed_bytes(), first->compression() == PageCompression::kZstd)
          << rows;
    }
  }
}

TEST(CsegPageCodecTest, EnforcesFixedBoundsBeforeChecksumOrProviderAccess) {
  const schema::LogicalType logical_type = type(schema::LogicalTypeKind::kUInt8);
  std::byte sentinel{1U};
  CsegPageDescriptor page{.granule_ordinal = 0U,
                          .stored_column_ordinal = 0U,
                          .compression = PageCompression::kNone,
                          .row_count = 1U,
                          .null_count = 0U,
                          .page_offset = 0U,
                          .stored_length = format::kMaximumStoredPageLength + 1U,
                          .uncompressed_length = 1U,
                          .validity_length = 0U,
                          .offsets_length = 0U,
                          .values_length = 1U,
                          .page_crc32c = 0U};
  const common::ByteView oversized{&sentinel, format::kMaximumStoredPageLength + 1U};
  const auto stored = decode_cseg_v1_page(oversized, column(logical_type, false), page);
  ASSERT_FALSE(stored.has_value());
  EXPECT_EQ(stored.error().code(), common::StatusCode::kCorruption);

  page.stored_length = 1U;
  page.uncompressed_length = format::kMaximumUncompressedPageLength + 1U;
  const auto uncompressed =
      decode_cseg_v1_page(common::ByteView{&sentinel, 1U}, column(logical_type, false), page);
  ASSERT_FALSE(uncompressed.has_value());
  EXPECT_EQ(uncompressed.error().code(), common::StatusCode::kCorruption);

  const auto physical = columnar::PhysicalColumnView::create(
      {.type = logical_type, .nullable = false, .row_count = 1U, .null_count = 0U},
      {.validity = {}, .offsets = {}, .values = common::ByteView{&sentinel, 1U}});
  ASSERT_TRUE(physical.has_value());
  // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
  const auto invalid_policy = encode_cseg_v1_page(*physical, static_cast<PageCompression>(3U));
  ASSERT_FALSE(invalid_policy.has_value());
  EXPECT_EQ(invalid_policy.error().code(), common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::cseg
