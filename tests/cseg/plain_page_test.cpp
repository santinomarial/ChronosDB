#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/status.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/cseg/plain_page.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/logical_type.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <vector>

namespace chronos::cseg {
namespace {

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return kind == schema::LogicalTypeKind::kDecimal ? schema::LogicalType::decimal(18U, 4U).value()
                                                   : schema::LogicalType::create(kind).value();
}

void append_u32(std::vector<std::byte>& bytes, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes.push_back(std::byte{static_cast<std::uint8_t>(value >> (index * 8U))});
  }
}

[[nodiscard]] std::size_t width(const schema::LogicalTypeKind kind) {
  using schema::LogicalTypeKind;
  switch (kind) {
  case LogicalTypeKind::kInt8:
  case LogicalTypeKind::kUInt8:
    return 1U;
  case LogicalTypeKind::kInt16:
  case LogicalTypeKind::kUInt16:
    return 2U;
  case LogicalTypeKind::kInt32:
  case LogicalTypeKind::kUInt32:
  case LogicalTypeKind::kFloat32:
  case LogicalTypeKind::kDate:
    return 4U;
  case LogicalTypeKind::kInt64:
  case LogicalTypeKind::kUInt64:
  case LogicalTypeKind::kFloat64:
  case LogicalTypeKind::kTimestampNs:
    return 8U;
  case LogicalTypeKind::kDecimal:
  case LogicalTypeKind::kUuid:
    return 16U;
  case LogicalTypeKind::kBool:
  case LogicalTypeKind::kSymbol:
  case LogicalTypeKind::kString:
  case LogicalTypeKind::kBinary:
    return 0U;
  }
  return 0U;
}

[[nodiscard]] CsegColumnDescriptor column_descriptor(const schema::LogicalType logical_type,
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

[[nodiscard]] CsegPageDescriptor page_descriptor(const columnar::PhysicalColumnView& physical,
                                                 const EncodedCsegPlainPage& encoded) {
  return {.granule_ordinal = 0U,
          .stored_column_ordinal = 0U,
          .compression = PageCompression::kNone,
          .row_count = physical.row_count(),
          .null_count = physical.null_count(),
          .page_offset = 0U,
          .stored_length = encoded.size(),
          .uncompressed_length = encoded.size(),
          .validity_length = encoded.validity_length(),
          .offsets_length = encoded.offsets_length(),
          .values_length = encoded.values_length(),
          .page_crc32c = 0U};
}

TEST(CsegPlainPageTest, EncodesExactBufferOrderAndDecodesBorrowedCells) {
  const std::array<std::byte, 1U> validity{std::byte{0x05U}};
  std::vector<std::byte> offsets;
  append_u32(offsets, 0U);
  append_u32(offsets, 1U);
  append_u32(offsets, 1U);
  append_u32(offsets, 3U);
  const std::array<std::byte, 3U> values{std::byte{'a'}, std::byte{'b'}, std::byte{'c'}};
  const schema::LogicalType string_type = type(schema::LogicalTypeKind::kString);
  const auto physical = columnar::PhysicalColumnView::create(
      {.type = string_type, .nullable = true, .row_count = 3U, .null_count = 1U},
      {.validity = validity, .offsets = offsets, .values = values});
  ASSERT_TRUE(physical.has_value());
  const common::Result<EncodedCsegPlainPage> encoded = encode_cseg_v1_plain_page(*physical);
  ASSERT_TRUE(encoded.has_value());
  EXPECT_EQ(encoded->validity_length(), 1U);
  EXPECT_EQ(encoded->offsets_length(), 16U);
  EXPECT_EQ(encoded->values_length(), 3U);
  ASSERT_EQ(encoded->size(), 20U);
  EXPECT_EQ(encoded->bytes().front(), validity.front());
  EXPECT_TRUE(std::ranges::equal(encoded->bytes().subspan(1U, 16U), offsets));
  EXPECT_TRUE(std::ranges::equal(encoded->bytes().last(3U), values));

  const CsegPageDescriptor page = page_descriptor(*physical, *encoded);
  const auto decoded =
      decode_cseg_v1_plain_page(encoded->bytes(), column_descriptor(string_type, true), page);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->encoded_bytes().data(), encoded->bytes().data());
  EXPECT_EQ(decoded->physical().validity().data(), encoded->bytes().data());
  EXPECT_EQ(decoded->physical().offsets().data(), encoded->bytes().data() + 1U);
  EXPECT_EQ(decoded->physical().values().data(), encoded->bytes().data() + 17U);
  EXPECT_EQ(decoded->physical().cell(0U)->bytes()->size(), 1U);
  EXPECT_TRUE(decoded->physical().cell(1U)->is_null());
  EXPECT_EQ(decoded->physical().cell(2U)->bytes()->size(), 2U);

  for (std::size_t prefix_size = 0U; prefix_size < encoded->size(); ++prefix_size) {
    const auto truncated = decode_cseg_v1_plain_page(encoded->bytes().first(prefix_size),
                                                     column_descriptor(string_type, true), page);
    ASSERT_FALSE(truncated.has_value()) << prefix_size;
    EXPECT_EQ(truncated.error().code(), common::StatusCode::kCorruption) << prefix_size;
  }
  std::vector<std::byte> with_suffix(encoded->bytes().begin(), encoded->bytes().end());
  with_suffix.push_back(std::byte{0U});
  const auto suffixed =
      decode_cseg_v1_plain_page(with_suffix, column_descriptor(string_type, true), page);
  ASSERT_FALSE(suffixed.has_value());
  EXPECT_EQ(suffixed.error().code(), common::StatusCode::kCorruption);
}

TEST(CsegPlainPagePropertyTest, EveryFrozenLogicalTypeRoundTripsCanonicalPhysicalBytes) {
  for (std::uint16_t code = 1U; code <= 18U; ++code) {
    const auto kind = static_cast<schema::LogicalTypeKind>(code);
    const schema::LogicalType logical_type = type(kind);
    std::vector<std::byte> offsets;
    std::vector<std::byte> values;
    if (logical_type.is_variable_width()) {
      append_u32(offsets, 0U);
      append_u32(offsets, 0U);
    } else if (kind == schema::LogicalTypeKind::kBool) {
      values.push_back(std::byte{1U});
    } else {
      values.resize(width(kind), std::byte{0U});
    }
    const auto physical = columnar::PhysicalColumnView::create(
        {.type = logical_type, .nullable = false, .row_count = 1U, .null_count = 0U},
        {.validity = {}, .offsets = offsets, .values = values});
    ASSERT_TRUE(physical.has_value()) << code;
    const auto encoded = encode_cseg_v1_plain_page(*physical);
    ASSERT_TRUE(encoded.has_value()) << code;
    const auto decoded =
        decode_cseg_v1_plain_page(encoded->bytes(), column_descriptor(logical_type, false),
                                  page_descriptor(*physical, *encoded));
    ASSERT_TRUE(decoded.has_value()) << code;
    EXPECT_TRUE(std::ranges::equal(decoded->physical().offsets(), offsets)) << code;
    EXPECT_TRUE(std::ranges::equal(decoded->physical().values(), values)) << code;
  }
}

TEST(CsegPlainPageTest, RejectsInvalidUtf8OffsetsNullSlotsAndUnusedBitsAsCorruption) {
  const schema::LogicalType string_type = type(schema::LogicalTypeKind::kString);
  std::vector<std::byte> offsets;
  append_u32(offsets, 0U);
  append_u32(offsets, 1U);
  const std::array<std::byte, 1U> invalid_utf8{std::byte{0xffU}};
  CsegPageDescriptor page{.granule_ordinal = 0U,
                          .stored_column_ordinal = 0U,
                          .compression = PageCompression::kNone,
                          .row_count = 1U,
                          .null_count = 0U,
                          .page_offset = 0U,
                          .stored_length = 9U,
                          .uncompressed_length = 9U,
                          .validity_length = 0U,
                          .offsets_length = 8U,
                          .values_length = 1U,
                          .page_crc32c = 0U};
  std::vector<std::byte> payload = offsets;
  payload.insert(payload.end(), invalid_utf8.begin(), invalid_utf8.end());
  auto decoded = decode_cseg_v1_plain_page(payload, column_descriptor(string_type, false), page);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().code(), common::StatusCode::kCorruption);

  payload[4U] = std::byte{2U};
  decoded = decode_cseg_v1_plain_page(payload, column_descriptor(string_type, false), page);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().code(), common::StatusCode::kCorruption);

  const schema::LogicalType bool_type = type(schema::LogicalTypeKind::kBool);
  page = {.granule_ordinal = 0U,
          .stored_column_ordinal = 0U,
          .compression = PageCompression::kNone,
          .row_count = 1U,
          .null_count = 1U,
          .page_offset = 0U,
          .stored_length = 2U,
          .uncompressed_length = 2U,
          .validity_length = 1U,
          .offsets_length = 0U,
          .values_length = 1U,
          .page_crc32c = 0U};
  const std::array<std::byte, 2U> dirty_null{std::byte{0U}, std::byte{1U}};
  decoded = decode_cseg_v1_plain_page(dirty_null, column_descriptor(bool_type, true), page);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().code(), common::StatusCode::kCorruption);

  page.null_count = 0U;
  const std::array<std::byte, 2U> dirty_high_bits{std::byte{1U}, std::byte{0x81U}};
  decoded = decode_cseg_v1_plain_page(dirty_high_bits, column_descriptor(bool_type, true), page);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().code(), common::StatusCode::kCorruption);
}

TEST(CsegPlainPageTest, RejectsDescriptorLengthContradictionsAndLimitsBeforeAccess) {
  const schema::LogicalType timestamp = type(schema::LogicalTypeKind::kTimestampNs);
  const std::array<std::byte, 8U> value{};
  CsegPageDescriptor page{.granule_ordinal = 0U,
                          .stored_column_ordinal = 0U,
                          .compression = PageCompression::kNone,
                          .row_count = 1U,
                          .null_count = 0U,
                          .page_offset = 0U,
                          .stored_length = 8U,
                          .uncompressed_length = 8U,
                          .validity_length = 0U,
                          .offsets_length = 0U,
                          .values_length = 7U,
                          .page_crc32c = 0U};
  auto decoded = decode_cseg_v1_plain_page(value, column_descriptor(timestamp, false), page);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().code(), common::StatusCode::kCorruption);

  std::byte sentinel{0U};
  page.uncompressed_length = format::kMaximumUncompressedPageLength + 1U;
  page.values_length = page.uncompressed_length;
  const common::ByteView oversized{&sentinel, format::kMaximumUncompressedPageLength + 1U};
  decoded = decode_cseg_v1_plain_page(oversized, column_descriptor(timestamp, false), page);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().code(), common::StatusCode::kCorruption);

  std::vector<std::byte> offsets;
  append_u32(offsets, 0U);
  append_u32(offsets, static_cast<std::uint32_t>(format::kMaximumUncompressedPageLength + 1U));
  const auto oversized_physical = columnar::PhysicalColumnView::create(
      {.type = type(schema::LogicalTypeKind::kBinary),
       .nullable = false,
       .row_count = 1U,
       .null_count = 0U},
      {.validity = {}, .offsets = offsets, .values = oversized});
  ASSERT_TRUE(oversized_physical.has_value());
  const auto encoded = encode_cseg_v1_plain_page(*oversized_physical);
  ASSERT_FALSE(encoded.has_value());
  EXPECT_EQ(encoded.error().code(), common::StatusCode::kResourceExhausted);
}

} // namespace
} // namespace chronos::cseg
