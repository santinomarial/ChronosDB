#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/common/status.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/logical_type.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <span>
#include <type_traits>
#include <vector>

namespace chronos::cseg {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  for (std::size_t index = 0U; index < bytes.size(); ++index) {
    bytes[index] = std::byte{static_cast<std::uint8_t>(seed + index)};
  }
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

template <typename Integer> void append_le(std::vector<std::byte>& bytes, const Integer value) {
  using Unsigned = std::make_unsigned_t<Integer>;
  const Unsigned encoded = std::bit_cast<Unsigned>(value);
  for (std::size_t index = 0U; index < sizeof(Integer); ++index) {
    bytes.push_back(std::byte{static_cast<std::uint8_t>(encoded >> (index * 8U))});
  }
}

[[nodiscard]] EncodedCsegPage encode_fixed(const schema::LogicalType logical_type,
                                           const std::uint32_t rows, const common::ByteView values,
                                           const PageCompression policy) {
  const auto physical = columnar::PhysicalColumnView::create(
      {.type = logical_type, .nullable = false, .row_count = rows, .null_count = 0U},
      {.validity = {}, .offsets = {}, .values = values});
  return encode_cseg_v1_page(physical.value(), policy).value();
}

struct PartFixture {
  PartId part_id{id<PartId>(0x10U)};
  schema::TableId table_id{id<schema::TableId>(0x20U)};
  schema::TabletId tablet_id{id<schema::TabletId>(0x30U)};
  schema::SchemaId schema_id{id<schema::SchemaId>(0x40U)};
  std::vector<CsegColumnDescriptor> columns{
      CsegColumnDescriptor{.column_id = id<schema::ColumnId>(0x50U),
                           .storage_kind = StorageKind::kUser,
                           .logical_type = type(schema::LogicalTypeKind::kTimestampNs),
                           .nullable = false,
                           .event_time = true,
                           .schema_ordinal = 0U,
                           .ordering_ordinal = 0U},
      CsegColumnDescriptor{.column_id = std::nullopt,
                           .storage_kind = StorageKind::kWalId,
                           .logical_type = type(schema::LogicalTypeKind::kUuid),
                           .nullable = false,
                           .event_time = false,
                           .schema_ordinal = std::nullopt,
                           .ordering_ordinal = std::nullopt},
      CsegColumnDescriptor{.column_id = std::nullopt,
                           .storage_kind = StorageKind::kRecordSequence,
                           .logical_type = type(schema::LogicalTypeKind::kUInt64),
                           .nullable = false,
                           .event_time = false,
                           .schema_ordinal = std::nullopt,
                           .ordering_ordinal = std::nullopt},
      CsegColumnDescriptor{.column_id = std::nullopt,
                           .storage_kind = StorageKind::kRowOrdinal,
                           .logical_type = type(schema::LogicalTypeKind::kUInt32),
                           .nullable = false,
                           .event_time = false,
                           .schema_ordinal = std::nullopt,
                           .ordering_ordinal = std::nullopt},
      CsegColumnDescriptor{.column_id = std::nullopt,
                           .storage_kind = StorageKind::kOperation,
                           .logical_type = type(schema::LogicalTypeKind::kUInt8),
                           .nullable = false,
                           .event_time = false,
                           .schema_ordinal = std::nullopt,
                           .ordering_ordinal = std::nullopt},
  };
  std::vector<CsegGranuleDescriptor> granules;
  std::vector<EncodedCsegPage> pages;
  std::uint32_t rows;

  explicit PartFixture(const std::uint32_t row_count = 2U,
                       const PageCompression policy = PageCompression::kNone)
      : rows(row_count) {
    granules.push_back({.first_row = 0U,
                        .row_count = rows,
                        .first_page_index = 0U,
                        .minimum_event_time = -5,
                        .maximum_event_time = static_cast<std::int64_t>(rows) - 6});
    std::vector<std::byte> event_time;
    std::vector<std::byte> wal_id;
    std::vector<std::byte> sequence;
    std::vector<std::byte> row_ordinal;
    std::vector<std::byte> operation(rows, std::byte{format::kAppendRowsOperation});
    for (std::uint32_t row = 0U; row < rows; ++row) {
      append_le(event_time, static_cast<std::int64_t>(row) - 5);
      const common::Uuid::Bytes wal = id<schema::SchemaId>(0x70U).bytes();
      wal_id.insert(wal_id.end(), wal.begin(), wal.end());
      append_le(sequence, std::uint64_t{7U});
      append_le(row_ordinal, row);
    }
    pages.reserve(columns.size());
    pages.push_back(encode_fixed(columns[0].logical_type, rows, event_time, policy));
    pages.push_back(encode_fixed(columns[1].logical_type, rows, wal_id, policy));
    pages.push_back(encode_fixed(columns[2].logical_type, rows, sequence, policy));
    pages.push_back(encode_fixed(columns[3].logical_type, rows, row_ordinal, policy));
    pages.push_back(encode_fixed(columns[4].logical_type, rows, operation, policy));
  }

  [[nodiscard]] CsegPartEncodeInput input() const {
    return {.part_id = part_id,
            .table_id = table_id,
            .tablet_id = tablet_id,
            .schema_id = schema_id,
            .schema_version = schema::SchemaVersion::initial(),
            .row_count = rows,
            .event_time_column_ordinal = 0U,
            .ordering_column_count = 1U,
            .minimum_event_time = -5,
            .maximum_event_time = static_cast<std::int64_t>(rows) - 6,
            .columns = columns,
            .granules = granules,
            .pages = pages};
  }
};

[[nodiscard]] std::vector<std::byte> encoded_fixture() {
  PartFixture fixture;
  const auto encoded = encode_cseg_v1_part(fixture.input());
  return {encoded->bytes().begin(), encoded->bytes().end()};
}

void store_u16(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint16_t value) {
  bytes[offset] = std::byte{static_cast<std::uint8_t>(value)};
  bytes[offset + 1U] = std::byte{static_cast<std::uint8_t>(value >> 8U)};
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = std::byte{static_cast<std::uint8_t>(value >> (index * 8U))};
  }
}

void refresh_metadata_crc(std::vector<std::byte>& bytes) {
  constexpr std::size_t metadata_length = 1'208U;
  const std::size_t crc_offset = metadata_length - format::kMetadataCrc32cLength;
  store_u32(bytes, crc_offset, common::crc32c(common::ByteView{bytes}.first(crc_offset)));
}

[[nodiscard]] std::uint32_t independent_crc32c(const common::ByteView bytes) {
  std::uint32_t crc = 0xffffffffU;
  for (const std::byte byte : bytes) {
    crc ^= std::to_integer<std::uint8_t>(byte);
    for (std::size_t bit = 0U; bit < 8U; ++bit) {
      const std::uint32_t mask = 0U - (crc & 1U);
      crc = (crc >> 1U) ^ (0x82f63b78U & mask);
    }
  }
  return ~crc;
}

TEST(CsegPartCodecTest, RawGoldenComposesExactCanonicalFileAndBorrowedSlices) {
  PartFixture fixture;
  const auto first = encode_cseg_v1_part(fixture.input());
  const auto second = encode_cseg_v1_part(fixture.input());
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  ASSERT_EQ(first->size(), 1'288U);
  EXPECT_TRUE(std::ranges::equal(first->bytes(), second->bytes()));
  // Independently computed from literal v1 fields/pages with a tableless reflected Castagnoli
  // implementation. It fingerprints the complete metadata, page ordering, and zero padding.
  EXPECT_EQ(independent_crc32c(first->bytes()), 0xa77e7845U);
  EXPECT_EQ(common::crc32c(first->bytes()), 0xa77e7845U);

  const auto decoded = decode_cseg_v1_part_exact(first->bytes());
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->encoded_part().data(), first->bytes().data());
  EXPECT_EQ(decoded->metadata().encoded_metadata().data(), first->bytes().data());
  ASSERT_EQ(decoded->metadata().pages().size(), fixture.pages.size());
  for (std::size_t index = 0U; index < fixture.pages.size(); ++index) {
    EXPECT_EQ(decoded->stored_page(index).data(),
              first->bytes().data() + decoded->metadata().pages()[index].page_offset);
    EXPECT_TRUE(std::ranges::equal(decoded->stored_page(index), fixture.pages[index].bytes()));
    const auto page = decoded->decode_page(index);
    ASSERT_TRUE(page.has_value()) << index;
    EXPECT_FALSE(page->owns_uncompressed_bytes());
  }
  EXPECT_TRUE(decoded->stored_page(fixture.pages.size()).empty());
  const auto outside = decoded->decode_page(fixture.pages.size());
  ASSERT_FALSE(outside.has_value());
  EXPECT_EQ(outside.error().code(), common::StatusCode::kOutOfRange);
}

TEST(CsegPartCodecTest, PrefixClassifiesEveryTruncationAndExactRejectsSuffix) {
  const std::vector<std::byte> bytes = encoded_fixture();
  constexpr std::size_t metadata_length = 1'208U;
  for (std::size_t size = 0U; size < bytes.size(); ++size) {
    const auto decoded = decode_cseg_v1_part_prefix(common::ByteView{bytes}.first(size));
    ASSERT_FALSE(decoded.has_value()) << size;
    EXPECT_EQ(decoded.error().kind(), CsegPartDecodeErrorKind::kIncomplete) << size;
    const std::uint64_t expected =
        size < format::kMagic.size()
            ? format::kMagic.size()
            : (size < format::kFileHeaderLength
                   ? format::kFileHeaderLength
                   : (size < metadata_length ? metadata_length : bytes.size()));
    EXPECT_EQ(decoded.error().required_size(), expected) << size;
  }
  std::vector<std::byte> suffixed = bytes;
  suffixed.push_back(std::byte{0x7fU});
  const auto prefix = decode_cseg_v1_part_prefix(suffixed);
  ASSERT_TRUE(prefix.has_value());
  EXPECT_EQ(prefix->encoded_part().size(), bytes.size());
  const auto exact = decode_cseg_v1_part_exact(suffixed);
  ASSERT_FALSE(exact.has_value());
  EXPECT_EQ(exact.error().kind(), CsegPartDecodeErrorKind::kCorruption);
}

TEST(CsegPartCodecTest, EveryStoredByteAndPaddingMutationFailsClosed) {
  const std::vector<std::byte> canonical = encoded_fixture();
  const auto decoded = decode_cseg_v1_part_exact(canonical);
  ASSERT_TRUE(decoded.has_value());
  for (const CsegPageDescriptor& page : decoded->metadata().pages()) {
    for (std::uint64_t offset = page.page_offset; offset < page.page_offset + page.stored_length;
         ++offset) {
      std::vector<std::byte> bytes = canonical;
      bytes[static_cast<std::size_t>(offset)] ^= std::byte{1U};
      const auto corrupt = decode_cseg_v1_part_exact(bytes);
      ASSERT_FALSE(corrupt.has_value()) << offset;
      EXPECT_EQ(corrupt.error().kind(), CsegPartDecodeErrorKind::kCorruption) << offset;
    }
  }
  const auto pages = decoded->metadata().pages();
  for (std::size_t index = 0U; index < pages.size(); ++index) {
    const std::uint64_t page_end = pages[index].page_offset + pages[index].stored_length;
    const std::uint64_t next =
        index + 1U == pages.size() ? canonical.size() : pages[index + 1U].page_offset;
    for (std::uint64_t offset = page_end; offset < next; ++offset) {
      std::vector<std::byte> bytes = canonical;
      bytes[static_cast<std::size_t>(offset)] = std::byte{1U};
      const auto corrupt = decode_cseg_v1_part_exact(bytes);
      ASSERT_FALSE(corrupt.has_value()) << offset;
      EXPECT_EQ(corrupt.error().kind(), CsegPartDecodeErrorKind::kCorruption) << offset;
    }
  }
}

TEST(CsegPartCodecTest, PreservesUnsupportedClassificationFromAuthenticatedDescriptors) {
  std::vector<std::byte> bytes = encoded_fixture();
  constexpr std::size_t first_page_descriptor = format::kFileHeaderLength +
                                                5U * format::kColumnDescriptorLength +
                                                format::kGranuleDescriptorLength;
  store_u16(bytes, first_page_descriptor + format::kPageCompressionOffset, 3U);
  refresh_metadata_crc(bytes);
  const auto decoded = decode_cseg_v1_part_exact(bytes);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().kind(), CsegPartDecodeErrorKind::kUnsupported);
  EXPECT_EQ(decoded.error().status().code(), common::StatusCode::kNotSupported);
}

TEST(CsegPartCodecTest, ZstdPagesAreOwnedOnDemandAndResourceLimitsPrecedeBodies) {
  PartFixture fixture{1'024U, PageCompression::kZstd};
  const auto encoded = encode_cseg_v1_part(fixture.input());
  ASSERT_TRUE(encoded.has_value());
  const auto decoded = decode_cseg_v1_part_exact(encoded->bytes());
  ASSERT_TRUE(decoded.has_value());
  EXPECT_TRUE(std::ranges::any_of(decoded->metadata().pages(), [](const CsegPageDescriptor& page) {
    return page.compression == PageCompression::kZstd;
  }));
  for (std::size_t index = 0U; index < decoded->metadata().pages().size(); ++index) {
    auto page = decoded->decode_page(index);
    ASSERT_TRUE(page.has_value()) << index;
    EXPECT_EQ(page->owns_uncompressed_bytes(),
              decoded->metadata().pages()[index].compression == PageCompression::kZstd);
  }

  CsegMetadataDecodeLimits limits;
  limits.max_file_length = encoded->size() - 1U;
  const auto limited = decode_cseg_v1_part_prefix(encoded->bytes(), limits);
  ASSERT_FALSE(limited.has_value());
  EXPECT_EQ(limited.error().kind(), CsegPartDecodeErrorKind::kResourceLimit);
}

TEST(CsegPartCodecPropertyTest, GeneratedRawAndCompressedPartsRoundTripDeterministically) {
  for (std::uint32_t rows = 1U; rows <= 257U; rows += 16U) {
    for (const PageCompression policy : {PageCompression::kNone, PageCompression::kZstd}) {
      PartFixture fixture{rows, policy};
      const auto first = encode_cseg_v1_part(fixture.input());
      const auto second = encode_cseg_v1_part(fixture.input());
      ASSERT_TRUE(first.has_value()) << rows;
      ASSERT_TRUE(second.has_value()) << rows;
      EXPECT_TRUE(std::ranges::equal(first->bytes(), second->bytes())) << rows;
      const auto decoded = decode_cseg_v1_part_exact(first->bytes());
      ASSERT_TRUE(decoded.has_value()) << rows;
      EXPECT_EQ(decoded->metadata().row_count(), rows);
      for (std::size_t index = 0U; index < fixture.pages.size(); ++index) {
        const auto page = decoded->decode_page(index);
        ASSERT_TRUE(page.has_value()) << rows << ':' << index;
        EXPECT_EQ(page->physical().row_count(), rows);
      }
    }
  }
}

TEST(CsegPartCodecTest, EncoderRejectsPageCountAndRowShapeContradictions) {
  PartFixture fixture;
  const CsegPartEncodeInput input = fixture.input();
  const auto short_pages = encode_cseg_v1_part(CsegPartEncodeInput{
      .part_id = input.part_id,
      .table_id = input.table_id,
      .tablet_id = input.tablet_id,
      .schema_id = input.schema_id,
      .schema_version = input.schema_version,
      .row_count = input.row_count,
      .event_time_column_ordinal = input.event_time_column_ordinal,
      .ordering_column_count = input.ordering_column_count,
      .minimum_event_time = input.minimum_event_time,
      .maximum_event_time = input.maximum_event_time,
      .columns = input.columns,
      .granules = input.granules,
      .pages = input.pages.first(input.pages.size() - 1U),
  });
  ASSERT_FALSE(short_pages.has_value());
  EXPECT_EQ(short_pages.error().code(), common::StatusCode::kInvalidArgument);

  fixture.granules[0].row_count += 1U;
  const auto wrong_rows = encode_cseg_v1_part(fixture.input());
  ASSERT_FALSE(wrong_rows.has_value());
  EXPECT_EQ(wrong_rows.error().code(), common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::cseg
