#include "chronos/cseg/metadata_codec.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/cseg/layout.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

namespace chronos::cseg {
namespace {

[[nodiscard]] common::Status status(const common::StatusCode code, const std::string_view message) {
  return common::Status{code, std::string{message}};
}

[[nodiscard]] CsegMetadataDecodeError incomplete(const std::string_view message,
                                                 const std::uint64_t required) {
  return {CsegMetadataDecodeErrorKind::kIncomplete,
          status(common::StatusCode::kOutOfRange, message), required};
}

[[nodiscard]] CsegMetadataDecodeError corruption(const std::string_view message) {
  return {CsegMetadataDecodeErrorKind::kCorruption,
          status(common::StatusCode::kCorruption, message)};
}

[[nodiscard]] CsegMetadataDecodeError unsupported(const std::string_view message) {
  return {CsegMetadataDecodeErrorKind::kUnsupported,
          status(common::StatusCode::kNotSupported, message)};
}

[[nodiscard]] CsegMetadataDecodeError resource_limit(const std::string_view message) {
  return {CsegMetadataDecodeErrorKind::kResourceLimit,
          status(common::StatusCode::kResourceExhausted, message)};
}

[[nodiscard]] std::uint16_t load_u16_le(const common::ByteView bytes,
                                        const std::size_t offset) noexcept {
  return static_cast<std::uint16_t>(std::to_integer<std::uint16_t>(bytes[offset]) |
                                    (std::to_integer<std::uint16_t>(bytes[offset + 1U]) << 8U));
}

[[nodiscard]] std::uint32_t load_u32_le(const common::ByteView bytes,
                                        const std::size_t offset) noexcept {
  std::uint32_t value = 0U;
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    value |= std::to_integer<std::uint32_t>(bytes[offset + index]) << (index * 8U);
  }
  return value;
}

[[nodiscard]] std::uint64_t load_u64_le(const common::ByteView bytes,
                                        const std::size_t offset) noexcept {
  std::uint64_t value = 0U;
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    value |= std::to_integer<std::uint64_t>(bytes[offset + index]) << (index * 8U);
  }
  return value;
}

[[nodiscard]] std::int64_t load_i64_le(const common::ByteView bytes,
                                       const std::size_t offset) noexcept {
  return std::bit_cast<std::int64_t>(load_u64_le(bytes, offset));
}

void store_u16_le(const common::MutableByteView bytes, const std::size_t offset,
                  const std::uint16_t value) noexcept {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = std::byte{static_cast<std::uint8_t>(value >> (index * 8U))};
  }
}

void store_u32_le(const common::MutableByteView bytes, const std::size_t offset,
                  const std::uint32_t value) noexcept {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = std::byte{static_cast<std::uint8_t>(value >> (index * 8U))};
  }
}

void store_u64_le(const common::MutableByteView bytes, const std::size_t offset,
                  const std::uint64_t value) noexcept {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = std::byte{static_cast<std::uint8_t>(value >> (index * 8U))};
  }
}

void store_i64_le(const common::MutableByteView bytes, const std::size_t offset,
                  const std::int64_t value) noexcept {
  store_u64_le(bytes, offset, std::bit_cast<std::uint64_t>(value));
}

[[nodiscard]] bool is_zero(const common::ByteView bytes) noexcept {
  return std::ranges::all_of(bytes, [](const std::byte value) { return value == std::byte{0}; });
}

template <typename Identifier>
[[nodiscard]] common::Result<Identifier> parse_identifier(const common::ByteView bytes,
                                                          const std::size_t offset) {
  common::Uuid::Bytes encoded{};
  std::copy_n(bytes.begin() + static_cast<std::ptrdiff_t>(offset), encoded.size(), encoded.begin());
  return Identifier::from_bytes(encoded);
}

[[nodiscard]] std::size_t fixed_width(const schema::LogicalTypeKind kind) noexcept {
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

[[nodiscard]] std::optional<CsegMetadataDecodeError>
validate_columns(const std::span<const CsegColumnDescriptor> columns,
                 const std::uint32_t event_time_ordinal,
                 const std::uint32_t ordering_column_count) {
  if (columns.size() <= format::kSystemColumnCount ||
      columns.size() > format::kMaximumStoredColumnCount) {
    return corruption("CSEG stored column count is outside v1 bounds");
  }
  const std::size_t user_count = columns.size() - format::kSystemColumnCount;
  if (event_time_ordinal >= user_count || ordering_column_count == 0U ||
      ordering_column_count > user_count) {
    return corruption("CSEG event-time or ordering column count is outside the user schema");
  }

  std::vector<bool> ordering_seen(ordering_column_count, false);
  std::vector<schema::ColumnId> user_ids;
  user_ids.reserve(user_count);
  std::uint32_t event_count = 0U;
  for (std::size_t ordinal = 0U; ordinal < columns.size(); ++ordinal) {
    const CsegColumnDescriptor& column = columns[ordinal];
    if (ordinal < user_count) {
      if (column.storage_kind != StorageKind::kUser || !column.column_id.has_value() ||
          !column.schema_ordinal.has_value() || *column.schema_ordinal != ordinal) {
        return corruption("CSEG user column identity, kind, or schema ordinal is invalid");
      }
      if (std::ranges::find(user_ids, *column.column_id) != user_ids.end()) {
        return corruption("CSEG user column identities are not unique");
      }
      user_ids.push_back(*column.column_id);
      if (column.event_time) {
        ++event_count;
        if (ordinal != event_time_ordinal || column.nullable ||
            column.logical_type.kind() != schema::LogicalTypeKind::kTimestampNs) {
          return corruption("CSEG event-time column metadata is invalid");
        }
      } else if (ordinal == event_time_ordinal) {
        return corruption("CSEG header event-time ordinal lacks its descriptor flag");
      }
      if (column.ordering_ordinal.has_value()) {
        const std::uint32_t ordering = *column.ordering_ordinal;
        if (ordering >= ordering_column_count || ordering_seen[ordering]) {
          return corruption("CSEG physical ordering ordinal is outside a unique dense sequence");
        }
        ordering_seen[ordering] = true;
      }
      continue;
    }

    const std::size_t system_ordinal = ordinal - user_count;
    constexpr std::array<StorageKind, format::kSystemColumnCount> kinds{
        StorageKind::kWalId, StorageKind::kRecordSequence, StorageKind::kRowOrdinal,
        StorageKind::kOperation};
    constexpr std::array<schema::LogicalTypeKind, format::kSystemColumnCount> types{
        schema::LogicalTypeKind::kUuid, schema::LogicalTypeKind::kUInt64,
        schema::LogicalTypeKind::kUInt32, schema::LogicalTypeKind::kUInt8};
    if (column.column_id.has_value() || column.storage_kind != kinds[system_ordinal] ||
        column.logical_type.kind() != types[system_ordinal] || column.nullable ||
        column.event_time || column.schema_ordinal.has_value() ||
        column.ordering_ordinal.has_value()) {
      return corruption("CSEG system column registry entry is invalid");
    }
  }
  if (event_count != 1U || !columns[event_time_ordinal].ordering_ordinal.has_value() ||
      !std::ranges::all_of(ordering_seen, [](const bool seen) { return seen; })) {
    return corruption("CSEG event-time and physical ordering descriptors are incomplete");
  }
  return std::nullopt;
}

[[nodiscard]] std::optional<CsegMetadataDecodeError>
// These adjacent counts and extrema mirror one granule-directory/header comparison.
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
validate_granules(
    const std::span<const CsegGranuleDescriptor> granules,
    const std::uint32_t stored_column_count, // NOLINT(bugprone-easily-swappable-parameters)
    const std::uint64_t row_count, const std::int64_t minimum_event_time,
    const std::int64_t maximum_event_time) {
  if (granules.empty() || granules.size() > format::kMaximumGranuleCount || row_count == 0U ||
      row_count > format::kMaximumRowCount || minimum_event_time > maximum_event_time) {
    return corruption("CSEG row, granule, or event-time bounds are invalid");
  }
  std::uint64_t next_row = 0U;
  std::int64_t observed_minimum = std::numeric_limits<std::int64_t>::max();
  std::int64_t observed_maximum = std::numeric_limits<std::int64_t>::min();
  for (std::size_t ordinal = 0U; ordinal < granules.size(); ++ordinal) {
    const CsegGranuleDescriptor& granule = granules[ordinal];
    const std::uint64_t expected_page = static_cast<std::uint64_t>(ordinal) * stored_column_count;
    if (granule.first_row != next_row || granule.row_count == 0U ||
        granule.row_count > format::kMaximumGranuleRowCount ||
        granule.first_page_index != expected_page ||
        granule.minimum_event_time > granule.maximum_event_time) {
      return corruption("CSEG granules are not a canonical contiguous directory");
    }
    const std::optional<std::uint64_t> end =
        common::checked_add(next_row, static_cast<std::uint64_t>(granule.row_count));
    if (!end.has_value() || *end > row_count) {
      return corruption("CSEG granule row coverage exceeds the header row count");
    }
    next_row = *end;
    observed_minimum = std::min(observed_minimum, granule.minimum_event_time);
    observed_maximum = std::max(observed_maximum, granule.maximum_event_time);
  }
  if (next_row != row_count || observed_minimum != minimum_event_time ||
      observed_maximum != maximum_event_time) {
    return corruption("CSEG granules do not exactly cover header rows and event-time extrema");
  }
  return std::nullopt;
}

[[nodiscard]] std::optional<CsegMetadataDecodeError>
// Metadata and file lengths are the conventional inner/outer bound pair.
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
validate_pages(const std::span<const CsegPageDescriptor> pages,
               const std::span<const CsegColumnDescriptor> columns,
               const std::span<const CsegGranuleDescriptor> granules,
               const std::uint64_t metadata_length, // NOLINT(bugprone-easily-swappable-parameters)
               const std::uint64_t total_length) {
  const std::optional<std::size_t> expected_page_count =
      common::checked_multiply(columns.size(), granules.size());
  if (!expected_page_count.has_value() || pages.size() != *expected_page_count) {
    return corruption("CSEG page count does not match granules times stored columns");
  }
  std::uint64_t cursor = metadata_length;
  for (std::size_t index = 0U; index < pages.size(); ++index) {
    const CsegPageDescriptor& page = pages[index];
    const auto granule_ordinal = static_cast<std::uint32_t>(index / columns.size());
    const auto column_ordinal = static_cast<std::uint32_t>(index % columns.size());
    const CsegColumnDescriptor& column = columns[column_ordinal];
    if (page.granule_ordinal != granule_ordinal || page.stored_column_ordinal != column_ordinal ||
        page.row_count != granules[granule_ordinal].row_count || page.null_count > page.row_count ||
        (!column.nullable && page.null_count != 0U)) {
      return corruption("CSEG page ownership, row count, or null count is invalid");
    }
    if (page.stored_length == 0U || page.stored_length > format::kMaximumStoredPageLength ||
        page.uncompressed_length == 0U ||
        page.uncompressed_length > format::kMaximumUncompressedPageLength) {
      return corruption("CSEG page length is outside v1 bounds");
    }
    const std::optional<std::uint64_t> first_sum =
        common::checked_add(page.validity_length, page.offsets_length);
    const std::optional<std::uint64_t> all_sum =
        first_sum.has_value() ? common::checked_add(*first_sum, page.values_length) : std::nullopt;
    if (!all_sum.has_value() || *all_sum != page.uncompressed_length) {
      return corruption("CSEG PLAIN buffer lengths do not sum to the uncompressed page length");
    }
    const std::uint64_t bitmap_length =
        static_cast<std::uint64_t>(page.row_count) / 8U + (page.row_count % 8U == 0U ? 0U : 1U);
    if (page.validity_length != (column.nullable ? bitmap_length : 0U)) {
      return corruption("CSEG page validity length is not canonical");
    }
    if (column.logical_type.is_variable_width()) {
      const std::uint64_t expected_offsets =
          (static_cast<std::uint64_t>(page.row_count) + 1U) * sizeof(std::uint32_t);
      if (page.offsets_length != expected_offsets) {
        return corruption("CSEG variable page offsets length is not canonical");
      }
    } else {
      if (page.offsets_length != 0U) {
        return corruption("CSEG fixed page unexpectedly has offsets");
      }
      const std::uint64_t expected_values =
          column.logical_type.kind() == schema::LogicalTypeKind::kBool
              ? bitmap_length
              : static_cast<std::uint64_t>(page.row_count) *
                    fixed_width(column.logical_type.kind());
      if (page.values_length != expected_values) {
        return corruption("CSEG fixed page values length is not canonical");
      }
    }
    switch (page.compression) {
    case PageCompression::kNone:
      if (page.stored_length != page.uncompressed_length) {
        return corruption("raw CSEG page stored and uncompressed lengths differ");
      }
      break;
    case PageCompression::kZstd:
      if (page.stored_length >= page.uncompressed_length) {
        return corruption("Zstandard CSEG page is not canonically smaller");
      }
      break;
    default:
      return corruption("CSEG page compression value is invalid");
    }
    const common::Result<CsegPageLayout> layout =
        plan_cseg_v1_page_layout(cursor, page.stored_length);
    if (!layout.has_value() || page.page_offset != cursor) {
      return corruption("CSEG page offsets are not canonical");
    }
    cursor = layout->next_offset;
  }
  if (cursor != total_length) {
    return corruption("CSEG total length does not equal the canonical aligned page end");
  }
  return std::nullopt;
}

[[nodiscard]] std::optional<CsegMetadataDecodeError>
// This helper validates the named durable header fields as one frozen-format tuple.
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
validate_model(
    const std::uint64_t total_length,
    const std::uint64_t metadata_length, // NOLINT(bugprone-easily-swappable-parameters)
    const std::uint64_t row_count, const std::uint32_t event_time_ordinal,
    const std::uint32_t ordering_column_count, // NOLINT(bugprone-easily-swappable-parameters)
    const std::int64_t minimum_event_time, const std::int64_t maximum_event_time,
    const std::span<const CsegColumnDescriptor> columns,
    const std::span<const CsegGranuleDescriptor> granules,
    const std::span<const CsegPageDescriptor> pages) {
  if (const auto error = validate_columns(columns, event_time_ordinal, ordering_column_count);
      error.has_value()) {
    return error;
  }
  if (const auto error = validate_granules(granules, static_cast<std::uint32_t>(columns.size()),
                                           row_count, minimum_event_time, maximum_event_time);
      error.has_value()) {
    return error;
  }
  return validate_pages(pages, columns, granules, metadata_length, total_length);
}

[[nodiscard]] common::Result<StorageKind> storage_kind_from_code(const std::uint16_t code) {
  if (code == 0U) {
    return common::make_unexpected(
        status(common::StatusCode::kCorruption, "CSEG storage-kind code zero is invalid"));
  }
  if (code > format::kOperationStorageKind) {
    return common::make_unexpected(
        status(common::StatusCode::kNotSupported, "CSEG storage-kind code is unsupported"));
  }
  return static_cast<StorageKind>(code);
}

[[nodiscard]] common::Result<PageCompression> compression_from_code(const std::uint16_t code) {
  if (code == 0U) {
    return common::make_unexpected(
        status(common::StatusCode::kCorruption, "CSEG compression code zero is invalid"));
  }
  if (code > format::kZstdCompression) {
    return common::make_unexpected(
        status(common::StatusCode::kNotSupported, "CSEG compression code is unsupported"));
  }
  return static_cast<PageCompression>(code);
}

[[nodiscard]] bool valid_limits(const CsegMetadataDecodeLimits limits) noexcept {
  return limits.max_file_length > 0U && limits.max_file_length <= format::kMaximumFileLength &&
         limits.max_metadata_length > 0U &&
         limits.max_metadata_length <= format::kMaximumFileLength && limits.max_user_columns > 0U &&
         limits.max_user_columns <= format::kMaximumUserColumnCount && limits.max_granules > 0U &&
         limits.max_granules <= format::kMaximumGranuleCount && limits.max_pages > 0U;
}

} // namespace

EncodedCsegMetadata::EncodedCsegMetadata(std::vector<std::byte> bytes,
                                         const std::uint64_t total_length) noexcept
    : bytes_(std::move(bytes)), total_length_(total_length) {}

common::ByteView EncodedCsegMetadata::bytes() const noexcept {
  return bytes_;
}

std::size_t EncodedCsegMetadata::size() const noexcept {
  return bytes_.size();
}

CsegMetadataDecodeError::CsegMetadataDecodeError(const CsegMetadataDecodeErrorKind kind,
                                                 common::Status status_value,
                                                 const std::uint64_t required_size) noexcept
    : kind_(kind), status_(std::move(status_value)), required_size_(required_size) {}

// The private constructor preserves the frozen header field order; callers use named accessors.
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
DecodedCsegMetadataView::DecodedCsegMetadataView(
    PartId part_id, schema::TableId table_id, schema::TabletId tablet_id,
    schema::SchemaId schema_id, const schema::SchemaVersion schema_version,
    const std::uint64_t total_length, // NOLINT(bugprone-easily-swappable-parameters)
    const std::uint64_t row_count, const std::uint32_t event_time_column_ordinal,
    const std::uint32_t ordering_column_count, const std::int64_t minimum_event_time,
    const std::int64_t maximum_event_time, std::vector<CsegColumnDescriptor> columns,
    std::vector<CsegGranuleDescriptor> granules, std::vector<CsegPageDescriptor> pages,
    const common::ByteView encoded_metadata) noexcept
    : part_id_(part_id), table_id_(table_id), tablet_id_(tablet_id), schema_id_(schema_id),
      schema_version_(schema_version), total_length_(total_length), row_count_(row_count),
      event_time_column_ordinal_(event_time_column_ordinal),
      ordering_column_count_(ordering_column_count), minimum_event_time_(minimum_event_time),
      maximum_event_time_(maximum_event_time), columns_(std::move(columns)),
      granules_(std::move(granules)), pages_(std::move(pages)),
      encoded_metadata_(encoded_metadata) {}

std::span<const CsegColumnDescriptor> DecodedCsegMetadataView::columns() const noexcept {
  return columns_;
}

std::span<const CsegGranuleDescriptor> DecodedCsegMetadataView::granules() const noexcept {
  return granules_;
}

std::span<const CsegPageDescriptor> DecodedCsegMetadataView::pages() const noexcept {
  return pages_;
}

common::ByteView DecodedCsegMetadataView::encoded_metadata() const noexcept {
  return encoded_metadata_;
}

common::Result<EncodedCsegMetadata> encode_cseg_v1_metadata(const CsegMetadataEncodeInput& input) {
  if (input.columns.size() <= format::kSystemColumnCount ||
      input.columns.size() > format::kMaximumStoredColumnCount || input.granules.empty() ||
      input.granules.size() > format::kMaximumGranuleCount) {
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument, "CSEG encoder descriptor counts are invalid"));
  }
  const auto user_count =
      static_cast<std::uint32_t>(input.columns.size() - format::kSystemColumnCount);
  const common::Result<CsegMetadataLayout> metadata = plan_cseg_v1_metadata_layout(
      {.user_column_count = user_count,
       .granule_count = static_cast<std::uint32_t>(input.granules.size())});
  if (!metadata.has_value()) {
    return common::make_unexpected(metadata.error());
  }
  if (input.pages.size() != metadata->page_count) {
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument, "CSEG encoder page count is not canonical"));
  }

  std::vector<CsegPageDescriptor> pages;
  pages.reserve(input.pages.size());
  std::uint64_t cursor = metadata->metadata_length;
  for (std::size_t index = 0U; index < input.pages.size(); ++index) {
    const CsegPageMetadataInput& page = input.pages[index];
    if (page.uncompressed_length == 0U) {
      return common::make_unexpected(status(common::StatusCode::kInvalidArgument,
                                            "CSEG uncompressed page length must be nonzero"));
    }
    if (page.uncompressed_length > format::kMaximumUncompressedPageLength) {
      return common::make_unexpected(status(common::StatusCode::kResourceExhausted,
                                            "CSEG uncompressed page length exceeds the v1 limit"));
    }
    const common::Result<CsegPageLayout> placement =
        plan_cseg_v1_page_layout(cursor, page.stored_length);
    if (!placement.has_value()) {
      return common::make_unexpected(placement.error());
    }
    pages.push_back(CsegPageDescriptor{
        .granule_ordinal = static_cast<std::uint32_t>(index / input.columns.size()),
        .stored_column_ordinal = static_cast<std::uint32_t>(index % input.columns.size()),
        .compression = page.compression,
        .row_count = page.row_count,
        .null_count = page.null_count,
        .page_offset = cursor,
        .stored_length = page.stored_length,
        .uncompressed_length = page.uncompressed_length,
        .validity_length = page.validity_length,
        .offsets_length = page.offsets_length,
        .values_length = page.values_length,
        .page_crc32c = page.page_crc32c,
    });
    cursor = placement->next_offset;
  }
  if (const auto error = validate_model(
          cursor, metadata->metadata_length, input.row_count, input.event_time_column_ordinal,
          input.ordering_column_count, input.minimum_event_time, input.maximum_event_time,
          input.columns, input.granules, pages);
      error.has_value()) {
    return common::make_unexpected(
        status(common::StatusCode::kInvalidArgument, error->status().message()));
  }
  if (metadata->metadata_length > std::numeric_limits<std::size_t>::max()) {
    return common::make_unexpected(
        status(common::StatusCode::kResourceExhausted, "CSEG metadata does not fit this platform"));
  }

  std::vector<std::byte> storage(static_cast<std::size_t>(metadata->metadata_length), std::byte{0});
  const common::MutableByteView bytes{storage};
  std::copy(format::kMagic.begin(), format::kMagic.end(), storage.begin());
  store_u16_le(bytes, format::kFormatMajorOffset, format::kFormatMajor);
  store_u16_le(bytes, format::kFormatMinorOffset, format::kFormatMinor);
  store_u32_le(bytes, format::kHeaderLengthOffset, format::kFileHeaderLength);
  store_u64_le(bytes, format::kTotalLengthOffset, cursor);
  store_u64_le(bytes, format::kMetadataLengthOffset, metadata->metadata_length);
  store_u64_le(bytes, format::kRowCountOffset, input.row_count);
  store_u32_le(bytes, format::kUserColumnCountOffset, user_count);
  store_u32_le(bytes, format::kStoredColumnCountOffset, metadata->stored_column_count);
  store_u32_le(bytes, format::kGranuleCountOffset,
               static_cast<std::uint32_t>(input.granules.size()));
  store_u32_le(bytes, format::kPageCountOffset, metadata->page_count);
  const auto copy_id = [&storage](const auto& id, const std::size_t offset) {
    std::copy(id.bytes().begin(), id.bytes().end(),
              storage.begin() + static_cast<std::ptrdiff_t>(offset));
  };
  copy_id(input.part_id, format::kPartIdOffset);
  copy_id(input.table_id, format::kTableIdOffset);
  copy_id(input.tablet_id, format::kTabletIdOffset);
  copy_id(input.schema_id, format::kSchemaIdOffset);
  store_u64_le(bytes, format::kSchemaVersionOffset, input.schema_version.value());
  store_u64_le(bytes, format::kColumnsOffsetFieldOffset, metadata->columns_offset);
  store_u64_le(bytes, format::kGranulesOffsetFieldOffset, metadata->granules_offset);
  store_u64_le(bytes, format::kPagesOffsetFieldOffset, metadata->pages_offset);
  store_u64_le(bytes, format::kPageDataOffsetFieldOffset, metadata->metadata_length);
  store_u32_le(bytes, format::kEventTimeColumnOrdinalOffset, input.event_time_column_ordinal);
  store_u32_le(bytes, format::kOrderingColumnCountOffset, input.ordering_column_count);
  store_i64_le(bytes, format::kMinimumEventTimeOffset, input.minimum_event_time);
  store_i64_le(bytes, format::kMaximumEventTimeOffset, input.maximum_event_time);

  for (std::size_t ordinal = 0U; ordinal < input.columns.size(); ++ordinal) {
    const CsegColumnDescriptor& column = input.columns[ordinal];
    const std::size_t offset = static_cast<std::size_t>(metadata->columns_offset) +
                               ordinal * format::kColumnDescriptorLength;
    if (column.column_id.has_value()) {
      copy_id(*column.column_id, offset + format::kColumnIdOffset);
    }
    store_u16_le(bytes, offset + format::kStorageKindOffset,
                 static_cast<std::uint16_t>(column.storage_kind));
    store_u16_le(bytes, offset + format::kLogicalTypeOffset, column.logical_type.code());
    store_u16_le(bytes, offset + format::kTypeParameter0Offset, column.logical_type.parameter_0());
    store_u16_le(bytes, offset + format::kTypeParameter1Offset, column.logical_type.parameter_1());
    const std::uint32_t flags =
        (column.nullable ? format::kNullableColumnFlag : 0U) |
        (column.event_time ? format::kEventTimeColumnFlag : 0U) |
        (column.ordering_ordinal.has_value() ? format::kPhysicalOrderingColumnFlag : 0U);
    store_u32_le(bytes, offset + format::kColumnFlagsOffset, flags);
    store_u32_le(bytes, offset + format::kSchemaOrdinalOffset,
                 column.schema_ordinal.value_or(format::kAbsentOrdinal));
    store_u32_le(bytes, offset + format::kOrderingOrdinalOffset,
                 column.ordering_ordinal.value_or(format::kAbsentOrdinal));
  }

  for (std::size_t ordinal = 0U; ordinal < input.granules.size(); ++ordinal) {
    const CsegGranuleDescriptor& granule = input.granules[ordinal];
    const std::size_t offset = static_cast<std::size_t>(metadata->granules_offset) +
                               ordinal * format::kGranuleDescriptorLength;
    store_u64_le(bytes, offset + format::kGranuleFirstRowOffset, granule.first_row);
    store_u32_le(bytes, offset + format::kGranuleRowCountOffset, granule.row_count);
    store_u32_le(bytes, offset + format::kGranulePageCountOffset, metadata->stored_column_count);
    store_u64_le(bytes, offset + format::kGranuleFirstPageIndexOffset, granule.first_page_index);
    store_i64_le(bytes, offset + format::kGranuleMinimumEventTimeOffset,
                 granule.minimum_event_time);
    store_i64_le(bytes, offset + format::kGranuleMaximumEventTimeOffset,
                 granule.maximum_event_time);
  }

  for (std::size_t index = 0U; index < pages.size(); ++index) {
    const CsegPageDescriptor& page = pages[index];
    const std::size_t offset =
        static_cast<std::size_t>(metadata->pages_offset) + index * format::kPageDescriptorLength;
    store_u32_le(bytes, offset + format::kPageGranuleOrdinalOffset, page.granule_ordinal);
    store_u32_le(bytes, offset + format::kPageStoredColumnOrdinalOffset,
                 page.stored_column_ordinal);
    store_u16_le(bytes, offset + format::kPagePhysicalEncodingOffset,
                 format::kPlainPhysicalEncoding);
    store_u16_le(bytes, offset + format::kPageCompressionOffset,
                 static_cast<std::uint16_t>(page.compression));
    store_u32_le(bytes, offset + format::kPageRowCountOffset, page.row_count);
    store_u32_le(bytes, offset + format::kPageNullCountOffset, page.null_count);
    store_u64_le(bytes, offset + format::kPageOffsetFieldOffset, page.page_offset);
    store_u64_le(bytes, offset + format::kPageStoredLengthOffset, page.stored_length);
    store_u64_le(bytes, offset + format::kPageUncompressedLengthOffset, page.uncompressed_length);
    store_u64_le(bytes, offset + format::kPageValidityLengthOffset, page.validity_length);
    store_u64_le(bytes, offset + format::kPageOffsetsLengthOffset, page.offsets_length);
    store_u64_le(bytes, offset + format::kPageValuesLengthOffset, page.values_length);
    store_u32_le(bytes, offset + format::kPageCrc32cOffset, page.page_crc32c);
  }

  store_u32_le(bytes, format::kHeaderCrc32cOffset,
               common::crc32c(common::ByteView{storage}.first(format::kHeaderCrc32cOffset)));
  const std::size_t metadata_crc_offset = storage.size() - format::kMetadataCrc32cLength;
  store_u32_le(bytes, metadata_crc_offset,
               common::crc32c(common::ByteView{storage}.first(metadata_crc_offset)));
  return EncodedCsegMetadata{std::move(storage), cursor};
}

CsegMetadataDecodeResult decode_cseg_v1_metadata_prefix(const common::ByteView bytes,
                                                        const CsegMetadataDecodeLimits limits) {
  if (!valid_limits(limits)) {
    return std::unexpected(resource_limit("CSEG metadata decode limits are outside v1 bounds"));
  }
  if (bytes.size() < format::kMagic.size()) {
    return std::unexpected(
        incomplete("CSEG metadata requires the complete magic", format::kMagic.size()));
  }
  if (!std::equal(format::kMagic.begin(), format::kMagic.end(), bytes.begin())) {
    return std::unexpected(corruption("CSEG magic mismatch"));
  }
  if (bytes.size() < format::kFileHeaderLength) {
    return std::unexpected(incomplete("CSEG metadata requires the complete 256-byte header",
                                      format::kFileHeaderLength));
  }
  const common::ByteView header = bytes.first(format::kFileHeaderLength);
  if (common::crc32c(header.first(format::kHeaderCrc32cOffset)) !=
      load_u32_le(header, format::kHeaderCrc32cOffset)) {
    return std::unexpected(corruption("CSEG header CRC32C mismatch"));
  }
  const std::uint16_t major = load_u16_le(header, format::kFormatMajorOffset);
  const std::uint16_t minor = load_u16_le(header, format::kFormatMinorOffset);
  if (major == 0U) {
    return std::unexpected(corruption("CSEG format major version zero is invalid"));
  }
  if (major != format::kFormatMajor || minor != format::kFormatMinor) {
    return std::unexpected(unsupported("CSEG format version is unsupported"));
  }
  if (load_u32_le(header, format::kFileFlagsOffset) != 0U) {
    return std::unexpected(unsupported("CSEG required file flags are unsupported"));
  }
  if (load_u32_le(header, format::kHeaderLengthOffset) != format::kFileHeaderLength ||
      load_u32_le(header, format::kHeaderReserved0Offset) != 0U ||
      !is_zero(header.subspan(format::kHeaderReserved1Offset,
                              format::kHeaderCrc32cOffset - format::kHeaderReserved1Offset)) ||
      load_u32_le(header, format::kHeaderReserved2Offset) != 0U) {
    return std::unexpected(corruption("CSEG fixed header layout or reserved bytes are invalid"));
  }

  const std::uint64_t total_length = load_u64_le(header, format::kTotalLengthOffset);
  const std::uint64_t metadata_length = load_u64_le(header, format::kMetadataLengthOffset);
  const std::uint64_t row_count = load_u64_le(header, format::kRowCountOffset);
  const std::uint32_t user_count = load_u32_le(header, format::kUserColumnCountOffset);
  const std::uint32_t stored_count = load_u32_le(header, format::kStoredColumnCountOffset);
  const std::uint32_t granule_count = load_u32_le(header, format::kGranuleCountOffset);
  const std::uint32_t page_count = load_u32_le(header, format::kPageCountOffset);
  if (total_length == 0U || total_length > format::kMaximumFileLength ||
      (total_length % format::kAlignment) != 0U || metadata_length > total_length ||
      row_count == 0U || row_count > format::kMaximumRowCount) {
    return std::unexpected(corruption("CSEG header lengths or row count are outside v1 bounds"));
  }
  if (total_length > limits.max_file_length || metadata_length > limits.max_metadata_length ||
      user_count > limits.max_user_columns || granule_count > limits.max_granules ||
      page_count > limits.max_pages) {
    return std::unexpected(resource_limit("CSEG metadata exceeds configured decode limits"));
  }

  const common::Result<CsegMetadataLayout> layout = plan_cseg_v1_metadata_layout(
      {.user_column_count = user_count, .granule_count = granule_count});
  if (!layout.has_value() || stored_count != layout->stored_column_count ||
      page_count != layout->page_count || metadata_length != layout->metadata_length ||
      load_u64_le(header, format::kColumnsOffsetFieldOffset) != layout->columns_offset ||
      load_u64_le(header, format::kGranulesOffsetFieldOffset) != layout->granules_offset ||
      load_u64_le(header, format::kPagesOffsetFieldOffset) != layout->pages_offset ||
      load_u64_le(header, format::kPageDataOffsetFieldOffset) != layout->metadata_length) {
    return std::unexpected(
        corruption("CSEG header counts and canonical metadata offsets disagree"));
  }
  if (metadata_length > std::numeric_limits<std::size_t>::max()) {
    return std::unexpected(resource_limit("CSEG metadata does not fit this platform"));
  }
  if (bytes.size() < static_cast<std::size_t>(metadata_length)) {
    return std::unexpected(incomplete("CSEG metadata prefix is incomplete", metadata_length));
  }
  const common::ByteView metadata = bytes.first(static_cast<std::size_t>(metadata_length));
  const std::size_t metadata_crc_offset = metadata.size() - format::kMetadataCrc32cLength;
  if (!is_zero(metadata.subspan(metadata_crc_offset - format::kMetadataTrailerPaddingLength,
                                format::kMetadataTrailerPaddingLength)) ||
      common::crc32c(metadata.first(metadata_crc_offset)) !=
          load_u32_le(metadata, metadata_crc_offset)) {
    return std::unexpected(corruption("CSEG metadata trailer padding or CRC32C is invalid"));
  }

  const common::Result<PartId> part_id = parse_identifier<PartId>(header, format::kPartIdOffset);
  const common::Result<schema::TableId> table_id =
      parse_identifier<schema::TableId>(header, format::kTableIdOffset);
  const common::Result<schema::TabletId> tablet_id =
      parse_identifier<schema::TabletId>(header, format::kTabletIdOffset);
  const common::Result<schema::SchemaId> schema_id =
      parse_identifier<schema::SchemaId>(header, format::kSchemaIdOffset);
  const common::Result<schema::SchemaVersion> schema_version =
      schema::SchemaVersion::from_value(load_u64_le(header, format::kSchemaVersionOffset));
  if (!part_id.has_value() || !table_id.has_value() || !tablet_id.has_value() ||
      !schema_id.has_value() || !schema_version.has_value()) {
    return std::unexpected(corruption("CSEG header contains a zero identity or schema version"));
  }

  std::vector<CsegColumnDescriptor> columns;
  columns.reserve(stored_count);
  for (std::uint32_t ordinal = 0U; ordinal < stored_count; ++ordinal) {
    const std::size_t offset = static_cast<std::size_t>(layout->columns_offset) +
                               static_cast<std::size_t>(ordinal) * format::kColumnDescriptorLength;
    const common::ByteView descriptor = metadata.subspan(offset, format::kColumnDescriptorLength);
    if (!is_zero(descriptor.subspan(format::kColumnReservedOffset))) {
      return std::unexpected(corruption("CSEG column descriptor reserved bytes are nonzero"));
    }
    const common::Result<StorageKind> kind =
        storage_kind_from_code(load_u16_le(descriptor, format::kStorageKindOffset));
    if (!kind.has_value()) {
      return std::unexpected(kind.error().code() == common::StatusCode::kNotSupported
                                 ? unsupported(kind.error().message())
                                 : corruption(kind.error().message()));
    }
    const std::uint16_t type_code = load_u16_le(descriptor, format::kLogicalTypeOffset);
    if (type_code == 0U) {
      return std::unexpected(corruption("CSEG logical type code zero is invalid"));
    }
    const common::Result<schema::LogicalTypeKind> type_kind =
        schema::logical_type_kind_from_code(type_code);
    if (!type_kind.has_value()) {
      return std::unexpected(unsupported("CSEG logical type code is unsupported"));
    }
    const common::Result<schema::LogicalType> logical_type = schema::LogicalType::create(
        *type_kind, load_u16_le(descriptor, format::kTypeParameter0Offset),
        load_u16_le(descriptor, format::kTypeParameter1Offset));
    if (!logical_type.has_value()) {
      return std::unexpected(corruption("CSEG logical type parameters are invalid"));
    }
    const std::uint32_t flags = load_u32_le(descriptor, format::kColumnFlagsOffset);
    constexpr std::uint32_t known_flags = format::kNullableColumnFlag |
                                          format::kEventTimeColumnFlag |
                                          format::kPhysicalOrderingColumnFlag;
    if ((flags & ~known_flags) != 0U) {
      return std::unexpected(unsupported("CSEG column required flags are unsupported"));
    }
    const bool user = *kind == StorageKind::kUser;
    std::optional<schema::ColumnId> column_id;
    if (user) {
      common::Result<schema::ColumnId> parsed =
          parse_identifier<schema::ColumnId>(descriptor, format::kColumnIdOffset);
      if (!parsed.has_value()) {
        return std::unexpected(corruption("CSEG user column identity is zero"));
      }
      column_id = *parsed;
    } else if (!is_zero(descriptor.first(common::Uuid::kSize))) {
      return std::unexpected(corruption("CSEG system column identity bytes are nonzero"));
    }
    const std::uint32_t schema_ordinal = load_u32_le(descriptor, format::kSchemaOrdinalOffset);
    const std::uint32_t ordering_ordinal = load_u32_le(descriptor, format::kOrderingOrdinalOffset);
    const bool ordering_flag = (flags & format::kPhysicalOrderingColumnFlag) != 0U;
    if ((ordering_flag && ordering_ordinal == format::kAbsentOrdinal) ||
        (!ordering_flag && ordering_ordinal != format::kAbsentOrdinal)) {
      return std::unexpected(corruption("CSEG ordering flag and ordinal disagree"));
    }
    columns.push_back(CsegColumnDescriptor{
        .column_id = column_id,
        .storage_kind = *kind,
        .logical_type = *logical_type,
        .nullable = (flags & format::kNullableColumnFlag) != 0U,
        .event_time = (flags & format::kEventTimeColumnFlag) != 0U,
        .schema_ordinal = schema_ordinal == format::kAbsentOrdinal
                              ? std::nullopt
                              : std::optional<std::uint32_t>{schema_ordinal},
        .ordering_ordinal =
            ordering_flag ? std::optional<std::uint32_t>{ordering_ordinal} : std::nullopt,
    });
  }

  std::vector<CsegGranuleDescriptor> granules;
  granules.reserve(granule_count);
  for (std::uint32_t ordinal = 0U; ordinal < granule_count; ++ordinal) {
    const std::size_t offset = static_cast<std::size_t>(layout->granules_offset) +
                               static_cast<std::size_t>(ordinal) * format::kGranuleDescriptorLength;
    const common::ByteView descriptor = metadata.subspan(offset, format::kGranuleDescriptorLength);
    if (load_u32_le(descriptor, format::kGranulePageCountOffset) != stored_count ||
        !is_zero(descriptor.subspan(format::kGranuleReservedOffset))) {
      return std::unexpected(corruption("CSEG granule page count or reserved bytes are invalid"));
    }
    granules.push_back(CsegGranuleDescriptor{
        .first_row = load_u64_le(descriptor, format::kGranuleFirstRowOffset),
        .row_count = load_u32_le(descriptor, format::kGranuleRowCountOffset),
        .first_page_index = load_u64_le(descriptor, format::kGranuleFirstPageIndexOffset),
        .minimum_event_time = load_i64_le(descriptor, format::kGranuleMinimumEventTimeOffset),
        .maximum_event_time = load_i64_le(descriptor, format::kGranuleMaximumEventTimeOffset),
    });
  }

  std::vector<CsegPageDescriptor> pages;
  pages.reserve(page_count);
  for (std::uint32_t index = 0U; index < page_count; ++index) {
    const std::size_t offset = static_cast<std::size_t>(layout->pages_offset) +
                               static_cast<std::size_t>(index) * format::kPageDescriptorLength;
    const common::ByteView descriptor = metadata.subspan(offset, format::kPageDescriptorLength);
    const std::uint16_t encoding = load_u16_le(descriptor, format::kPagePhysicalEncodingOffset);
    if (encoding == 0U) {
      return std::unexpected(corruption("CSEG physical encoding code zero is invalid"));
    }
    if (encoding != format::kPlainPhysicalEncoding) {
      return std::unexpected(unsupported("CSEG physical encoding is unsupported"));
    }
    const common::Result<PageCompression> compression =
        compression_from_code(load_u16_le(descriptor, format::kPageCompressionOffset));
    if (!compression.has_value()) {
      return std::unexpected(compression.error().code() == common::StatusCode::kNotSupported
                                 ? unsupported(compression.error().message())
                                 : corruption(compression.error().message()));
    }
    if (load_u32_le(descriptor, format::kPageFlagsOffset) != 0U) {
      return std::unexpected(unsupported("CSEG page required flags are unsupported"));
    }
    if (load_u32_le(descriptor, format::kPageReservedOffset) != 0U) {
      return std::unexpected(corruption("CSEG page descriptor reserved bytes are nonzero"));
    }
    pages.push_back(CsegPageDescriptor{
        .granule_ordinal = load_u32_le(descriptor, format::kPageGranuleOrdinalOffset),
        .stored_column_ordinal = load_u32_le(descriptor, format::kPageStoredColumnOrdinalOffset),
        .compression = *compression,
        .row_count = load_u32_le(descriptor, format::kPageRowCountOffset),
        .null_count = load_u32_le(descriptor, format::kPageNullCountOffset),
        .page_offset = load_u64_le(descriptor, format::kPageOffsetFieldOffset),
        .stored_length = load_u64_le(descriptor, format::kPageStoredLengthOffset),
        .uncompressed_length = load_u64_le(descriptor, format::kPageUncompressedLengthOffset),
        .validity_length = load_u64_le(descriptor, format::kPageValidityLengthOffset),
        .offsets_length = load_u64_le(descriptor, format::kPageOffsetsLengthOffset),
        .values_length = load_u64_le(descriptor, format::kPageValuesLengthOffset),
        .page_crc32c = load_u32_le(descriptor, format::kPageCrc32cOffset),
    });
  }

  const std::uint32_t event_time_ordinal =
      load_u32_le(header, format::kEventTimeColumnOrdinalOffset);
  const std::uint32_t ordering_column_count =
      load_u32_le(header, format::kOrderingColumnCountOffset);
  const std::int64_t minimum_event_time = load_i64_le(header, format::kMinimumEventTimeOffset);
  const std::int64_t maximum_event_time = load_i64_le(header, format::kMaximumEventTimeOffset);
  if (const auto error = validate_model(
          total_length, metadata_length, row_count, event_time_ordinal, ordering_column_count,
          minimum_event_time, maximum_event_time, columns, granules, pages);
      error.has_value()) {
    return std::unexpected(*error);
  }
  return DecodedCsegMetadataView{
      *part_id,
      *table_id,
      *tablet_id,
      *schema_id,
      *schema_version,
      total_length,
      row_count,
      event_time_ordinal,
      ordering_column_count,
      minimum_event_time,
      maximum_event_time,
      std::move(columns),
      std::move(granules),
      std::move(pages),
      metadata,
  };
}

CsegMetadataDecodeResult decode_cseg_v1_metadata_exact(const common::ByteView bytes,
                                                       const CsegMetadataDecodeLimits limits) {
  CsegMetadataDecodeResult decoded = decode_cseg_v1_metadata_prefix(bytes, limits);
  if (!decoded.has_value()) {
    return decoded;
  }
  if (bytes.size() != decoded->encoded_metadata().size()) {
    return std::unexpected(corruption("CSEG metadata exact decoder rejects trailing bytes"));
  }
  return decoded;
}

common::Status validate_cseg_v1_metadata_schema(const DecodedCsegMetadataView& metadata,
                                                const schema::TableSchema& schema_value,
                                                const schema::TabletId& target_tablet) {
  if (metadata.table_id() != schema_value.table_id() ||
      metadata.schema_id() != schema_value.schema_id() ||
      metadata.schema_version() != schema_value.version() ||
      metadata.tablet_id() != target_tablet ||
      metadata.columns().size() != schema_value.columns().size() + format::kSystemColumnCount) {
    return status(common::StatusCode::kInvalidArgument,
                  "CSEG table, tablet, schema identity, version, or column count does not bind");
  }
  for (std::size_t ordinal = 0U; ordinal < schema_value.columns().size(); ++ordinal) {
    const CsegColumnDescriptor& encoded = metadata.columns()[ordinal];
    const schema::ColumnDefinition& expected = schema_value.columns()[ordinal];
    const bool event_time = expected.id() == schema_value.event_time_column();
    const auto ordering = std::ranges::find(schema_value.physical_ordering_key(), expected.id());
    const bool ordered = ordering != schema_value.physical_ordering_key().end();
    const std::optional<std::uint32_t> ordering_ordinal =
        ordered ? std::optional<std::uint32_t>{static_cast<std::uint32_t>(
                      std::distance(schema_value.physical_ordering_key().begin(), ordering))}
                : std::nullopt;
    if (encoded.storage_kind != StorageKind::kUser || encoded.column_id != expected.id() ||
        encoded.schema_ordinal != static_cast<std::uint32_t>(ordinal) ||
        encoded.logical_type != expected.type() || encoded.nullable != expected.nullable() ||
        encoded.event_time != event_time || encoded.ordering_ordinal != ordering_ordinal) {
      return status(common::StatusCode::kInvalidArgument,
                    "CSEG user descriptor does not exactly bind to its schema ordinal and roles");
    }
  }
  const std::optional<std::size_t> event_ordinal =
      schema_value.column_ordinal(schema_value.event_time_column());
  if (!event_ordinal.has_value() ||
      metadata.event_time_column_ordinal() != static_cast<std::uint32_t>(*event_ordinal) ||
      metadata.ordering_column_count() != schema_value.physical_ordering_key().size()) {
    return status(common::StatusCode::kInvalidArgument,
                  "CSEG event-time or physical ordering header does not bind to the schema");
  }
  return common::Status::ok();
}

} // namespace chronos::cseg
