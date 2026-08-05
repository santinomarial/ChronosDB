#include "chronos/columnar/columnar_batch_codec.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace chronos::columnar {
namespace {

static_assert(format::kMaximumColumnCount == schema::kMaximumSchemaColumnCount);

[[nodiscard]] common::Status invalid_argument(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status resource_exhausted(std::string message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] ColumnarBatchDecodeError incomplete(std::string message,
                                                  const std::size_t required_size) {
  return ColumnarBatchDecodeError{
      ColumnarBatchDecodeErrorKind::kIncomplete,
      common::Status{common::StatusCode::kOutOfRange, std::move(message)}, required_size};
}

[[nodiscard]] ColumnarBatchDecodeError invalid(std::string message) {
  return ColumnarBatchDecodeError{
      ColumnarBatchDecodeErrorKind::kInvalid,
      common::Status{common::StatusCode::kCorruption, std::move(message)}};
}

[[nodiscard]] ColumnarBatchDecodeError unsupported(std::string message) {
  return ColumnarBatchDecodeError{
      ColumnarBatchDecodeErrorKind::kUnsupported,
      common::Status{common::StatusCode::kNotSupported, std::move(message)}};
}

[[nodiscard]] ColumnarBatchDecodeError limit_exceeded(std::string message) {
  return ColumnarBatchDecodeError{
      ColumnarBatchDecodeErrorKind::kResourceLimit,
      common::Status{common::StatusCode::kResourceExhausted, std::move(message)}};
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

void store_u16_le(const common::MutableByteView bytes, const std::size_t offset,
                  const std::uint16_t value) noexcept {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
  }
}

void store_u32_le(const common::MutableByteView bytes, const std::size_t offset,
                  const std::uint32_t value) noexcept {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
  }
}

void store_u64_le(const common::MutableByteView bytes, const std::size_t offset,
                  const std::uint64_t value) noexcept {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
  }
}

[[nodiscard]] bool is_zero(const common::ByteView bytes) noexcept {
  return std::ranges::all_of(bytes, [](const std::byte value) { return value == std::byte{0}; });
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

[[nodiscard]] common::Result<std::size_t> aligned_end(const std::size_t offset,
                                                      const std::size_t length) {
  const std::optional<std::size_t> end = common::checked_range_end(offset, length);
  if (!end.has_value()) {
    return common::make_unexpected(resource_exhausted("columnar batch buffer end overflowed"));
  }
  return common::checked_align_up(*end, format::kAlignment);
}

[[nodiscard]] common::Result<BufferLayout> place_buffer(std::size_t& cursor,
                                                        const std::size_t length) {
  if (length == 0U) {
    return BufferLayout{.offset = 0U, .length = 0U};
  }
  const std::size_t offset = cursor;
  const common::Result<std::size_t> next = aligned_end(offset, length);
  if (!next.has_value()) {
    return common::make_unexpected(next.error());
  }
  cursor = *next;
  return BufferLayout{.offset = offset, .length = length};
}

void copy_buffer(const common::MutableByteView destination, const BufferLayout layout,
                 const common::ByteView source) {
  if (!source.empty()) {
    std::memcpy(destination.data() + layout.offset, source.data(), source.size());
  }
}

[[nodiscard]] common::Result<std::size_t> exact_values_length(const schema::LogicalType type,
                                                              const std::uint32_t row_count) {
  if (type.is_variable_width()) {
    return common::make_unexpected(
        invalid_argument("variable values length is descriptor-defined"));
  }
  if (type.kind() == schema::LogicalTypeKind::kBool) {
    return bitmap_size(row_count);
  }
  const std::optional<std::size_t> length =
      common::checked_multiply(static_cast<std::size_t>(row_count), fixed_width(type.kind()));
  if (!length.has_value()) {
    return common::make_unexpected(resource_exhausted("fixed-width values length overflowed"));
  }
  return *length;
}

[[nodiscard]] common::Result<schema::ColumnId> read_column_id(const common::ByteView bytes,
                                                              const std::size_t offset) {
  common::Uuid::Bytes raw{};
  std::copy_n(bytes.begin() + static_cast<std::ptrdiff_t>(offset), raw.size(), raw.begin());
  return schema::ColumnId::from_bytes(raw);
}

template <typename Identifier>
[[nodiscard]] common::Result<Identifier> read_identifier(const common::ByteView bytes,
                                                         const std::size_t offset) {
  common::Uuid::Bytes raw{};
  std::copy_n(bytes.begin() + static_cast<std::ptrdiff_t>(offset), raw.size(), raw.begin());
  return Identifier::from_bytes(raw);
}

struct ParsedBuffer {
  std::size_t offset;
  std::size_t length;
};

struct EncodedBufferSpec {
  std::size_t field_offset;
  std::size_t expected_length;
  const char* label;
};

struct PendingColumn {
  ColumnVectorMetadata metadata;
  ColumnVectorBufferView buffers;
};

[[nodiscard]] std::expected<ParsedBuffer, ColumnarBatchDecodeError>
parse_buffer(const common::ByteView batch, const EncodedBufferSpec spec, std::size_t& cursor,
             const std::size_t buffers_end) {
  const std::uint64_t encoded_offset = load_u64_le(batch, spec.field_offset);
  const std::uint64_t encoded_length =
      load_u64_le(batch, spec.field_offset + sizeof(std::uint64_t));
  if (spec.expected_length == 0U) {
    if (encoded_offset != 0U || encoded_length != 0U) {
      return std::unexpected(invalid(std::string{spec.label} + " absent buffer is not (0,0)"));
    }
    return ParsedBuffer{.offset = 0U, .length = 0U};
  }
  if (encoded_length != spec.expected_length || encoded_offset != cursor) {
    return std::unexpected(
        invalid(std::string{spec.label} + " buffer does not match the canonical layout"));
  }
  const std::optional<std::size_t> end = common::checked_range_end(cursor, spec.expected_length);
  if (!end.has_value() || *end > buffers_end) {
    return std::unexpected(
        invalid(std::string{spec.label} + " buffer extends beyond the data region"));
  }
  const common::Result<std::size_t> next = common::checked_align_up(*end, format::kAlignment);
  if (!next.has_value() || *next > buffers_end) {
    return std::unexpected(invalid(std::string{spec.label} + " buffer alignment is invalid"));
  }
  if (!is_zero(batch.subspan(*end, *next - *end))) {
    return std::unexpected(invalid(std::string{spec.label} + " buffer padding is nonzero"));
  }
  const ParsedBuffer parsed{.offset = cursor, .length = spec.expected_length};
  cursor = *next;
  return parsed;
}

[[nodiscard]] common::ByteView buffer_view(const common::ByteView batch,
                                           const ParsedBuffer buffer) noexcept {
  if (buffer.length == 0U) {
    return {};
  }
  return batch.subspan(buffer.offset, buffer.length);
}

} // namespace

ColumnarBatchLayout::ColumnarBatchLayout(const std::size_t total_length,
                                         std::vector<ColumnLayout> columns) noexcept
    : total_length_(total_length), columns_(std::move(columns)) {}

std::size_t ColumnarBatchLayout::total_length() const noexcept {
  return total_length_;
}

std::span<const ColumnLayout> ColumnarBatchLayout::columns() const noexcept {
  return columns_;
}

common::Result<ColumnarBatchLayout> plan_columnar_batch_v1_layout(const OwnedColumnarBatch& batch) {
  const std::optional<std::size_t> descriptor_bytes =
      common::checked_multiply(batch.columns().size(), format::kColumnDescriptorLength);
  if (!descriptor_bytes.has_value()) {
    return common::make_unexpected(resource_exhausted("column descriptor table overflowed"));
  }
  const std::optional<std::size_t> descriptor_end =
      common::checked_add(format::kBatchHeaderLength, *descriptor_bytes);
  if (!descriptor_end.has_value()) {
    return common::make_unexpected(resource_exhausted("column descriptor table end overflowed"));
  }

  std::size_t cursor = *descriptor_end;
  std::vector<ColumnLayout> columns;
  columns.reserve(batch.columns().size());
  for (const OwnedColumnVector& column : batch.columns()) {
    const ColumnVectorView view = column.view();
    const common::Result<BufferLayout> validity = place_buffer(cursor, view.validity().size());
    if (!validity.has_value()) {
      return common::make_unexpected(validity.error());
    }
    const common::Result<BufferLayout> offsets = place_buffer(cursor, view.offsets().size());
    if (!offsets.has_value()) {
      return common::make_unexpected(offsets.error());
    }
    const common::Result<BufferLayout> values = place_buffer(cursor, view.values().size());
    if (!values.has_value()) {
      return common::make_unexpected(values.error());
    }
    columns.push_back(ColumnLayout{.validity = *validity, .offsets = *offsets, .values = *values});
  }

  const std::optional<std::size_t> total =
      common::checked_add(cursor, format::kTerminalPaddingLength + format::kBatchTrailerLength);
  if (!total.has_value() || *total > format::kMaximumEmbeddedBatchLength) {
    return common::make_unexpected(
        resource_exhausted("columnar batch encoded length exceeds the v1 maximum"));
  }
  if ((*total % format::kAlignment) != 0U) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "canonical batch layout is not aligned"});
  }
  return ColumnarBatchLayout{*total, std::move(columns)};
}

EncodedColumnarBatch::EncodedColumnarBatch(std::vector<std::byte> bytes) noexcept
    : bytes_(std::move(bytes)) {}

common::ByteView EncodedColumnarBatch::bytes() const noexcept {
  return bytes_;
}

std::size_t EncodedColumnarBatch::size() const noexcept {
  return bytes_.size();
}

common::Result<EncodedColumnarBatch> encode_columnar_batch_v1(const OwnedColumnarBatch& batch) {
  const common::Result<ColumnarBatchLayout> layout = plan_columnar_batch_v1_layout(batch);
  if (!layout.has_value()) {
    return common::make_unexpected(layout.error());
  }

  std::vector<std::byte> storage(layout->total_length(), std::byte{0});
  const common::MutableByteView bytes{storage};
  std::copy(format::kMagic.begin(), format::kMagic.end(), storage.begin());
  store_u16_le(bytes, format::kFormatMajorOffset, format::kFormatMajor);
  store_u16_le(bytes, format::kFormatMinorOffset, format::kFormatMinor);
  store_u32_le(bytes, format::kHeaderLengthOffset,
               static_cast<std::uint32_t>(format::kBatchHeaderLength));
  store_u32_le(bytes, format::kBatchFlagsOffset, 0U);
  store_u32_le(bytes, format::kRowCountOffset, batch.row_count());
  store_u32_le(bytes, format::kColumnCountOffset,
               static_cast<std::uint32_t>(batch.columns().size()));
  store_u32_le(bytes, format::kColumnDescriptorLengthOffset,
               static_cast<std::uint32_t>(format::kColumnDescriptorLength));
  store_u64_le(bytes, format::kTotalLengthOffset, layout->total_length());
  std::copy(batch.schema().table_id().bytes().begin(), batch.schema().table_id().bytes().end(),
            storage.begin() + static_cast<std::ptrdiff_t>(format::kTableIdOffset));
  std::copy(batch.schema().schema_id().bytes().begin(), batch.schema().schema_id().bytes().end(),
            storage.begin() + static_cast<std::ptrdiff_t>(format::kSchemaIdOffset));
  store_u64_le(bytes, format::kSchemaVersionOffset, batch.schema().version().value());
  store_u64_le(bytes, format::kDescriptorsOffsetFieldOffset, format::kDescriptorsOffset);

  for (std::size_t ordinal = 0U; ordinal < batch.columns().size(); ++ordinal) {
    const OwnedColumnVector& column = batch.columns()[ordinal];
    const ColumnVectorView view = column.view();
    const ColumnLayout& column_layout = layout->columns()[ordinal];
    const std::size_t descriptor =
        format::kDescriptorsOffset + ordinal * format::kColumnDescriptorLength;
    std::copy(column.column_id().bytes().begin(), column.column_id().bytes().end(),
              storage.begin() + static_cast<std::ptrdiff_t>(descriptor + format::kColumnIdOffset));
    store_u16_le(bytes, descriptor + format::kLogicalTypeOffset, column.type().code());
    store_u16_le(bytes, descriptor + format::kPhysicalEncodingOffset,
                 format::kPlainPhysicalEncoding);
    store_u16_le(bytes, descriptor + format::kTypeParameter0Offset, column.type().parameter_0());
    store_u16_le(bytes, descriptor + format::kTypeParameter1Offset, column.type().parameter_1());
    store_u32_le(bytes, descriptor + format::kColumnFlagsOffset,
                 column.nullable() ? format::kNullableColumnFlag : 0U);
    store_u32_le(bytes, descriptor + format::kNullCountOffset, column.null_count());
    store_u64_le(bytes, descriptor + format::kValidityOffset, column_layout.validity.offset);
    store_u64_le(bytes, descriptor + format::kValidityLengthOffset, column_layout.validity.length);
    store_u64_le(bytes, descriptor + format::kOffsetsOffset, column_layout.offsets.offset);
    store_u64_le(bytes, descriptor + format::kOffsetsLengthOffset, column_layout.offsets.length);
    store_u64_le(bytes, descriptor + format::kValuesOffset, column_layout.values.offset);
    store_u64_le(bytes, descriptor + format::kValuesLengthOffset, column_layout.values.length);
    copy_buffer(bytes, column_layout.validity, view.validity());
    copy_buffer(bytes, column_layout.offsets, view.offsets());
    copy_buffer(bytes, column_layout.values, view.values());
  }

  store_u32_le(bytes, format::kHeaderCrc32cOffset,
               common::crc32c(common::ByteView{storage}.first(format::kHeaderCrc32cOffset)));
  const std::size_t trailer_offset = storage.size() - format::kBatchTrailerLength;
  store_u32_le(bytes, trailer_offset,
               common::crc32c(common::ByteView{storage}.first(trailer_offset)));
  return EncodedColumnarBatch{std::move(storage)};
}

ColumnarBatchDecodeError::ColumnarBatchDecodeError(const ColumnarBatchDecodeErrorKind kind,
                                                   common::Status status,
                                                   const std::size_t required_size) noexcept
    : kind_(kind), status_(std::move(status)), required_size_(required_size) {}

DecodedColumnarBatchView::DecodedColumnarBatchView(const schema::TableId table_id,
                                                   const schema::SchemaId schema_id,
                                                   const schema::SchemaVersion schema_version,
                                                   const std::uint32_t row_count,
                                                   std::vector<ColumnVectorView> columns,
                                                   const common::ByteView encoded_bytes) noexcept
    : table_id_(table_id), schema_id_(schema_id), schema_version_(schema_version),
      row_count_(row_count), columns_(std::move(columns)), encoded_bytes_(encoded_bytes) {}

std::span<const ColumnVectorView> DecodedColumnarBatchView::columns() const noexcept {
  return columns_;
}

const ColumnVectorView* DecodedColumnarBatchView::column(const std::size_t ordinal) const noexcept {
  if (ordinal >= columns_.size()) {
    return nullptr;
  }
  return &columns_[ordinal];
}

common::ByteView DecodedColumnarBatchView::encoded_bytes() const noexcept {
  return encoded_bytes_;
}

ColumnarBatchDecodeResult decode_columnar_batch_v1_prefix(const common::ByteView bytes,
                                                          const ColumnarBatchDecodeLimits limits) {
  if (limits.max_batch_length == 0U || limits.max_rows == 0U || limits.max_columns == 0U ||
      limits.max_batch_length > format::kMaximumEmbeddedBatchLength ||
      limits.max_columns > format::kMaximumColumnCount) {
    return std::unexpected(limit_exceeded("columnar batch decode limits are outside v1 bounds"));
  }
  if (bytes.size() < format::kBatchHeaderLength) {
    return std::unexpected(incomplete("columnar batch requires a complete 96-byte header",
                                      format::kBatchHeaderLength));
  }
  const common::ByteView header = bytes.first(format::kBatchHeaderLength);
  if (!std::equal(format::kMagic.begin(), format::kMagic.end(), header.begin())) {
    return std::unexpected(invalid("columnar batch magic mismatch"));
  }
  const std::uint32_t stored_header_crc = load_u32_le(header, format::kHeaderCrc32cOffset);
  if (common::crc32c(header.first(format::kHeaderCrc32cOffset)) != stored_header_crc) {
    return std::unexpected(invalid("columnar batch header CRC32C mismatch"));
  }

  if (load_u16_le(header, format::kFormatMajorOffset) != format::kFormatMajor ||
      load_u16_le(header, format::kFormatMinorOffset) != format::kFormatMinor) {
    return std::unexpected(unsupported("columnar batch format version is unsupported"));
  }
  if (load_u32_le(header, format::kBatchFlagsOffset) != 0U) {
    return std::unexpected(unsupported("columnar batch required flags are unsupported"));
  }
  if (load_u32_le(header, format::kHeaderLengthOffset) != format::kBatchHeaderLength ||
      load_u32_le(header, format::kColumnDescriptorLengthOffset) !=
          format::kColumnDescriptorLength ||
      load_u64_le(header, format::kDescriptorsOffsetFieldOffset) != format::kDescriptorsOffset) {
    return std::unexpected(invalid("columnar batch fixed layout fields are invalid"));
  }
  if (load_u32_le(header, format::kHeaderReservedOffset) != 0U) {
    return std::unexpected(invalid("columnar batch header reserved field is nonzero"));
  }

  const std::uint32_t row_count = load_u32_le(header, format::kRowCountOffset);
  const std::uint32_t column_count = load_u32_le(header, format::kColumnCountOffset);
  const std::uint64_t encoded_total_length = load_u64_le(header, format::kTotalLengthOffset);
  if (row_count == 0U || column_count == 0U || column_count > format::kMaximumColumnCount) {
    return std::unexpected(invalid("columnar batch row or column count is outside v1 bounds"));
  }
  if (encoded_total_length == 0U || encoded_total_length > format::kMaximumEmbeddedBatchLength ||
      (encoded_total_length % format::kAlignment) != 0U) {
    return std::unexpected(invalid("columnar batch total length is outside v1 bounds"));
  }
  const auto total_length = static_cast<std::size_t>(encoded_total_length);
  const std::optional<std::size_t> descriptor_bytes = common::checked_multiply(
      static_cast<std::size_t>(column_count), format::kColumnDescriptorLength);
  const std::optional<std::size_t> descriptor_end =
      descriptor_bytes.has_value()
          ? common::checked_add(format::kDescriptorsOffset, *descriptor_bytes)
          : std::nullopt;
  if (!descriptor_end.has_value() ||
      *descriptor_end >
          total_length - (format::kTerminalPaddingLength + format::kBatchTrailerLength)) {
    return std::unexpected(invalid("columnar batch descriptor table does not fit"));
  }
  if (total_length > limits.max_batch_length || row_count > limits.max_rows ||
      column_count > limits.max_columns) {
    return std::unexpected(limit_exceeded("columnar batch exceeds configured decode limits"));
  }
  if (bytes.size() < total_length) {
    return std::unexpected(
        incomplete("complete columnar batch extends beyond input", total_length));
  }

  const common::ByteView batch = bytes.first(total_length);
  const std::size_t trailer_offset = total_length - format::kBatchTrailerLength;
  if (common::crc32c(batch.first(trailer_offset)) != load_u32_le(batch, trailer_offset)) {
    return std::unexpected(invalid("columnar batch complete-batch CRC32C mismatch"));
  }

  const common::Result<schema::TableId> table_id =
      read_identifier<schema::TableId>(header, format::kTableIdOffset);
  const common::Result<schema::SchemaId> schema_id =
      read_identifier<schema::SchemaId>(header, format::kSchemaIdOffset);
  const common::Result<schema::SchemaVersion> schema_version =
      schema::SchemaVersion::from_value(load_u64_le(header, format::kSchemaVersionOffset));
  if (!table_id.has_value() || !schema_id.has_value() || !schema_version.has_value()) {
    return std::unexpected(invalid("columnar batch contains a zero identity or schema version"));
  }

  const std::size_t buffers_end =
      total_length - (format::kTerminalPaddingLength + format::kBatchTrailerLength);
  std::size_t cursor = *descriptor_end;
  std::vector<PendingColumn> pending_columns;
  pending_columns.reserve(column_count);
  for (std::size_t ordinal = 0U; ordinal < column_count; ++ordinal) {
    const std::size_t descriptor =
        format::kDescriptorsOffset + ordinal * format::kColumnDescriptorLength;
    const common::Result<schema::ColumnId> column_id =
        read_column_id(batch, descriptor + format::kColumnIdOffset);
    if (!column_id.has_value()) {
      return std::unexpected(invalid("columnar batch contains a zero column identity"));
    }
    for (const PendingColumn& previous : pending_columns) {
      if (previous.metadata.column_id == *column_id) {
        return std::unexpected(invalid("columnar batch contains a duplicate column identity"));
      }
    }

    const common::Result<schema::LogicalTypeKind> kind = schema::logical_type_kind_from_code(
        load_u16_le(batch, descriptor + format::kLogicalTypeOffset));
    if (!kind.has_value()) {
      return std::unexpected(unsupported("columnar batch logical type is unsupported"));
    }
    if (load_u16_le(batch, descriptor + format::kPhysicalEncodingOffset) !=
        format::kPlainPhysicalEncoding) {
      return std::unexpected(unsupported("columnar batch physical encoding is unsupported"));
    }
    const common::Result<schema::LogicalType> type = schema::LogicalType::create(
        *kind, load_u16_le(batch, descriptor + format::kTypeParameter0Offset),
        load_u16_le(batch, descriptor + format::kTypeParameter1Offset));
    if (!type.has_value()) {
      return std::unexpected(invalid("columnar batch logical type parameters are invalid"));
    }
    const std::uint32_t flags = load_u32_le(batch, descriptor + format::kColumnFlagsOffset);
    if ((flags & ~format::kNullableColumnFlag) != 0U) {
      return std::unexpected(unsupported("columnar batch column required flags are unsupported"));
    }
    const bool nullable = (flags & format::kNullableColumnFlag) != 0U;
    const std::size_t validity_length = nullable ? bitmap_size(row_count) : 0U;
    const std::optional<std::size_t> offset_count =
        common::checked_add(static_cast<std::size_t>(row_count), std::size_t{1U});
    const std::optional<std::size_t> variable_offsets_length =
        offset_count.has_value() ? common::checked_multiply(*offset_count, sizeof(std::uint32_t))
                                 : std::nullopt;
    if (type->is_variable_width() && !variable_offsets_length.has_value()) {
      return std::unexpected(invalid("columnar batch variable offsets length overflowed"));
    }
    const std::size_t offsets_length = type->is_variable_width() ? *variable_offsets_length : 0U;

    const auto validity =
        parse_buffer(batch,
                     EncodedBufferSpec{.field_offset = descriptor + format::kValidityOffset,
                                       .expected_length = validity_length,
                                       .label = "validity"},
                     cursor, buffers_end);
    if (!validity.has_value()) {
      return std::unexpected(validity.error());
    }
    const auto offsets =
        parse_buffer(batch,
                     EncodedBufferSpec{.field_offset = descriptor + format::kOffsetsOffset,
                                       .expected_length = offsets_length,
                                       .label = "offsets"},
                     cursor, buffers_end);
    if (!offsets.has_value()) {
      return std::unexpected(offsets.error());
    }

    std::size_t values_length = 0U;
    if (type->is_variable_width()) {
      const std::uint64_t encoded_values_length =
          load_u64_le(batch, descriptor + format::kValuesLengthOffset);
      if (encoded_values_length > std::numeric_limits<std::uint32_t>::max() ||
          encoded_values_length > buffers_end) {
        return std::unexpected(invalid("columnar batch variable values length is invalid"));
      }
      values_length = static_cast<std::size_t>(encoded_values_length);
    } else {
      const common::Result<std::size_t> fixed_length = exact_values_length(*type, row_count);
      if (!fixed_length.has_value()) {
        return std::unexpected(invalid("columnar batch fixed values length overflowed"));
      }
      values_length = *fixed_length;
    }
    const auto values =
        parse_buffer(batch,
                     EncodedBufferSpec{.field_offset = descriptor + format::kValuesOffset,
                                       .expected_length = values_length,
                                       .label = "values"},
                     cursor, buffers_end);
    if (!values.has_value()) {
      return std::unexpected(values.error());
    }

    pending_columns.push_back(PendingColumn{
        .metadata = ColumnVectorMetadata{.column_id = *column_id,
                                         .type = *type,
                                         .nullable = nullable,
                                         .row_count = row_count,
                                         .null_count = load_u32_le(
                                             batch, descriptor + format::kNullCountOffset)},
        .buffers = ColumnVectorBufferView{.validity = buffer_view(batch, *validity),
                                          .offsets = buffer_view(batch, *offsets),
                                          .values = buffer_view(batch, *values)}});
  }

  if (cursor != buffers_end) {
    return std::unexpected(invalid("columnar batch contains a gap or trailing data region"));
  }
  if (!is_zero(batch.subspan(buffers_end, format::kTerminalPaddingLength))) {
    return std::unexpected(invalid("columnar batch terminal padding is nonzero"));
  }

  std::vector<ColumnVectorView> columns;
  columns.reserve(column_count);
  for (const PendingColumn& pending : pending_columns) {
    const common::Result<ColumnVectorView> column =
        ColumnVectorView::create(pending.metadata, pending.buffers);
    if (!column.has_value()) {
      return std::unexpected(
          invalid(std::string{"columnar batch column is invalid: "} + column.error().message()));
    }
    columns.push_back(*column);
  }
  return DecodedColumnarBatchView{*table_id, *schema_id,         *schema_version,
                                  row_count, std::move(columns), batch};
}

ColumnarBatchDecodeResult decode_columnar_batch_v1_exact(const common::ByteView bytes,
                                                         const ColumnarBatchDecodeLimits limits) {
  ColumnarBatchDecodeResult decoded = decode_columnar_batch_v1_prefix(bytes, limits);
  if (!decoded.has_value()) {
    return decoded;
  }
  if (decoded->encoded_bytes().size() != bytes.size()) {
    return std::unexpected(invalid("exact columnar batch input contains trailing bytes"));
  }
  return decoded;
}

common::Status validate_columnar_batch_schema(const DecodedColumnarBatchView& batch,
                                              const schema::TableSchema& schema) {
  if (batch.table_id() != schema.table_id() || batch.schema_id() != schema.schema_id() ||
      batch.schema_version() != schema.version()) {
    return invalid_argument("columnar batch identity or version does not match the schema");
  }
  if (batch.columns().size() != schema.columns().size()) {
    return invalid_argument("columnar batch column count does not match the schema");
  }
  for (std::size_t ordinal = 0U; ordinal < batch.columns().size(); ++ordinal) {
    const ColumnVectorView& column = batch.columns()[ordinal];
    const schema::ColumnDefinition& definition = schema.columns()[ordinal];
    if (column.column_id() != definition.id() || column.type() != definition.type() ||
        column.nullable() != definition.nullable()) {
      return invalid_argument("columnar batch column identity, type, parameters, or nullability "
                              "does not match schema ordinal");
    }
  }
  return common::Status::ok();
}

} // namespace chronos::columnar
