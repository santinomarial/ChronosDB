#include "chronos/columnar/column_vector.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/schema/utf8.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <utility>

namespace chronos::columnar {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] bool bit_at(const common::ByteView bytes, const std::uint32_t row) noexcept {
  const std::size_t index = static_cast<std::size_t>(row) / 8U;
  const auto mask = static_cast<std::uint8_t>(1U << (row % 8U));
  return (std::to_integer<std::uint8_t>(bytes[index]) & mask) != 0U;
}

[[nodiscard]] std::uint32_t read_u32_le(const common::ByteView bytes,
                                        const std::size_t offset) noexcept {
  std::uint32_t value = 0;
  for (std::size_t index = 0; index < sizeof(value); ++index) {
    value |= std::to_integer<std::uint32_t>(bytes[offset + index]) << (index * 8U);
  }
  return value;
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

[[nodiscard]] common::Status validate_unused_bits(const common::ByteView bitmap,
                                                  const std::uint32_t row_count,
                                                  const std::string_view label) {
  const std::uint32_t used_bits = row_count % 8U;
  if (used_bits == 0U) {
    return common::Status::ok();
  }
  const auto used_mask = static_cast<std::uint8_t>((1U << used_bits) - 1U);
  const auto last = std::to_integer<std::uint8_t>(bitmap.back());
  if ((last & static_cast<std::uint8_t>(~used_mask)) != 0U) {
    return invalid(std::string{label} + " has nonzero unused high bits");
  }
  return common::Status::ok();
}

[[nodiscard]] bool is_zero(const common::ByteView bytes) noexcept {
  return std::ranges::all_of(bytes, [](const std::byte value) { return value == std::byte{0}; });
}

[[nodiscard]] common::Status validate_buffer_accounting(const ColumnVectorBufferView buffers) {
  const auto validity_and_offsets =
      common::checked_add(buffers.validity.size(), buffers.offsets.size());
  if (!validity_and_offsets.has_value() ||
      !common::checked_add(*validity_and_offsets, buffers.values.size()).has_value()) {
    return invalid("column vector buffer-byte accounting overflows this platform");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status validate_retained_accounting(const ColumnVectorBuffers& buffers) {
  const auto validity_and_offsets =
      common::checked_add(buffers.validity.capacity(), buffers.offsets.capacity());
  if (!validity_and_offsets.has_value() ||
      !common::checked_add(*validity_and_offsets, buffers.values.capacity()).has_value()) {
    return invalid("owned column vector retained-byte accounting overflows this platform");
  }
  return common::Status::ok();
}

[[nodiscard]] bool decimal_in_domain(const common::ByteView bytes,
                                     const std::uint16_t precision) noexcept {
  std::array<std::uint8_t, 16> magnitude{};
  const bool negative = (std::to_integer<std::uint8_t>(bytes[15]) & 0x80U) != 0U;
  std::uint16_t carry = negative ? 1U : 0U;
  for (std::size_t index = 0; index < magnitude.size(); ++index) {
    std::uint16_t value = std::to_integer<std::uint8_t>(bytes[index]);
    if (negative) {
      value = static_cast<std::uint16_t>(~value) & 0xffU;
      value = static_cast<std::uint16_t>(value + carry);
      carry = static_cast<std::uint16_t>(value >> 8U);
    }
    magnitude[index] = static_cast<std::uint8_t>(value & 0xffU);
  }

  std::array<std::uint8_t, 16> limit{};
  limit[0] = 1U;
  for (std::uint16_t digit = 0; digit < precision; ++digit) {
    carry = 0U;
    for (std::uint8_t& limb : limit) {
      const auto product = static_cast<std::uint16_t>(limb * 10U + carry);
      limb = static_cast<std::uint8_t>(product & 0xffU);
      carry = static_cast<std::uint16_t>(product >> 8U);
    }
  }

  for (std::size_t index = magnitude.size(); index > 0U; --index) {
    if (magnitude[index - 1U] != limit[index - 1U]) {
      return magnitude[index - 1U] < limit[index - 1U];
    }
  }
  return false;
}

[[nodiscard]] common::Status validate_validity(const PhysicalColumnMetadata metadata,
                                               const ColumnVectorBufferView buffers) {
  if (!metadata.nullable) {
    if (!buffers.validity.empty() || metadata.null_count != 0U) {
      return invalid("non-nullable column must have no validity bytes and zero null count");
    }
    return common::Status::ok();
  }
  if (buffers.validity.size() != bitmap_size(metadata.row_count)) {
    return invalid("nullable column validity length is not ceil(row_count / 8)");
  }
  common::Status unused =
      validate_unused_bits(buffers.validity, metadata.row_count, "validity bitmap");
  if (!unused.is_ok()) {
    return unused;
  }
  std::uint32_t observed_nulls = 0U;
  for (std::uint32_t row = 0; row < metadata.row_count; ++row) {
    if (!bit_at(buffers.validity, row)) {
      ++observed_nulls;
    }
  }
  if (observed_nulls != metadata.null_count) {
    return invalid("validity bitmap does not match the declared null count");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status validate_variable(const PhysicalColumnMetadata metadata,
                                               const ColumnVectorBufferView buffers) {
  const auto offset_count =
      common::checked_add<std::size_t>(static_cast<std::size_t>(metadata.row_count), 1U);
  if (!offset_count.has_value()) {
    return invalid("variable offset count overflows this platform");
  }
  const auto offset_bytes = common::checked_multiply(*offset_count, sizeof(std::uint32_t));
  if (!offset_bytes.has_value() || buffers.offsets.size() != *offset_bytes) {
    return invalid("variable offsets length is not (row_count + 1) * 4");
  }
  if (buffers.values.size() > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
    return invalid("variable values exceed the UINT32 offset domain");
  }
  if (read_u32_le(buffers.offsets, 0U) != 0U) {
    return invalid("first variable offset must be zero");
  }

  std::uint32_t previous = 0U;
  for (std::uint32_t row = 0; row < metadata.row_count; ++row) {
    const std::size_t next_offset = (static_cast<std::size_t>(row) + 1U) * sizeof(std::uint32_t);
    const std::uint32_t next = read_u32_le(buffers.offsets, next_offset);
    if (next < previous || static_cast<std::size_t>(next) > buffers.values.size()) {
      return invalid("variable offsets must be nondecreasing and within the values buffer");
    }
    const bool row_null = metadata.nullable && !bit_at(buffers.validity, row);
    if (row_null && next != previous) {
      return invalid("null variable-width row must have equal adjacent offsets");
    }
    if (!row_null && (metadata.type.kind() == schema::LogicalTypeKind::kString ||
                      metadata.type.kind() == schema::LogicalTypeKind::kSymbol)) {
      const common::ByteView text = buffers.values.subspan(
          static_cast<std::size_t>(previous), static_cast<std::size_t>(next - previous));
      if (!schema::is_valid_utf8(text)) {
        return invalid("STRING and SYMBOL rows must contain valid UTF-8");
      }
    }
    previous = next;
  }
  if (static_cast<std::size_t>(previous) != buffers.values.size()) {
    return invalid("last variable offset must equal values length");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status validate_fixed(const PhysicalColumnMetadata metadata,
                                            const ColumnVectorBufferView buffers) {
  if (!buffers.offsets.empty()) {
    return invalid("fixed-width column must not have offsets");
  }
  if (metadata.type.kind() == schema::LogicalTypeKind::kBool) {
    if (buffers.values.size() != bitmap_size(metadata.row_count)) {
      return invalid("BOOL values length is not ceil(row_count / 8)");
    }
    common::Status unused = validate_unused_bits(buffers.values, metadata.row_count, "BOOL bitmap");
    if (!unused.is_ok()) {
      return unused;
    }
    for (std::uint32_t row = 0; row < metadata.row_count; ++row) {
      if (metadata.nullable && !bit_at(buffers.validity, row) && bit_at(buffers.values, row)) {
        return invalid("null BOOL value bit must be zero");
      }
    }
    return common::Status::ok();
  }

  const std::size_t width = fixed_width(metadata.type.kind());
  const auto required =
      common::checked_multiply(static_cast<std::size_t>(metadata.row_count), width);
  if (!required.has_value() || buffers.values.size() != *required) {
    return invalid("fixed-width values length does not match row_count and logical type");
  }
  for (std::uint32_t row = 0; row < metadata.row_count; ++row) {
    const common::ByteView slot =
        buffers.values.subspan(static_cast<std::size_t>(row) * width, width);
    const bool row_null = metadata.nullable && !bit_at(buffers.validity, row);
    if (row_null && !is_zero(slot)) {
      return invalid("null fixed-width value slot must be all zero");
    }
    if (!row_null && metadata.type.kind() == schema::LogicalTypeKind::kDecimal &&
        !decimal_in_domain(slot, metadata.type.parameter_0())) {
      return invalid("DECIMAL value exceeds its declared precision");
    }
  }
  return common::Status::ok();
}

} // namespace

common::Result<bool> ColumnCellView::boolean() const {
  if (kind_ != Kind::kBoolean) {
    return common::make_unexpected(invalid("cell does not contain a Boolean value"));
  }
  return boolean_;
}

common::Result<common::ByteView> ColumnCellView::bytes() const {
  if (kind_ != Kind::kBytes) {
    return common::make_unexpected(invalid("cell does not contain a byte value"));
  }
  return bytes_;
}

common::Result<PhysicalColumnView>
PhysicalColumnView::create(const PhysicalColumnMetadata metadata,
                           const ColumnVectorBufferView buffers) {
  if (metadata.row_count == 0U) {
    return common::make_unexpected(invalid("physical column row count must be nonzero"));
  }
  common::Status status = validate_buffer_accounting(buffers);
  if (!status.is_ok()) {
    return common::make_unexpected(std::move(status));
  }
  status = validate_validity(metadata, buffers);
  if (!status.is_ok()) {
    return common::make_unexpected(std::move(status));
  }
  status = metadata.type.is_variable_width() ? validate_variable(metadata, buffers)
                                             : validate_fixed(metadata, buffers);
  if (!status.is_ok()) {
    return common::make_unexpected(std::move(status));
  }
  return PhysicalColumnView{metadata, buffers};
}

std::size_t PhysicalColumnView::buffer_bytes() const noexcept {
  return validity_.size() + offsets_.size() + values_.size();
}

common::Result<bool> PhysicalColumnView::is_null(const std::uint32_t row) const {
  if (row >= row_count_) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kOutOfRange, "column row is out of range"});
  }
  return nullable_ && !bit_at(validity_, row);
}

common::Result<ColumnCellView> PhysicalColumnView::cell(const std::uint32_t row) const {
  const common::Result<bool> null = is_null(row);
  if (!null.has_value()) {
    return common::make_unexpected(null.error());
  }
  if (*null) {
    return ColumnCellView::null();
  }
  if (type_.kind() == schema::LogicalTypeKind::kBool) {
    return ColumnCellView::boolean(bit_at(values_, row));
  }
  if (type_.is_variable_width()) {
    const std::size_t offset = static_cast<std::size_t>(row) * sizeof(std::uint32_t);
    const std::uint32_t begin = read_u32_le(offsets_, offset);
    const std::uint32_t end = read_u32_le(offsets_, offset + sizeof(std::uint32_t));
    return ColumnCellView::bytes(
        values_.subspan(static_cast<std::size_t>(begin), static_cast<std::size_t>(end - begin)));
  }
  const std::size_t width = fixed_width(type_.kind());
  return ColumnCellView::bytes(values_.subspan(static_cast<std::size_t>(row) * width, width));
}

common::Result<ColumnVectorView> ColumnVectorView::create(const ColumnVectorMetadata metadata,
                                                          const ColumnVectorBufferView buffers) {
  const common::Result<PhysicalColumnView> physical =
      PhysicalColumnView::create({.type = metadata.type,
                                  .nullable = metadata.nullable,
                                  .row_count = metadata.row_count,
                                  .null_count = metadata.null_count},
                                 buffers);
  if (!physical.has_value()) {
    return common::make_unexpected(physical.error());
  }
  return ColumnVectorView{metadata, buffers};
}

std::size_t ColumnVectorView::buffer_bytes() const noexcept {
  return physical_.buffer_bytes();
}

common::Result<bool> ColumnVectorView::is_null(const std::uint32_t row) const {
  return physical_.is_null(row);
}

common::Result<ColumnCellView> ColumnVectorView::cell(const std::uint32_t row) const {
  return physical_.cell(row);
}

OwnedColumnVector::OwnedColumnVector(const ColumnVectorMetadata metadata,
                                     ColumnVectorBuffers buffers) noexcept
    : column_id_(metadata.column_id), type_(metadata.type), nullable_(metadata.nullable),
      row_count_(metadata.row_count), null_count_(metadata.null_count),
      buffers_(std::move(buffers)) {}

common::Result<OwnedColumnVector> OwnedColumnVector::create(const ColumnVectorMetadata metadata,
                                                            ColumnVectorBuffers buffers) {
  const common::Status retained = validate_retained_accounting(buffers);
  if (!retained.is_ok()) {
    return common::make_unexpected(retained);
  }
  const common::Result<ColumnVectorView> validated =
      ColumnVectorView::create(metadata, ColumnVectorBufferView{.validity = buffers.validity,
                                                                .offsets = buffers.offsets,
                                                                .values = buffers.values});
  if (!validated.has_value()) {
    return common::make_unexpected(validated.error());
  }
  return OwnedColumnVector{metadata, std::move(buffers)};
}

ColumnVectorView OwnedColumnVector::view() const noexcept {
  return ColumnVectorView{ColumnVectorMetadata{.column_id = column_id_,
                                               .type = type_,
                                               .nullable = nullable_,
                                               .row_count = row_count_,
                                               .null_count = null_count_},
                          ColumnVectorBufferView{.validity = buffers_.validity,
                                                 .offsets = buffers_.offsets,
                                                 .values = buffers_.values}};
}

const schema::ColumnId& OwnedColumnVector::column_id() const noexcept {
  return column_id_;
}

const schema::LogicalType& OwnedColumnVector::type() const noexcept {
  return type_;
}

bool OwnedColumnVector::nullable() const noexcept {
  return nullable_;
}

std::uint32_t OwnedColumnVector::row_count() const noexcept {
  return row_count_;
}

std::uint32_t OwnedColumnVector::null_count() const noexcept {
  return null_count_;
}

std::size_t OwnedColumnVector::buffer_bytes() const noexcept {
  return view().buffer_bytes();
}

std::size_t OwnedColumnVector::retained_buffer_bytes() const noexcept {
  return buffers_.validity.capacity() + buffers_.offsets.capacity() + buffers_.values.capacity();
}

common::Result<bool> OwnedColumnVector::is_null(const std::uint32_t row) const {
  return view().is_null(row);
}

common::Result<ColumnCellView> OwnedColumnVector::cell(const std::uint32_t row) const {
  return view().cell(row);
}

} // namespace chronos::columnar
