#include "sort_order_internal.hpp"

#include "chronos/common/status.hpp"

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <string>

namespace chronos::cseg::detail {
namespace {

[[nodiscard]] common::Status malformed(const char* message) {
  return common::Status{common::StatusCode::kCorruption, std::string{message}};
}

template <typename Unsigned>
[[nodiscard]] common::Result<Unsigned> load_little_endian(const common::ByteView bytes) {
  if (bytes.size() != sizeof(Unsigned)) {
    return common::make_unexpected(malformed("CSEG sort cell has an invalid fixed width"));
  }
  Unsigned value = 0U;
  for (std::size_t index = 0U; index < bytes.size(); ++index) {
    const Unsigned byte = static_cast<Unsigned>(std::to_integer<std::uint8_t>(bytes[index]));
    const Unsigned shifted = static_cast<Unsigned>(byte << (index * 8U));
    value = static_cast<Unsigned>(value | shifted);
  }
  return value;
}

[[nodiscard]] int compare_bytes(const common::ByteView left,
                                const common::ByteView right) noexcept {
  for (std::size_t index = 0U; index < std::min(left.size(), right.size()); ++index) {
    const std::uint8_t lhs = std::to_integer<std::uint8_t>(left[index]);
    const std::uint8_t rhs = std::to_integer<std::uint8_t>(right[index]);
    if (lhs != rhs) {
      return lhs < rhs ? -1 : 1;
    }
  }
  return left.size() == right.size() ? 0 : (left.size() < right.size() ? -1 : 1);
}

template <typename Unsigned>
[[nodiscard]] common::Result<int> compare_unsigned(const SortCellView left,
                                                   const SortCellView right) {
  const common::Result<Unsigned> lhs = load_little_endian<Unsigned>(left.bytes);
  const common::Result<Unsigned> rhs = load_little_endian<Unsigned>(right.bytes);
  if (!lhs.has_value()) {
    return common::make_unexpected(lhs.error());
  }
  if (!rhs.has_value()) {
    return common::make_unexpected(rhs.error());
  }
  return *lhs == *rhs ? 0 : (*lhs < *rhs ? -1 : 1);
}

template <typename Signed, typename Unsigned>
[[nodiscard]] common::Result<int> compare_signed(const SortCellView left,
                                                 const SortCellView right) {
  const common::Result<Unsigned> lhs = load_little_endian<Unsigned>(left.bytes);
  const common::Result<Unsigned> rhs = load_little_endian<Unsigned>(right.bytes);
  if (!lhs.has_value()) {
    return common::make_unexpected(lhs.error());
  }
  if (!rhs.has_value()) {
    return common::make_unexpected(rhs.error());
  }
  const Signed lhs_value = std::bit_cast<Signed>(*lhs);
  const Signed rhs_value = std::bit_cast<Signed>(*rhs);
  return lhs_value == rhs_value ? 0 : (lhs_value < rhs_value ? -1 : 1);
}

template <typename Float, typename Unsigned>
[[nodiscard]] common::Result<int> compare_float(const SortCellView left, const SortCellView right) {
  const common::Result<Unsigned> lhs = load_little_endian<Unsigned>(left.bytes);
  const common::Result<Unsigned> rhs = load_little_endian<Unsigned>(right.bytes);
  if (!lhs.has_value()) {
    return common::make_unexpected(lhs.error());
  }
  if (!rhs.has_value()) {
    return common::make_unexpected(rhs.error());
  }
  const Float lhs_value = std::bit_cast<Float>(*lhs);
  const Float rhs_value = std::bit_cast<Float>(*rhs);
  const bool lhs_nan = std::isnan(lhs_value);
  const bool rhs_nan = std::isnan(rhs_value);
  if (lhs_nan || rhs_nan) {
    return lhs_nan == rhs_nan ? 0 : (lhs_nan ? 1 : -1);
  }
  return lhs_value == rhs_value ? 0 : (lhs_value < rhs_value ? -1 : 1);
}

[[nodiscard]] common::Result<int> compare_decimal(const SortCellView left,
                                                  const SortCellView right) {
  if (left.bytes.size() != 16U || right.bytes.size() != 16U) {
    return common::make_unexpected(malformed("CSEG DECIMAL sort cell has an invalid width"));
  }
  const bool lhs_negative = (std::to_integer<std::uint8_t>(left.bytes.back()) & 0x80U) != 0U;
  const bool rhs_negative = (std::to_integer<std::uint8_t>(right.bytes.back()) & 0x80U) != 0U;
  if (lhs_negative != rhs_negative) {
    return lhs_negative ? -1 : 1;
  }
  for (std::size_t index = 16U; index > 0U; --index) {
    const std::uint8_t lhs = std::to_integer<std::uint8_t>(left.bytes[index - 1U]);
    const std::uint8_t rhs = std::to_integer<std::uint8_t>(right.bytes[index - 1U]);
    if (lhs != rhs) {
      return lhs < rhs ? -1 : 1;
    }
  }
  return 0;
}

} // namespace

common::Result<int> compare_sort_cells(const schema::LogicalTypeKind kind, const SortCellView left,
                                       const SortCellView right) {
  if (left.is_null || right.is_null) {
    return left.is_null == right.is_null ? 0 : (left.is_null ? 1 : -1);
  }
  if (kind == schema::LogicalTypeKind::kBool) {
    if (!left.is_boolean || !right.is_boolean) {
      return common::make_unexpected(malformed("CSEG BOOL sort cell kind is invalid"));
    }
    return left.boolean == right.boolean ? 0 : (left.boolean ? 1 : -1);
  }
  if (left.is_boolean || right.is_boolean) {
    return common::make_unexpected(malformed("CSEG non-BOOL sort cell kind is invalid"));
  }
  using schema::LogicalTypeKind;
  switch (kind) {
  case LogicalTypeKind::kInt8:
    return compare_signed<std::int8_t, std::uint8_t>(left, right);
  case LogicalTypeKind::kInt16:
    return compare_signed<std::int16_t, std::uint16_t>(left, right);
  case LogicalTypeKind::kInt32:
  case LogicalTypeKind::kDate:
    return compare_signed<std::int32_t, std::uint32_t>(left, right);
  case LogicalTypeKind::kInt64:
  case LogicalTypeKind::kTimestampNs:
    return compare_signed<std::int64_t, std::uint64_t>(left, right);
  case LogicalTypeKind::kUInt8:
    return compare_unsigned<std::uint8_t>(left, right);
  case LogicalTypeKind::kUInt16:
    return compare_unsigned<std::uint16_t>(left, right);
  case LogicalTypeKind::kUInt32:
    return compare_unsigned<std::uint32_t>(left, right);
  case LogicalTypeKind::kUInt64:
    return compare_unsigned<std::uint64_t>(left, right);
  case LogicalTypeKind::kFloat32:
    return compare_float<float, std::uint32_t>(left, right);
  case LogicalTypeKind::kFloat64:
    return compare_float<double, std::uint64_t>(left, right);
  case LogicalTypeKind::kDecimal:
    return compare_decimal(left, right);
  case LogicalTypeKind::kSymbol:
  case LogicalTypeKind::kString:
  case LogicalTypeKind::kBinary:
  case LogicalTypeKind::kUuid:
    return compare_bytes(left.bytes, right.bytes);
  case LogicalTypeKind::kBool:
    break;
  }
  return common::make_unexpected(malformed("CSEG sort logical type is invalid"));
}

} // namespace chronos::cseg::detail
