#include "chronos/query/value.hpp"

#include "chronos/common/status.hpp"
#include "chronos/schema/utf8.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] common::Status invalid(const std::string_view message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::string{message}};
}

[[nodiscard]] bool signed_kind(const schema::LogicalTypeKind kind) noexcept {
  return (kind >= schema::LogicalTypeKind::kInt8 && kind <= schema::LogicalTypeKind::kInt64) ||
         kind == schema::LogicalTypeKind::kTimestampNs || kind == schema::LogicalTypeKind::kDate;
}

[[nodiscard]] bool unsigned_kind(const schema::LogicalTypeKind kind) noexcept {
  return kind >= schema::LogicalTypeKind::kUInt8 && kind <= schema::LogicalTypeKind::kUInt64;
}

[[nodiscard]] bool signed_integer_kind(const schema::LogicalTypeKind kind) noexcept {
  return kind >= schema::LogicalTypeKind::kInt8 && kind <= schema::LogicalTypeKind::kInt64;
}

[[nodiscard]] bool signed_in_domain(const schema::LogicalTypeKind kind,
                                    const std::int64_t value) noexcept {
  switch (kind) {
  case schema::LogicalTypeKind::kInt8:
    return value >= std::numeric_limits<std::int8_t>::min() &&
           value <= std::numeric_limits<std::int8_t>::max();
  case schema::LogicalTypeKind::kInt16:
    return value >= std::numeric_limits<std::int16_t>::min() &&
           value <= std::numeric_limits<std::int16_t>::max();
  case schema::LogicalTypeKind::kInt32:
  case schema::LogicalTypeKind::kDate:
    return value >= std::numeric_limits<std::int32_t>::min() &&
           value <= std::numeric_limits<std::int32_t>::max();
  case schema::LogicalTypeKind::kInt64:
  case schema::LogicalTypeKind::kTimestampNs:
    return true;
  default:
    return false;
  }
}

[[nodiscard]] bool unsigned_in_domain(const schema::LogicalTypeKind kind,
                                      const std::uint64_t value) noexcept {
  switch (kind) {
  case schema::LogicalTypeKind::kUInt8:
    return value <= std::numeric_limits<std::uint8_t>::max();
  case schema::LogicalTypeKind::kUInt16:
    return value <= std::numeric_limits<std::uint16_t>::max();
  case schema::LogicalTypeKind::kUInt32:
    return value <= std::numeric_limits<std::uint32_t>::max();
  case schema::LogicalTypeKind::kUInt64:
    return true;
  default:
    return false;
  }
}

[[nodiscard]] bool decimal_in_domain(const Decimal128Value& value,
                                     const std::uint16_t precision) noexcept {
  std::array<std::uint8_t, 16> magnitude{};
  const bool negative = (std::to_integer<std::uint8_t>(value.coefficient.back()) & 0x80U) != 0U;
  std::uint16_t carry = negative ? 1U : 0U;
  for (std::size_t index = 0U; index < magnitude.size(); ++index) {
    std::uint16_t limb = std::to_integer<std::uint8_t>(value.coefficient[index]);
    if (negative) {
      limb = static_cast<std::uint16_t>(~limb) & 0xffU;
      limb = static_cast<std::uint16_t>(limb + carry);
      carry = static_cast<std::uint16_t>(limb >> 8U);
    }
    magnitude[index] = static_cast<std::uint8_t>(limb & 0xffU);
  }
  std::array<std::uint8_t, 16> limit{};
  limit.front() = 1U;
  for (std::uint16_t digit = 0U; digit < precision; ++digit) {
    carry = 0U;
    for (std::uint8_t& limb : limit) {
      const std::uint16_t product = static_cast<std::uint16_t>(limb * 10U + carry);
      limb = static_cast<std::uint8_t>(product & 0xffU);
      carry = static_cast<std::uint16_t>(product >> 8U);
    }
  }
  for (std::size_t index = magnitude.size(); index > 0U; --index) {
    if (magnitude[index - 1U] != limit[index - 1U])
      return magnitude[index - 1U] < limit[index - 1U];
  }
  return false;
}

template <typename Unsigned>
[[nodiscard]] common::Result<Unsigned> load_little_endian(const common::ByteView bytes) {
  if (bytes.size() != sizeof(Unsigned))
    return common::make_unexpected(invalid("physical scalar has an invalid fixed width"));
  Unsigned value = 0U;
  for (std::size_t index = 0U; index < bytes.size(); ++index) {
    const Unsigned byte = static_cast<Unsigned>(std::to_integer<std::uint8_t>(bytes[index]));
    const Unsigned shifted = static_cast<Unsigned>(byte << (index * 8U));
    value = static_cast<Unsigned>(value | shifted);
  }
  return value;
}

template <typename Signed, typename Unsigned>
[[nodiscard]] common::Result<std::int64_t> load_signed(const common::ByteView bytes) {
  const common::Result<Unsigned> bits = load_little_endian<Unsigned>(bytes);
  if (!bits.has_value())
    return common::make_unexpected(bits.error());
  return static_cast<std::int64_t>(std::bit_cast<Signed>(*bits));
}

[[nodiscard]] int compare_bytes(const std::span<const std::byte> left,
                                const std::span<const std::byte> right) noexcept {
  const auto comparison = std::lexicographical_compare_three_way(
      left.begin(), left.end(), right.begin(), right.end(),
      [](const std::byte lhs, const std::byte rhs) {
        return std::to_integer<std::uint8_t>(lhs) <=> std::to_integer<std::uint8_t>(rhs);
      });
  return comparison < 0 ? -1 : (comparison > 0 ? 1 : 0);
}

[[nodiscard]] int compare_decimal(const Decimal128Value& left,
                                  const Decimal128Value& right) noexcept {
  const bool left_negative = (std::to_integer<std::uint8_t>(left.coefficient.back()) & 0x80U) != 0U;
  const bool right_negative =
      (std::to_integer<std::uint8_t>(right.coefficient.back()) & 0x80U) != 0U;
  if (left_negative != right_negative)
    return left_negative ? -1 : 1;
  for (std::size_t index = left.coefficient.size(); index > 0U; --index) {
    const std::uint8_t lhs = std::to_integer<std::uint8_t>(left.coefficient[index - 1U]);
    const std::uint8_t rhs = std::to_integer<std::uint8_t>(right.coefficient[index - 1U]);
    if (lhs != rhs)
      return lhs < rhs ? -1 : 1;
  }
  return 0;
}

template <typename Float> [[nodiscard]] int compare_float(const Float left, const Float right) {
  const bool left_nan = std::isnan(left);
  const bool right_nan = std::isnan(right);
  if (left_nan || right_nan)
    return left_nan == right_nan ? 0 : (left_nan ? 1 : -1);
  return left == right ? 0 : (left < right ? -1 : 1);
}

[[nodiscard]] const schema::LogicalType* value_type(const ScalarValue& value) noexcept {
  const std::optional<schema::LogicalType>& logical_type = value.type();
  if (!logical_type.has_value())
    return nullptr;
  return std::addressof(*logical_type);
}

[[nodiscard]] bool compatible_types(const schema::LogicalType& left,
                                    const schema::LogicalType& right) noexcept {
  if (left == right)
    return true;
  return (signed_integer_kind(left.kind()) && signed_integer_kind(right.kind())) ||
         (unsigned_kind(left.kind()) && unsigned_kind(right.kind())) ||
         ((left.kind() == schema::LogicalTypeKind::kFloat32 ||
           left.kind() == schema::LogicalTypeKind::kFloat64) &&
          (right.kind() == schema::LogicalTypeKind::kFloat32 ||
           right.kind() == schema::LogicalTypeKind::kFloat64));
}

} // namespace

ScalarValue::ScalarValue(std::optional<schema::LogicalType> type, ScalarStorage storage) noexcept
    : type_(type), storage_(std::move(storage)) {}

ScalarValue ScalarValue::untyped_null() noexcept {
  return ScalarValue{std::nullopt, std::monostate{}};
}

ScalarValue ScalarValue::null(const schema::LogicalType type) noexcept {
  return ScalarValue{type, std::monostate{}};
}

common::Result<ScalarValue> ScalarValue::boolean(const bool value) {
  return ScalarValue{schema::LogicalType::create(schema::LogicalTypeKind::kBool).value(), value};
}

common::Result<ScalarValue> ScalarValue::signed_value(const schema::LogicalType type,
                                                      const std::int64_t value) {
  if (!signed_in_domain(type.kind(), value))
    return common::make_unexpected(invalid("signed scalar is outside its logical type"));
  return ScalarValue{type, value};
}

common::Result<ScalarValue> ScalarValue::unsigned_value(const schema::LogicalType type,
                                                        const std::uint64_t value) {
  if (!unsigned_in_domain(type.kind(), value))
    return common::make_unexpected(invalid("unsigned scalar is outside its logical type"));
  return ScalarValue{type, value};
}

common::Result<ScalarValue> ScalarValue::float32(const float value) {
  return ScalarValue{schema::LogicalType::create(schema::LogicalTypeKind::kFloat32).value(), value};
}

common::Result<ScalarValue> ScalarValue::float64(const double value) {
  return ScalarValue{schema::LogicalType::create(schema::LogicalTypeKind::kFloat64).value(), value};
}

common::Result<ScalarValue> ScalarValue::decimal(const schema::LogicalType type,
                                                 Decimal128Value value) {
  if (!type.is_decimal() || !decimal_in_domain(value, type.parameter_0()))
    return common::make_unexpected(invalid("decimal scalar exceeds its declared precision"));
  return ScalarValue{type, value};
}

common::Result<ScalarValue> ScalarValue::text(const schema::LogicalType type, std::string value) {
  if ((type.kind() != schema::LogicalTypeKind::kString &&
       type.kind() != schema::LogicalTypeKind::kSymbol) ||
      !schema::is_valid_utf8(value))
    return common::make_unexpected(invalid("text scalar type or UTF-8 is invalid"));
  return ScalarValue{type, std::move(value)};
}

ScalarValue ScalarValue::binary(std::vector<std::byte> value) {
  return ScalarValue{schema::LogicalType::create(schema::LogicalTypeKind::kBinary).value(),
                     std::move(value)};
}

ScalarValue ScalarValue::uuid(const common::Uuid value) {
  return ScalarValue{schema::LogicalType::create(schema::LogicalTypeKind::kUuid).value(), value};
}

common::Result<ScalarValue> ScalarValue::from_column_cell(const schema::LogicalType type,
                                                          const columnar::ColumnCellView& cell) {
  if (cell.is_null())
    return ScalarValue::null(type);
  using schema::LogicalTypeKind;
  if (type.kind() == LogicalTypeKind::kBool) {
    const common::Result<bool> value = cell.boolean();
    if (!value.has_value())
      return common::make_unexpected(value.error());
    return boolean(*value);
  }
  const common::Result<common::ByteView> bytes_result = cell.bytes();
  if (!bytes_result.has_value())
    return common::make_unexpected(bytes_result.error());
  const common::ByteView bytes = *bytes_result;
  switch (type.kind()) {
  case LogicalTypeKind::kInt8: {
    const auto value = load_signed<std::int8_t, std::uint8_t>(bytes);
    return value.has_value() ? signed_value(type, *value) : common::make_unexpected(value.error());
  }
  case LogicalTypeKind::kInt16: {
    const auto value = load_signed<std::int16_t, std::uint16_t>(bytes);
    return value.has_value() ? signed_value(type, *value) : common::make_unexpected(value.error());
  }
  case LogicalTypeKind::kInt32:
  case LogicalTypeKind::kDate: {
    const auto value = load_signed<std::int32_t, std::uint32_t>(bytes);
    return value.has_value() ? signed_value(type, *value) : common::make_unexpected(value.error());
  }
  case LogicalTypeKind::kInt64:
  case LogicalTypeKind::kTimestampNs: {
    const auto value = load_signed<std::int64_t, std::uint64_t>(bytes);
    return value.has_value() ? signed_value(type, *value) : common::make_unexpected(value.error());
  }
  case LogicalTypeKind::kUInt8:
  case LogicalTypeKind::kUInt16:
  case LogicalTypeKind::kUInt32:
  case LogicalTypeKind::kUInt64: {
    common::Result<std::uint64_t> value = common::make_unexpected(invalid("invalid integer kind"));
    if (type.kind() == LogicalTypeKind::kUInt8)
      value = load_little_endian<std::uint8_t>(bytes);
    else if (type.kind() == LogicalTypeKind::kUInt16)
      value = load_little_endian<std::uint16_t>(bytes);
    else if (type.kind() == LogicalTypeKind::kUInt32)
      value = load_little_endian<std::uint32_t>(bytes);
    else
      value = load_little_endian<std::uint64_t>(bytes);
    return value.has_value() ? unsigned_value(type, *value)
                             : common::make_unexpected(value.error());
  }
  case LogicalTypeKind::kFloat32: {
    const auto bits = load_little_endian<std::uint32_t>(bytes);
    return bits.has_value() ? float32(std::bit_cast<float>(*bits))
                            : common::make_unexpected(bits.error());
  }
  case LogicalTypeKind::kFloat64: {
    const auto bits = load_little_endian<std::uint64_t>(bytes);
    return bits.has_value() ? float64(std::bit_cast<double>(*bits))
                            : common::make_unexpected(bits.error());
  }
  case LogicalTypeKind::kDecimal: {
    if (bytes.size() != 16U)
      return common::make_unexpected(invalid("decimal scalar has an invalid fixed width"));
    Decimal128Value value;
    std::ranges::copy(bytes, value.coefficient.begin());
    return decimal(type, value);
  }
  case LogicalTypeKind::kSymbol:
  case LogicalTypeKind::kString: {
    std::string value(bytes.size(), '\0');
    if (!bytes.empty())
      std::memcpy(value.data(), bytes.data(), bytes.size());
    return text(type, std::move(value));
  }
  case LogicalTypeKind::kBinary:
    return binary(std::vector<std::byte>{bytes.begin(), bytes.end()});
  case LogicalTypeKind::kUuid: {
    if (bytes.size() != common::Uuid::kSize)
      return common::make_unexpected(invalid("UUID scalar has an invalid fixed width"));
    common::Uuid::Bytes value{};
    std::ranges::copy(bytes, value.begin());
    return uuid(common::Uuid{value});
  }
  case LogicalTypeKind::kBool:
    break;
  }
  return common::make_unexpected(invalid("scalar logical type is invalid"));
}

bool ScalarValue::is_null() const noexcept {
  return std::holds_alternative<std::monostate>(storage_);
}

const std::optional<schema::LogicalType>& ScalarValue::type() const noexcept {
  return type_;
}

const ScalarStorage& ScalarValue::storage() const noexcept {
  return storage_;
}

common::Result<int> compare_scalar_values(const ScalarValue& left, const ScalarValue& right,
                                          const ScalarNullPlacement null_placement) {
  if (left.is_null() || right.is_null()) {
    if (left.is_null() && right.is_null())
      return 0;
    const int null_comparison = null_placement == ScalarNullPlacement::kFirst ? -1 : 1;
    return left.is_null() ? null_comparison : -null_comparison;
  }
  const schema::LogicalType* left_type = value_type(left);
  const schema::LogicalType* right_type = value_type(right);
  if (left_type == nullptr || right_type == nullptr || !compatible_types(*left_type, *right_type))
    return common::make_unexpected(invalid("scalar comparison types are incompatible"));

  if (signed_kind(left_type->kind())) {
    const auto* lhs = std::get_if<std::int64_t>(&left.storage());
    const auto* rhs = std::get_if<std::int64_t>(&right.storage());
    if (lhs == nullptr || rhs == nullptr)
      return common::make_unexpected(invalid("signed scalar storage is invalid"));
    return *lhs == *rhs ? 0 : (*lhs < *rhs ? -1 : 1);
  }
  if (unsigned_kind(left_type->kind())) {
    const auto* lhs = std::get_if<std::uint64_t>(&left.storage());
    const auto* rhs = std::get_if<std::uint64_t>(&right.storage());
    if (lhs == nullptr || rhs == nullptr)
      return common::make_unexpected(invalid("unsigned scalar storage is invalid"));
    return *lhs == *rhs ? 0 : (*lhs < *rhs ? -1 : 1);
  }
  if (left_type->kind() == schema::LogicalTypeKind::kFloat32 ||
      left_type->kind() == schema::LogicalTypeKind::kFloat64) {
    const double lhs = left_type->kind() == schema::LogicalTypeKind::kFloat32
                           ? static_cast<double>(*std::get_if<float>(&left.storage()))
                           : *std::get_if<double>(&left.storage());
    const double rhs = right_type->kind() == schema::LogicalTypeKind::kFloat32
                           ? static_cast<double>(*std::get_if<float>(&right.storage()))
                           : *std::get_if<double>(&right.storage());
    return compare_float(lhs, rhs);
  }
  switch (left_type->kind()) {
  case schema::LogicalTypeKind::kBool: {
    const bool lhs = *std::get_if<bool>(&left.storage());
    const bool rhs = *std::get_if<bool>(&right.storage());
    return lhs == rhs ? 0 : (lhs ? 1 : -1);
  }
  case schema::LogicalTypeKind::kDecimal:
    return compare_decimal(*std::get_if<Decimal128Value>(&left.storage()),
                           *std::get_if<Decimal128Value>(&right.storage()));
  case schema::LogicalTypeKind::kSymbol:
  case schema::LogicalTypeKind::kString: {
    const std::string& lhs = *std::get_if<std::string>(&left.storage());
    const std::string& rhs = *std::get_if<std::string>(&right.storage());
    const int comparison = lhs.compare(rhs);
    return comparison < 0 ? -1 : (comparison > 0 ? 1 : 0);
  }
  case schema::LogicalTypeKind::kBinary: {
    const auto& lhs = *std::get_if<std::vector<std::byte>>(&left.storage());
    const auto& rhs = *std::get_if<std::vector<std::byte>>(&right.storage());
    return compare_bytes(lhs, rhs);
  }
  case schema::LogicalTypeKind::kUuid: {
    const auto& lhs = std::get_if<common::Uuid>(&left.storage())->bytes();
    const auto& rhs = std::get_if<common::Uuid>(&right.storage())->bytes();
    return compare_bytes(lhs, rhs);
  }
  default:
    break;
  }
  return common::make_unexpected(invalid("scalar comparison kind is invalid"));
}

common::Result<int> compare_physical_cells(const schema::LogicalType type,
                                           const columnar::ColumnCellView& left,
                                           const columnar::ColumnCellView& right,
                                           const ScalarNullPlacement null_placement) {
  if (left.is_null() || right.is_null()) {
    if (left.is_null() && right.is_null())
      return 0;
    const int null_comparison = null_placement == ScalarNullPlacement::kFirst ? -1 : 1;
    return left.is_null() ? null_comparison : -null_comparison;
  }
  if (type.is_variable_width()) {
    const common::Result<common::ByteView> left_bytes = left.bytes();
    if (!left_bytes.has_value())
      return common::make_unexpected(left_bytes.error());
    const common::Result<common::ByteView> right_bytes = right.bytes();
    if (!right_bytes.has_value())
      return common::make_unexpected(right_bytes.error());
    return compare_bytes(*left_bytes, *right_bytes);
  }
  const common::Result<ScalarValue> left_value = ScalarValue::from_column_cell(type, left);
  if (!left_value.has_value())
    return common::make_unexpected(left_value.error());
  const common::Result<ScalarValue> right_value = ScalarValue::from_column_cell(type, right);
  if (!right_value.has_value())
    return common::make_unexpected(right_value.error());
  return compare_scalar_values(*left_value, *right_value, null_placement);
}

common::Result<SqlTruthValue> sql_scalar_equal(const ScalarValue& left, const ScalarValue& right) {
  if (left.is_null() || right.is_null())
    return SqlTruthValue::kUnknown;
  const schema::LogicalType* left_type = value_type(left);
  const schema::LogicalType* right_type = value_type(right);
  if (left_type != nullptr && right_type != nullptr &&
      (left_type->kind() == schema::LogicalTypeKind::kFloat32 ||
       left_type->kind() == schema::LogicalTypeKind::kFloat64) &&
      (right_type->kind() == schema::LogicalTypeKind::kFloat32 ||
       right_type->kind() == schema::LogicalTypeKind::kFloat64)) {
    const double lhs = left_type->kind() == schema::LogicalTypeKind::kFloat32
                           ? static_cast<double>(*std::get_if<float>(&left.storage()))
                           : *std::get_if<double>(&left.storage());
    const double rhs = right_type->kind() == schema::LogicalTypeKind::kFloat32
                           ? static_cast<double>(*std::get_if<float>(&right.storage()))
                           : *std::get_if<double>(&right.storage());
    if (std::isnan(lhs) || std::isnan(rhs))
      return SqlTruthValue::kFalse;
  }
  const common::Result<int> comparison =
      compare_scalar_values(left, right, ScalarNullPlacement::kLast);
  if (!comparison.has_value())
    return common::make_unexpected(comparison.error());
  return *comparison == 0 ? SqlTruthValue::kTrue : SqlTruthValue::kFalse;
}

} // namespace chronos::query
