#include "query/decimal_internal.hpp"

#include "chronos/common/status.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <locale>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>

namespace chronos::query::detail {
namespace {

constexpr std::size_t kLimbCount = 8U;

struct BigUnsigned {
  std::array<std::uint32_t, kLimbCount> limbs{};
};

struct SignedMagnitude {
  BigUnsigned magnitude;
  bool negative{};
};

[[nodiscard]] common::Status invalid(const std::string_view message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::string{message}};
}

[[nodiscard]] common::Status out_of_range(const std::string_view message) {
  return common::Status{common::StatusCode::kOutOfRange, std::string{message}};
}

[[nodiscard]] bool zero(const BigUnsigned& value) noexcept {
  for (const std::uint32_t limb : value.limbs) {
    if (limb != 0U)
      return false;
  }
  return true;
}

[[nodiscard]] int compare(const BigUnsigned& left, const BigUnsigned& right) noexcept {
  for (std::size_t index = kLimbCount; index > 0U; --index) {
    if (left.limbs[index - 1U] != right.limbs[index - 1U])
      return left.limbs[index - 1U] < right.limbs[index - 1U] ? -1 : 1;
  }
  return 0;
}

[[nodiscard]] bool add(BigUnsigned& left, const BigUnsigned& right) noexcept {
  std::uint64_t carry = 0U;
  for (std::size_t index = 0U; index < kLimbCount; ++index) {
    const std::uint64_t sum = static_cast<std::uint64_t>(left.limbs[index]) +
                              static_cast<std::uint64_t>(right.limbs[index]) + carry;
    left.limbs[index] = static_cast<std::uint32_t>(sum);
    carry = sum >> 32U;
  }
  return carry == 0U;
}

void subtract(BigUnsigned& left, const BigUnsigned& right) noexcept {
  std::uint64_t borrow = 0U;
  for (std::size_t index = 0U; index < kLimbCount; ++index) {
    const std::uint64_t lhs = left.limbs[index];
    const std::uint64_t rhs = static_cast<std::uint64_t>(right.limbs[index]) + borrow;
    left.limbs[index] = static_cast<std::uint32_t>(lhs - rhs);
    borrow = lhs < rhs ? 1U : 0U;
  }
}

[[nodiscard]] bool multiply_small(BigUnsigned& value, const std::uint32_t factor) noexcept {
  std::uint64_t carry = 0U;
  for (std::uint32_t& limb : value.limbs) {
    const std::uint64_t product = static_cast<std::uint64_t>(limb) * factor + carry;
    limb = static_cast<std::uint32_t>(product);
    carry = product >> 32U;
  }
  return carry == 0U;
}

[[nodiscard]] bool shift_left(BigUnsigned& value, const std::uint16_t count) noexcept {
  for (std::uint16_t shift = 0U; shift < count; ++shift) {
    const bool overflow = (value.limbs.back() & 0x8000'0000U) != 0U;
    std::uint32_t carry = 0U;
    for (std::uint32_t& limb : value.limbs) {
      const std::uint32_t next = limb >> 31U;
      limb = (limb << 1U) | carry;
      carry = next;
    }
    if (overflow)
      return false;
  }
  return true;
}

void shift_right(BigUnsigned& value, const std::uint16_t count) noexcept {
  if (count >= kLimbCount * 32U) {
    value = {};
    return;
  }
  for (std::uint16_t shift = 0U; shift < count; ++shift) {
    std::uint32_t carry = 0U;
    for (std::size_t index = kLimbCount; index > 0U; --index) {
      const std::uint32_t next = value.limbs[index - 1U] & 1U;
      value.limbs[index - 1U] = (value.limbs[index - 1U] >> 1U) | (carry << 31U);
      carry = next;
    }
  }
}

[[nodiscard]] std::uint32_t divide_small(BigUnsigned& value, const std::uint32_t divisor) noexcept {
  std::uint64_t remainder = 0U;
  for (std::size_t index = kLimbCount; index > 0U; --index) {
    const std::uint64_t dividend = (remainder << 32U) | value.limbs[index - 1U];
    value.limbs[index - 1U] = static_cast<std::uint32_t>(dividend / divisor);
    remainder = dividend % divisor;
  }
  return static_cast<std::uint32_t>(remainder);
}

[[nodiscard]] bool multiply_power_ten(BigUnsigned& value, const std::uint16_t power) noexcept {
  for (std::uint16_t index = 0U; index < power; ++index) {
    if (!multiply_small(value, 10U))
      return false;
  }
  return true;
}

void divide_power_ten(BigUnsigned& value, const std::uint16_t power) noexcept {
  for (std::uint16_t index = 0U; index < power; ++index)
    static_cast<void>(divide_small(value, 10U));
}

[[nodiscard]] BigUnsigned multiply(const BigUnsigned& left, const BigUnsigned& right) noexcept {
  BigUnsigned result;
  for (std::size_t left_index = 0U; left_index < kLimbCount; ++left_index) {
    std::uint64_t carry = 0U;
    for (std::size_t right_index = 0U; right_index + left_index < kLimbCount; ++right_index) {
      const std::size_t output_index = left_index + right_index;
      const std::uint64_t product =
          static_cast<std::uint64_t>(left.limbs[left_index]) * right.limbs[right_index] +
          result.limbs[output_index] + carry;
      result.limbs[output_index] = static_cast<std::uint32_t>(product);
      carry = product >> 32U;
    }
  }
  return result;
}

[[nodiscard]] bool bit(const BigUnsigned& value, const std::size_t index) noexcept {
  return ((value.limbs[index / 32U] >> (index % 32U)) & 1U) != 0U;
}

void set_bit(BigUnsigned& value, const std::size_t index) noexcept {
  value.limbs[index / 32U] |= static_cast<std::uint32_t>(1U << (index % 32U));
}

// Numerator and denominator intentionally share one checked representation.
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
[[nodiscard]] BigUnsigned divide(const BigUnsigned& dividend, const BigUnsigned& divisor,
                                 BigUnsigned& remainder) noexcept {
  BigUnsigned quotient;
  remainder = {};
  for (std::size_t index = kLimbCount * 32U; index > 0U; --index) {
    static_cast<void>(shift_left(remainder, 1U));
    if (bit(dividend, index - 1U))
      remainder.limbs.front() |= 1U;
    if (compare(remainder, divisor) >= 0) {
      subtract(remainder, divisor);
      set_bit(quotient, index - 1U);
    }
  }
  return quotient;
}

[[nodiscard]] SignedMagnitude decode(const Decimal128Value& value) noexcept {
  SignedMagnitude decoded;
  decoded.negative = (std::to_integer<std::uint8_t>(value.coefficient.back()) & 0x80U) != 0U;
  std::uint16_t carry = decoded.negative ? 1U : 0U;
  for (std::size_t index = 0U; index < value.coefficient.size(); ++index) {
    std::uint16_t byte = std::to_integer<std::uint8_t>(value.coefficient[index]);
    if (decoded.negative) {
      byte = static_cast<std::uint16_t>(byte ^ 0x00ffU);
      byte = static_cast<std::uint16_t>(byte + carry);
      carry = byte >> 8U;
    }
    decoded.magnitude.limbs[index / 4U] |= static_cast<std::uint32_t>(byte & 0xffU)
                                           << ((index % 4U) * 8U);
  }
  decoded.negative = decoded.negative && !zero(decoded.magnitude);
  return decoded;
}

[[nodiscard]] BigUnsigned precision_limit(const std::uint16_t precision) noexcept {
  BigUnsigned limit;
  limit.limbs.front() = 1U;
  static_cast<void>(multiply_power_ten(limit, precision));
  return limit;
}

[[nodiscard]] common::Result<Decimal128Value> encode(SignedMagnitude value,
                                                     const schema::LogicalType& type) {
  if (!type.is_decimal() || compare(value.magnitude, precision_limit(type.parameter_0())) >= 0)
    return common::make_unexpected(out_of_range("decimal result exceeds its declared precision"));
  Decimal128Value encoded;
  for (std::size_t index = 0U; index < encoded.coefficient.size(); ++index) {
    encoded.coefficient[index] = std::byte{
        static_cast<std::uint8_t>(value.magnitude.limbs[index / 4U] >> ((index % 4U) * 8U))};
  }
  if (value.negative && !zero(value.magnitude)) {
    std::uint16_t carry = 1U;
    for (std::byte& byte : encoded.coefficient) {
      const std::uint16_t inverted =
          static_cast<std::uint16_t>(~std::to_integer<std::uint8_t>(byte)) & 0xffU;
      const std::uint16_t sum = static_cast<std::uint16_t>(inverted + carry);
      byte = std::byte{static_cast<std::uint8_t>(sum)};
      carry = sum >> 8U;
    }
  }
  return encoded;
}

[[nodiscard]] SignedMagnitude signed_add(const SignedMagnitude& left,
                                         const SignedMagnitude& right) noexcept {
  if (left.negative == right.negative) {
    SignedMagnitude result = left;
    static_cast<void>(add(result.magnitude, right.magnitude));
    return result;
  }
  const int order = compare(left.magnitude, right.magnitude);
  if (order == 0)
    return {};
  SignedMagnitude result = order > 0 ? left : right;
  subtract(result.magnitude, order > 0 ? right.magnitude : left.magnitude);
  return result;
}

[[nodiscard]] SignedMagnitude from_unsigned(const std::uint64_t value) noexcept {
  SignedMagnitude result;
  result.magnitude.limbs[0] = static_cast<std::uint32_t>(value);
  result.magnitude.limbs[1] = static_cast<std::uint32_t>(value >> 32U);
  return result;
}

template <typename Float>
[[nodiscard]] common::Result<Decimal128Value>
decimal_from_floating(const Float value, const schema::LogicalType& target) {
  if (!target.is_decimal() || !std::isfinite(value))
    return common::make_unexpected(
        invalid("CAST from floating value requires finite DECIMAL input"));
  using Bits = std::conditional_t<sizeof(Float) == 4U, std::uint32_t, std::uint64_t>;
  constexpr std::size_t kFractionBits = sizeof(Float) == 4U ? 23U : 52U;
  constexpr std::size_t kExponentBits = sizeof(Float) == 4U ? 8U : 11U;
  constexpr int kExponentBias = sizeof(Float) == 4U ? 127 : 1023;
  const Bits bits = std::bit_cast<Bits>(value);
  const Bits fraction_mask = (Bits{1U} << kFractionBits) - 1U;
  const Bits exponent_mask = (Bits{1U} << kExponentBits) - 1U;
  const Bits exponent_bits = (bits >> kFractionBits) & exponent_mask;
  const Bits fraction = bits & fraction_mask;
  if (exponent_bits == 0U && fraction == 0U)
    return encode({}, target);
  const std::uint64_t significand =
      exponent_bits == 0U ? static_cast<std::uint64_t>(fraction)
                          : static_cast<std::uint64_t>(fraction | (Bits{1U} << kFractionBits));
  const int exponent = exponent_bits == 0U ? 1 - kExponentBias - static_cast<int>(kFractionBits)
                                           : static_cast<int>(exponent_bits) - kExponentBias -
                                                 static_cast<int>(kFractionBits);
  SignedMagnitude result = from_unsigned(significand);
  result.negative = (bits >> (kFractionBits + kExponentBits)) != 0U;
  for (std::uint16_t index = 0U; index < target.parameter_1(); ++index) {
    if (!multiply_small(result.magnitude, 5U))
      return common::make_unexpected(out_of_range("floating to DECIMAL cast overflows"));
  }
  const int binary_scale = exponent + static_cast<int>(target.parameter_1());
  if (binary_scale >= 0) {
    if (binary_scale >= static_cast<int>(kLimbCount * 32U) ||
        !shift_left(result.magnitude, static_cast<std::uint16_t>(binary_scale)))
      return common::make_unexpected(out_of_range("floating to DECIMAL cast overflows"));
  } else {
    const int right_shift = -binary_scale;
    shift_right(result.magnitude, static_cast<std::uint16_t>(
                                      std::min(right_shift, static_cast<int>(kLimbCount * 32U))));
  }
  return encode(result, target);
}

[[nodiscard]] std::string decimal_text(const Decimal128Value& value,
                                       const schema::LogicalType& source) {
  SignedMagnitude decoded = decode(value);
  std::string digits;
  do {
    const std::uint32_t digit = divide_small(decoded.magnitude, 10U);
    digits.push_back(static_cast<char>('0' + digit));
  } while (!zero(decoded.magnitude));
  std::ranges::reverse(digits);
  if (source.parameter_1() != 0U) {
    const std::size_t scale = source.parameter_1();
    if (digits.size() <= scale)
      digits.insert(0U, scale + 1U - digits.size(), '0');
    digits.insert(digits.size() - scale, 1U, '.');
  }
  if (decoded.negative)
    digits.insert(digits.begin(), '-');
  return digits;
}

template <typename Float>
[[nodiscard]] common::Result<Float> decimal_to_floating(const Decimal128Value& value,
                                                        const schema::LogicalType& source) {
  const std::string text = decimal_text(value, source);
  std::istringstream stream{text};
  stream.imbue(std::locale::classic());
  Float result{};
  stream >> result;
  if (!stream || stream.peek() != std::char_traits<char>::eof())
    return common::make_unexpected(out_of_range("DECIMAL to floating cast is out of range"));
  return result;
}

[[nodiscard]] SignedMagnitude accumulator_value(const ExactNumericAccumulator& accumulator) {
  return SignedMagnitude{.magnitude = BigUnsigned{.limbs = accumulator.magnitude},
                         .negative = accumulator.negative};
}

[[nodiscard]] common::Result<void> add_to_accumulator(ExactNumericAccumulator& accumulator,
                                                      const SignedMagnitude& addend) {
  SignedMagnitude current = accumulator_value(accumulator);
  if (current.negative == addend.negative) {
    if (!add(current.magnitude, addend.magnitude))
      return common::make_unexpected(out_of_range("exact numeric accumulator overflows"));
  } else {
    current = signed_add(current, addend);
  }
  current.negative = current.negative && !zero(current.magnitude);
  accumulator.magnitude = current.magnitude.limbs;
  accumulator.negative = current.negative;
  return {};
}

} // namespace

common::Result<void> ExactNumericAccumulator::add_signed(const std::int64_t value) {
  const std::uint64_t magnitude_value =
      value < 0 ? static_cast<std::uint64_t>(-(value + 1)) + 1U : static_cast<std::uint64_t>(value);
  SignedMagnitude addend = from_unsigned(magnitude_value);
  addend.negative = value < 0;
  return add_to_accumulator(*this, addend);
}

common::Result<void> ExactNumericAccumulator::add_unsigned(const std::uint64_t value) {
  return add_to_accumulator(*this, from_unsigned(value));
}

common::Result<void> ExactNumericAccumulator::add_decimal(const Decimal128Value& value) {
  return add_to_accumulator(*this, decode(value));
}

common::Result<std::int64_t> ExactNumericAccumulator::signed_result() const {
  const SignedMagnitude value = accumulator_value(*this);
  for (std::size_t index = 2U; index < value.magnitude.limbs.size(); ++index) {
    if (value.magnitude.limbs[index] != 0U)
      return common::make_unexpected(out_of_range("exact sum is outside signed range"));
  }
  const std::uint64_t magnitude_value =
      value.magnitude.limbs[0] | (static_cast<std::uint64_t>(value.magnitude.limbs[1]) << 32U);
  constexpr std::uint64_t kMinimumMagnitude = std::uint64_t{1U} << 63U;
  if ((!value.negative &&
       magnitude_value > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())) ||
      (value.negative && magnitude_value > kMinimumMagnitude))
    return common::make_unexpected(out_of_range("exact sum is outside signed range"));
  if (value.negative && magnitude_value == kMinimumMagnitude)
    return std::numeric_limits<std::int64_t>::min();
  const std::int64_t signed_magnitude = static_cast<std::int64_t>(magnitude_value);
  return value.negative ? -signed_magnitude : signed_magnitude;
}

common::Result<std::uint64_t> ExactNumericAccumulator::unsigned_result() const {
  const SignedMagnitude value = accumulator_value(*this);
  if (value.negative)
    return common::make_unexpected(out_of_range("exact sum is outside unsigned range"));
  for (std::size_t index = 2U; index < value.magnitude.limbs.size(); ++index) {
    if (value.magnitude.limbs[index] != 0U)
      return common::make_unexpected(out_of_range("exact sum is outside unsigned range"));
  }
  return value.magnitude.limbs[0] | (static_cast<std::uint64_t>(value.magnitude.limbs[1]) << 32U);
}

common::Result<Decimal128Value>
ExactNumericAccumulator::decimal_result(const schema::LogicalType& type) const {
  return encode(accumulator_value(*this), type);
}

common::Result<Decimal128Value> evaluate_decimal(const DecimalOperation operation,
                                                 const Decimal128Value& left,
                                                 const Decimal128Value& right,
                                                 const schema::LogicalType& result_type) {
  SignedMagnitude lhs = decode(left);
  SignedMagnitude rhs = decode(right);
  SignedMagnitude result;
  switch (operation) {
  case DecimalOperation::kAdd:
    result = signed_add(lhs, rhs);
    break;
  case DecimalOperation::kSubtract:
    rhs.negative = !rhs.negative && !zero(rhs.magnitude);
    result = signed_add(lhs, rhs);
    break;
  case DecimalOperation::kMultiply:
    result.magnitude = multiply(lhs.magnitude, rhs.magnitude);
    divide_power_ten(result.magnitude, result_type.parameter_1());
    result.negative = lhs.negative != rhs.negative;
    break;
  case DecimalOperation::kDivide: {
    if (zero(rhs.magnitude))
      return common::make_unexpected(invalid("decimal division by zero"));
    if (!multiply_power_ten(lhs.magnitude, result_type.parameter_1()))
      return common::make_unexpected(out_of_range("decimal division intermediate overflows"));
    BigUnsigned remainder;
    result.magnitude = divide(lhs.magnitude, rhs.magnitude, remainder);
    result.negative = lhs.negative != rhs.negative;
    break;
  }
  case DecimalOperation::kRemainder: {
    if (zero(rhs.magnitude))
      return common::make_unexpected(invalid("decimal remainder by zero"));
    BigUnsigned remainder;
    static_cast<void>(divide(lhs.magnitude, rhs.magnitude, remainder));
    result.magnitude = remainder;
    result.negative = lhs.negative;
    break;
  }
  }
  return encode(result, result_type);
}

common::Result<Decimal128Value> negate_decimal(const Decimal128Value& value,
                                               const schema::LogicalType& type) {
  SignedMagnitude result = decode(value);
  result.negative = !result.negative && !zero(result.magnitude);
  return encode(result, type);
}

common::Result<Decimal128Value> absolute_decimal(const Decimal128Value& value,
                                                 const schema::LogicalType& type) {
  SignedMagnitude result = decode(value);
  result.negative = false;
  return encode(result, type);
}

common::Result<Decimal128Value> decimal_from_signed(const std::int64_t value,
                                                    const schema::LogicalType& target) {
  const std::uint64_t magnitude =
      value < 0 ? static_cast<std::uint64_t>(-(value + 1)) + 1U : static_cast<std::uint64_t>(value);
  SignedMagnitude result = from_unsigned(magnitude);
  result.negative = value < 0;
  if (!multiply_power_ten(result.magnitude, target.parameter_1()))
    return common::make_unexpected(out_of_range("signed to DECIMAL cast overflows"));
  return encode(result, target);
}

common::Result<Decimal128Value> decimal_from_unsigned(const std::uint64_t value,
                                                      const schema::LogicalType& target) {
  SignedMagnitude result = from_unsigned(value);
  if (!multiply_power_ten(result.magnitude, target.parameter_1()))
    return common::make_unexpected(out_of_range("unsigned to DECIMAL cast overflows"));
  return encode(result, target);
}

common::Result<Decimal128Value> decimal_from_float(const float value,
                                                   const schema::LogicalType& target) {
  return decimal_from_floating(value, target);
}

common::Result<Decimal128Value> decimal_from_double(const double value,
                                                    const schema::LogicalType& target) {
  return decimal_from_floating(value, target);
}

common::Result<Decimal128Value> rescale_decimal(const Decimal128Value& value,
                                                const schema::LogicalType& source,
                                                const schema::LogicalType& target) {
  SignedMagnitude result = decode(value);
  if (target.parameter_1() > source.parameter_1()) {
    if (!multiply_power_ten(result.magnitude, static_cast<std::uint16_t>(target.parameter_1() -
                                                                         source.parameter_1())))
      return common::make_unexpected(out_of_range("DECIMAL scale expansion overflows"));
  } else {
    divide_power_ten(result.magnitude,
                     static_cast<std::uint16_t>(source.parameter_1() - target.parameter_1()));
  }
  return encode(result, target);
}

common::Result<std::int64_t> decimal_to_signed(const Decimal128Value& value,
                                               const schema::LogicalType& source) {
  SignedMagnitude decoded = decode(value);
  divide_power_ten(decoded.magnitude, source.parameter_1());
  if (decoded.magnitude.limbs[2] != 0U || decoded.magnitude.limbs[3] != 0U ||
      decoded.magnitude.limbs[4] != 0U || decoded.magnitude.limbs[5] != 0U ||
      decoded.magnitude.limbs[6] != 0U || decoded.magnitude.limbs[7] != 0U)
    return common::make_unexpected(out_of_range("DECIMAL to signed cast is out of range"));
  const std::uint64_t magnitude =
      decoded.magnitude.limbs[0] | (static_cast<std::uint64_t>(decoded.magnitude.limbs[1]) << 32U);
  constexpr std::uint64_t kMinimumMagnitude = std::uint64_t{1U} << 63U;
  if ((!decoded.negative &&
       magnitude > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())) ||
      (decoded.negative && magnitude > kMinimumMagnitude))
    return common::make_unexpected(out_of_range("DECIMAL to signed cast is out of range"));
  if (decoded.negative && magnitude == kMinimumMagnitude)
    return std::numeric_limits<std::int64_t>::min();
  const std::int64_t signed_magnitude = static_cast<std::int64_t>(magnitude);
  return decoded.negative ? -signed_magnitude : signed_magnitude;
}

common::Result<std::uint64_t> decimal_to_unsigned(const Decimal128Value& value,
                                                  const schema::LogicalType& source) {
  SignedMagnitude decoded = decode(value);
  divide_power_ten(decoded.magnitude, source.parameter_1());
  if (decoded.negative || decoded.magnitude.limbs[2] != 0U || decoded.magnitude.limbs[3] != 0U ||
      decoded.magnitude.limbs[4] != 0U || decoded.magnitude.limbs[5] != 0U ||
      decoded.magnitude.limbs[6] != 0U || decoded.magnitude.limbs[7] != 0U)
    return common::make_unexpected(out_of_range("DECIMAL to unsigned cast is out of range"));
  return decoded.magnitude.limbs[0] |
         (static_cast<std::uint64_t>(decoded.magnitude.limbs[1]) << 32U);
}

common::Result<float> decimal_to_float(const Decimal128Value& value,
                                       const schema::LogicalType& source) {
  return decimal_to_floating<float>(value, source);
}

common::Result<double> decimal_to_double(const Decimal128Value& value,
                                         const schema::LogicalType& source) {
  return decimal_to_floating<double>(value, source);
}

} // namespace chronos::query::detail
