#ifndef CHRONOS_QUERY_DECIMAL_INTERNAL_HPP_
#define CHRONOS_QUERY_DECIMAL_INTERNAL_HPP_

#include "chronos/common/result.hpp"
#include "chronos/query/value.hpp"
#include "chronos/schema/logical_type.hpp"

#include <array>
#include <cstdint>

namespace chronos::query::detail {

enum class DecimalOperation : std::uint8_t {
  kAdd,
  kSubtract,
  kMultiply,
  kDivide,
  kRemainder,
};

struct ExactNumericAccumulator {
  std::array<std::uint32_t, 8> magnitude{};
  bool negative{};

  [[nodiscard]] common::Result<void> add_signed(std::int64_t value);
  [[nodiscard]] common::Result<void> add_unsigned(std::uint64_t value);
  [[nodiscard]] common::Result<void> add_decimal(const Decimal128Value& value);
  [[nodiscard]] common::Result<void> merge(const ExactNumericAccumulator& other);
  [[nodiscard]] common::Result<std::int64_t> signed_result() const;
  [[nodiscard]] common::Result<std::uint64_t> unsigned_result() const;
  [[nodiscard]] common::Result<Decimal128Value>
  decimal_result(const schema::LogicalType& type) const;
};

[[nodiscard]] common::Result<Decimal128Value>
evaluate_decimal(DecimalOperation operation, const Decimal128Value& left,
                 const Decimal128Value& right, const schema::LogicalType& result_type);

[[nodiscard]] common::Result<Decimal128Value> negate_decimal(const Decimal128Value& value,
                                                             const schema::LogicalType& type);
[[nodiscard]] common::Result<Decimal128Value> absolute_decimal(const Decimal128Value& value,
                                                               const schema::LogicalType& type);

[[nodiscard]] common::Result<Decimal128Value>
decimal_from_signed(std::int64_t value, const schema::LogicalType& target);
[[nodiscard]] common::Result<Decimal128Value>
decimal_from_unsigned(std::uint64_t value, const schema::LogicalType& target);
[[nodiscard]] common::Result<Decimal128Value> decimal_from_float(float value,
                                                                 const schema::LogicalType& target);
[[nodiscard]] common::Result<Decimal128Value>
decimal_from_double(double value, const schema::LogicalType& target);
[[nodiscard]] common::Result<Decimal128Value> rescale_decimal(const Decimal128Value& value,
                                                              const schema::LogicalType& source,
                                                              const schema::LogicalType& target);

[[nodiscard]] common::Result<std::int64_t> decimal_to_signed(const Decimal128Value& value,
                                                             const schema::LogicalType& source);
[[nodiscard]] common::Result<std::uint64_t> decimal_to_unsigned(const Decimal128Value& value,
                                                                const schema::LogicalType& source);
[[nodiscard]] common::Result<float> decimal_to_float(const Decimal128Value& value,
                                                     const schema::LogicalType& source);
[[nodiscard]] common::Result<double> decimal_to_double(const Decimal128Value& value,
                                                       const schema::LogicalType& source);

} // namespace chronos::query::detail

#endif // CHRONOS_QUERY_DECIMAL_INTERNAL_HPP_
