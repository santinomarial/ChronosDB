#ifndef CHRONOS_SCHEMA_LOGICAL_TYPE_HPP_
#define CHRONOS_SCHEMA_LOGICAL_TYPE_HPP_

#include "chronos/common/result.hpp"

#include <compare>
#include <cstdint>
#include <string_view>

namespace chronos::schema {

enum class LogicalTypeKind : std::uint16_t {
  kBool = 1,
  kInt8 = 2,
  kInt16 = 3,
  kInt32 = 4,
  kInt64 = 5,
  kUInt8 = 6,
  kUInt16 = 7,
  kUInt32 = 8,
  kUInt64 = 9,
  kFloat32 = 10,
  kFloat64 = 11,
  kDecimal = 12,
  kTimestampNs = 13,
  kDate = 14,
  kSymbol = 15,
  kString = 16,
  kBinary = 17,
  kUuid = 18,
};

[[nodiscard]] common::Result<LogicalTypeKind> logical_type_kind_from_code(std::uint16_t code);
[[nodiscard]] std::string_view logical_type_kind_name(LogicalTypeKind kind) noexcept;

class LogicalType {
public:
  LogicalType() = delete;

  [[nodiscard]] static common::Result<LogicalType>
  create(LogicalTypeKind kind, std::uint16_t parameter_0 = 0,
         std::uint16_t parameter_1 = 0);
  [[nodiscard]] static common::Result<LogicalType> decimal(std::uint16_t precision,
                                                           std::uint16_t scale);

  [[nodiscard]] constexpr LogicalTypeKind kind() const noexcept { return kind_; }
  [[nodiscard]] constexpr std::uint16_t code() const noexcept {
    return static_cast<std::uint16_t>(kind_);
  }
  [[nodiscard]] constexpr std::uint16_t parameter_0() const noexcept { return parameter_0_; }
  [[nodiscard]] constexpr std::uint16_t parameter_1() const noexcept { return parameter_1_; }
  [[nodiscard]] constexpr bool is_decimal() const noexcept {
    return kind_ == LogicalTypeKind::kDecimal;
  }
  [[nodiscard]] constexpr bool is_variable_width() const noexcept {
    return kind_ == LogicalTypeKind::kSymbol || kind_ == LogicalTypeKind::kString ||
           kind_ == LogicalTypeKind::kBinary;
  }

  friend constexpr auto operator<=>(const LogicalType&, const LogicalType&) = default;

private:
  constexpr LogicalType(LogicalTypeKind kind, std::uint16_t parameter_0,
                        std::uint16_t parameter_1) noexcept
      : kind_(kind), parameter_0_(parameter_0), parameter_1_(parameter_1) {}

  LogicalTypeKind kind_;
  std::uint16_t parameter_0_;
  std::uint16_t parameter_1_;
};

} // namespace chronos::schema

#endif // CHRONOS_SCHEMA_LOGICAL_TYPE_HPP_
