#include "chronos/schema/logical_type.hpp"

namespace chronos::schema {
namespace {

[[nodiscard]] bool is_known_kind(const LogicalTypeKind kind) noexcept {
  switch (kind) {
  case LogicalTypeKind::kBool:
  case LogicalTypeKind::kInt8:
  case LogicalTypeKind::kInt16:
  case LogicalTypeKind::kInt32:
  case LogicalTypeKind::kInt64:
  case LogicalTypeKind::kUInt8:
  case LogicalTypeKind::kUInt16:
  case LogicalTypeKind::kUInt32:
  case LogicalTypeKind::kUInt64:
  case LogicalTypeKind::kFloat32:
  case LogicalTypeKind::kFloat64:
  case LogicalTypeKind::kDecimal:
  case LogicalTypeKind::kTimestampNs:
  case LogicalTypeKind::kDate:
  case LogicalTypeKind::kSymbol:
  case LogicalTypeKind::kString:
  case LogicalTypeKind::kBinary:
  case LogicalTypeKind::kUuid:
    return true;
  }
  return false;
}

} // namespace

common::Result<LogicalTypeKind> logical_type_kind_from_code(const std::uint16_t code) {
  if (code < static_cast<std::uint16_t>(LogicalTypeKind::kBool) ||
      code > static_cast<std::uint16_t>(LogicalTypeKind::kUuid)) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotSupported, "logical type code is unsupported"});
  }
  const auto kind = static_cast<LogicalTypeKind>(code);
  return kind;
}

std::string_view logical_type_kind_name(const LogicalTypeKind kind) noexcept {
  switch (kind) {
  case LogicalTypeKind::kBool:
    return "BOOL";
  case LogicalTypeKind::kInt8:
    return "INT8";
  case LogicalTypeKind::kInt16:
    return "INT16";
  case LogicalTypeKind::kInt32:
    return "INT32";
  case LogicalTypeKind::kInt64:
    return "INT64";
  case LogicalTypeKind::kUInt8:
    return "UINT8";
  case LogicalTypeKind::kUInt16:
    return "UINT16";
  case LogicalTypeKind::kUInt32:
    return "UINT32";
  case LogicalTypeKind::kUInt64:
    return "UINT64";
  case LogicalTypeKind::kFloat32:
    return "FLOAT32";
  case LogicalTypeKind::kFloat64:
    return "FLOAT64";
  case LogicalTypeKind::kDecimal:
    return "DECIMAL";
  case LogicalTypeKind::kTimestampNs:
    return "TIMESTAMP_NS";
  case LogicalTypeKind::kDate:
    return "DATE";
  case LogicalTypeKind::kSymbol:
    return "SYMBOL";
  case LogicalTypeKind::kString:
    return "STRING";
  case LogicalTypeKind::kBinary:
    return "BINARY";
  case LogicalTypeKind::kUuid:
    return "UUID";
  }
  return "UNKNOWN";
}

common::Result<LogicalType> LogicalType::create(const LogicalTypeKind kind,
                                                const std::uint16_t parameter_0,
                                                const std::uint16_t parameter_1) {
  if (!is_known_kind(kind)) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInvalidArgument, "logical type kind is invalid"});
  }
  if (kind == LogicalTypeKind::kDecimal) {
    if (parameter_0 == 0 || parameter_0 > 38U) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kInvalidArgument, "decimal precision must be in the range 1..38"});
    }
    if (parameter_1 > parameter_0) {
      return common::make_unexpected(common::Status{common::StatusCode::kInvalidArgument,
                                                    "decimal scale must not exceed precision"});
    }
    return LogicalType{kind, parameter_0, parameter_1};
  }
  if (parameter_0 != 0 || parameter_1 != 0) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInvalidArgument, "non-decimal logical type parameters must be zero"});
  }
  return LogicalType{kind, 0, 0};
}

common::Result<LogicalType> LogicalType::decimal(const std::uint16_t precision,
                                                 const std::uint16_t scale) {
  return create(LogicalTypeKind::kDecimal, precision, scale);
}

} // namespace chronos::schema
