#include "chronos/schema/logical_type.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <limits>
#include <string_view>
#include <utility>

namespace chronos::schema {
namespace {

TEST(LogicalTypeTest, RegistryCodesAndNamesExactlyMatchColumnarBatchV1) {
  constexpr std::array<std::pair<LogicalTypeKind, std::string_view>, 18> kKinds{{
      {LogicalTypeKind::kBool, "BOOL"},
      {LogicalTypeKind::kInt8, "INT8"},
      {LogicalTypeKind::kInt16, "INT16"},
      {LogicalTypeKind::kInt32, "INT32"},
      {LogicalTypeKind::kInt64, "INT64"},
      {LogicalTypeKind::kUInt8, "UINT8"},
      {LogicalTypeKind::kUInt16, "UINT16"},
      {LogicalTypeKind::kUInt32, "UINT32"},
      {LogicalTypeKind::kUInt64, "UINT64"},
      {LogicalTypeKind::kFloat32, "FLOAT32"},
      {LogicalTypeKind::kFloat64, "FLOAT64"},
      {LogicalTypeKind::kDecimal, "DECIMAL"},
      {LogicalTypeKind::kTimestampNs, "TIMESTAMP_NS"},
      {LogicalTypeKind::kDate, "DATE"},
      {LogicalTypeKind::kSymbol, "SYMBOL"},
      {LogicalTypeKind::kString, "STRING"},
      {LogicalTypeKind::kBinary, "BINARY"},
      {LogicalTypeKind::kUuid, "UUID"},
  }};

  for (std::size_t index = 0; index < kKinds.size(); ++index) {
    const auto [kind, name] = kKinds[index];
    const std::uint16_t expected_code = static_cast<std::uint16_t>(index + 1U);
    SCOPED_TRACE(name);
    const common::Result<LogicalTypeKind> decoded = logical_type_kind_from_code(expected_code);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, kind);
    EXPECT_EQ(logical_type_kind_name(kind), name);

    const common::Result<LogicalType> type = kind == LogicalTypeKind::kDecimal
                                                 ? LogicalType::decimal(38, 18)
                                                 : LogicalType::create(kind);
    ASSERT_TRUE(type.has_value());
    EXPECT_EQ(type->code(), expected_code);
  }
}

TEST(LogicalTypeTest, RejectsEveryUnknownCodeBoundaryWithoutNarrowing) {
  for (const std::uint16_t code : {std::uint16_t{0}, std::uint16_t{19}, std::uint16_t{257},
                                   std::numeric_limits<std::uint16_t>::max()}) {
    const common::Result<LogicalTypeKind> kind = logical_type_kind_from_code(code);
    ASSERT_FALSE(kind.has_value());
    EXPECT_EQ(kind.error().code(), common::StatusCode::kNotSupported);
  }
}

TEST(LogicalTypeTest, ValidatesEveryDecimalBoundary) {
  for (std::uint16_t precision = 1; precision <= 38; ++precision) {
    for (std::uint16_t scale = 0; scale <= precision; ++scale) {
      const common::Result<LogicalType> decimal = LogicalType::decimal(precision, scale);
      ASSERT_TRUE(decimal.has_value());
      EXPECT_TRUE(decimal->is_decimal());
      EXPECT_EQ(decimal->parameter_0(), precision);
      EXPECT_EQ(decimal->parameter_1(), scale);
    }
  }

  EXPECT_FALSE(LogicalType::decimal(0, 0).has_value());
  EXPECT_FALSE(LogicalType::decimal(39, 0).has_value());
  EXPECT_FALSE(LogicalType::decimal(10, 11).has_value());
}

TEST(LogicalTypeTest, RejectsParametersForEveryNonDecimalType) {
  for (std::uint16_t code = 1; code <= 18; ++code) {
    const LogicalTypeKind kind = logical_type_kind_from_code(code).value();
    if (kind == LogicalTypeKind::kDecimal) {
      continue;
    }
    EXPECT_FALSE(LogicalType::create(kind, 1, 0).has_value());
    EXPECT_FALSE(LogicalType::create(kind, 0, 1).has_value());
  }
}

TEST(LogicalTypeTest, IdentifiesOnlyTheThreeVariableWidthTypes) {
  EXPECT_TRUE(LogicalType::create(LogicalTypeKind::kSymbol)->is_variable_width());
  EXPECT_TRUE(LogicalType::create(LogicalTypeKind::kString)->is_variable_width());
  EXPECT_TRUE(LogicalType::create(LogicalTypeKind::kBinary)->is_variable_width());
  EXPECT_FALSE(LogicalType::create(LogicalTypeKind::kUuid)->is_variable_width());
}

} // namespace
} // namespace chronos::schema
