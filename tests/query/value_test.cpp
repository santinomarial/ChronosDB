#include "chronos/columnar/column_vector.hpp"
#include "chronos/query/value.hpp"
#include "chronos/schema/logical_type.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <limits>
#include <string>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

TEST(ScalarValueTest, ValidatesOwnedDomainsAndUtf8) {
  EXPECT_TRUE(ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt8), -128).has_value());
  EXPECT_FALSE(ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt8), 128).has_value());
  EXPECT_TRUE(
      ScalarValue::unsigned_value(type(schema::LogicalTypeKind::kUInt16), 65'535U).has_value());
  EXPECT_FALSE(
      ScalarValue::unsigned_value(type(schema::LogicalTypeKind::kUInt16), 65'536U).has_value());
  EXPECT_TRUE(
      ScalarValue::text(type(schema::LogicalTypeKind::kString), "valid \xE2\x82\xAC").has_value());
  EXPECT_FALSE(ScalarValue::text(type(schema::LogicalTypeKind::kString), std::string{"\xff", 1U})
                   .has_value());

  const schema::LogicalType decimal_type = schema::LogicalType::decimal(3U, 1U).value();
  Decimal128Value decimal;
  decimal.coefficient[0] = std::byte{0xe7}; // 999 little-endian.
  decimal.coefficient[1] = std::byte{0x03};
  EXPECT_TRUE(ScalarValue::decimal(decimal_type, decimal).has_value());
  decimal.coefficient[0] = std::byte{0xe8}; // 1000 is outside DECIMAL(3, 1).
  EXPECT_FALSE(ScalarValue::decimal(decimal_type, decimal).has_value());
}

TEST(ScalarValueTest, CopiesCanonicalPhysicalCellsWithoutUnalignedLoads) {
  constexpr std::array<std::byte, 8> kSignedBytes{std::byte{0xfe}, std::byte{0xff}, std::byte{0xff},
                                                  std::byte{0xff}, std::byte{0xff}, std::byte{0xff},
                                                  std::byte{0xff}, std::byte{0xff}};
  const auto signed_column =
      columnar::PhysicalColumnView::create({.type = type(schema::LogicalTypeKind::kInt64),
                                            .nullable = false,
                                            .row_count = 1U,
                                            .null_count = 0U},
                                           {.validity = {}, .offsets = {}, .values = kSignedBytes});
  ASSERT_TRUE(signed_column.has_value());
  const auto signed_scalar =
      ScalarValue::from_column_cell(signed_column->type(), signed_column->cell(0U).value());
  ASSERT_TRUE(signed_scalar.has_value());
  EXPECT_EQ(*std::get_if<std::int64_t>(&signed_scalar->storage()), -2);

  const std::array<std::byte, 4> offsets{std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0}};
  const std::array<std::byte, 4> end_offsets{std::byte{3}, std::byte{0}, std::byte{0},
                                             std::byte{0}};
  std::array<std::byte, 8> complete_offsets{};
  std::ranges::copy(offsets, complete_offsets.begin());
  std::ranges::copy(end_offsets, complete_offsets.begin() + 4);
  const std::array<std::byte, 3> text_bytes{std::byte{'a'}, std::byte{'b'}, std::byte{'c'}};
  const auto text_column = columnar::PhysicalColumnView::create(
      {.type = type(schema::LogicalTypeKind::kString),
       .nullable = false,
       .row_count = 1U,
       .null_count = 0U},
      {.validity = {}, .offsets = complete_offsets, .values = text_bytes});
  ASSERT_TRUE(text_column.has_value());
  const auto text_scalar =
      ScalarValue::from_column_cell(text_column->type(), text_column->cell(0U).value());
  ASSERT_TRUE(text_scalar.has_value());
  EXPECT_EQ(*std::get_if<std::string>(&text_scalar->storage()), "abc");
}

TEST(ScalarValueTest, EncodesOwnedValuesIntoCanonicalPhysicalBytes) {
  const auto signed_value =
      ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), -2).value();
  const auto signed_bytes = encode_canonical_scalar_value(signed_value);
  ASSERT_TRUE(signed_bytes.has_value());
  ASSERT_EQ(signed_bytes->size(), 8U);
  EXPECT_EQ(signed_bytes->front(), std::byte{0xfe});
  EXPECT_EQ(signed_bytes->back(), std::byte{0xff});

  const auto boolean = encode_canonical_scalar_value(ScalarValue::boolean(true).value());
  ASSERT_TRUE(boolean.has_value());
  EXPECT_EQ(*boolean, (std::vector<std::byte>{std::byte{1U}}));

  const auto text_value =
      ScalarValue::text(type(schema::LogicalTypeKind::kString), "hello").value();
  const auto text_bytes = encode_canonical_scalar_value(text_value);
  ASSERT_TRUE(text_bytes.has_value());
  EXPECT_TRUE(std::ranges::equal(*text_bytes, std::as_bytes(std::span{"hello", std::size_t{5U}})));

  const auto null =
      encode_canonical_scalar_value(ScalarValue::null(type(schema::LogicalTypeKind::kInt32)));
  ASSERT_TRUE(null.has_value());
  EXPECT_TRUE(null->empty());
  EXPECT_EQ(encode_canonical_scalar_value(ScalarValue::untyped_null()).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(ScalarValueTest, ImplementsSqlNullNaNAndTotalOrderingRules) {
  const ScalarValue null = ScalarValue::null(type(schema::LogicalTypeKind::kFloat64));
  const ScalarValue one = ScalarValue::float64(1.0).value();
  const ScalarValue infinity =
      ScalarValue::float64(std::numeric_limits<double>::infinity()).value();
  const ScalarValue nan = ScalarValue::float64(std::numeric_limits<double>::quiet_NaN()).value();

  EXPECT_EQ(sql_scalar_equal(null, null).value(), SqlTruthValue::kUnknown);
  EXPECT_EQ(sql_scalar_equal(nan, nan).value(), SqlTruthValue::kFalse);
  EXPECT_EQ(sql_scalar_equal(one, one).value(), SqlTruthValue::kTrue);
  EXPECT_LT(compare_scalar_values(infinity, nan, ScalarNullPlacement::kLast).value(), 0);
  EXPECT_LT(compare_scalar_values(nan, null, ScalarNullPlacement::kLast).value(), 0);
  EXPECT_GT(compare_scalar_values(null, one, ScalarNullPlacement::kLast).value(), 0);
  EXPECT_LT(compare_scalar_values(null, one, ScalarNullPlacement::kFirst).value(), 0);
}

TEST(ScalarValueTest, ComparesValidatedCanonicalBytesWithoutOwnedRowValues) {
  const std::array<std::byte, 8U> negative_two{std::byte{0xfe}, std::byte{0xff}, std::byte{0xff},
                                               std::byte{0xff}, std::byte{0xff}, std::byte{0xff},
                                               std::byte{0xff}, std::byte{0xff}};
  const std::array<std::byte, 8U> positive_one{std::byte{0x01}};
  EXPECT_LT(compare_canonical_scalar_bytes(type(schema::LogicalTypeKind::kInt64), false,
                                           negative_two, false, positive_one,
                                           ScalarNullPlacement::kLast)
                .value(),
            0);
  const std::array<std::byte, 2U> text_a{std::byte{'a'}, std::byte{'a'}};
  const std::array<std::byte, 2U> text_b{std::byte{'a'}, std::byte{'b'}};
  EXPECT_LT(compare_canonical_scalar_bytes(type(schema::LogicalTypeKind::kString), false, text_a,
                                           false, text_b, ScalarNullPlacement::kLast)
                .value(),
            0);
  EXPECT_LT(compare_canonical_scalar_bytes(type(schema::LogicalTypeKind::kString), true, {}, false,
                                           text_b, ScalarNullPlacement::kFirst)
                .value(),
            0);
  const std::array<std::byte, 1U> invalid_boolean{std::byte{2U}};
  EXPECT_EQ(compare_canonical_scalar_bytes(type(schema::LogicalTypeKind::kBool), false,
                                           invalid_boolean, false, invalid_boolean,
                                           ScalarNullPlacement::kLast)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(compare_canonical_scalar_bytes(type(schema::LogicalTypeKind::kInt64), true,
                                           positive_one, true, {}, ScalarNullPlacement::kLast)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

TEST(ScalarValuePropertyTest, SignedPhysicalRoundTripsAcrossDeterministicValues) {
  for (std::int64_t value = -10'000; value <= 10'000; value += 97) {
    const std::uint64_t bits = std::bit_cast<std::uint64_t>(value);
    std::array<std::byte, 8> bytes{};
    for (std::size_t index = 0U; index < bytes.size(); ++index)
      bytes[index] = static_cast<std::byte>((bits >> (index * 8U)) & 0xffU);
    const auto column =
        columnar::PhysicalColumnView::create({.type = type(schema::LogicalTypeKind::kInt64),
                                              .nullable = false,
                                              .row_count = 1U,
                                              .null_count = 0U},
                                             {.validity = {}, .offsets = {}, .values = bytes});
    ASSERT_TRUE(column.has_value());
    const auto scalar = ScalarValue::from_column_cell(column->type(), column->cell(0U).value());
    ASSERT_TRUE(scalar.has_value());
    EXPECT_EQ(*std::get_if<std::int64_t>(&scalar->storage()), value);
  }
}

} // namespace
} // namespace chronos::query
