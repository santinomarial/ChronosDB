#include "chronos/columnar/column_vector.hpp"
#include "columnar/columnar_test_support.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::columnar {
namespace {

using schema::LogicalTypeKind;

[[nodiscard]] common::Result<ColumnVectorView>
view(const LogicalTypeKind kind, const bool nullable, const std::uint32_t rows,
     const std::uint32_t nulls, const common::ByteView validity, const common::ByteView offsets,
     const common::ByteView values) {
  return ColumnVectorView::create(
      ColumnVectorMetadata{.column_id = test::id<schema::ColumnId>(1),
                           .type = test::type(kind),
                           .nullable = nullable,
                           .row_count = rows,
                           .null_count = nulls},
      ColumnVectorBufferView{.validity = validity, .offsets = offsets, .values = values});
}

TEST(ColumnVectorTest, ValidatesEveryFixedWidthRegistryTypeAndPreservesCanonicalBytes) {
  constexpr std::array<std::pair<LogicalTypeKind, std::size_t>, 15> kTypes{{
      {LogicalTypeKind::kInt8, 1U},
      {LogicalTypeKind::kInt16, 2U},
      {LogicalTypeKind::kInt32, 4U},
      {LogicalTypeKind::kInt64, 8U},
      {LogicalTypeKind::kUInt8, 1U},
      {LogicalTypeKind::kUInt16, 2U},
      {LogicalTypeKind::kUInt32, 4U},
      {LogicalTypeKind::kUInt64, 8U},
      {LogicalTypeKind::kFloat32, 4U},
      {LogicalTypeKind::kFloat64, 8U},
      {LogicalTypeKind::kDecimal, 16U},
      {LogicalTypeKind::kTimestampNs, 8U},
      {LogicalTypeKind::kDate, 4U},
      {LogicalTypeKind::kUuid, 16U},
      {LogicalTypeKind::kBool, 0U},
  }};

  for (const auto [kind, width] : kTypes) {
    SCOPED_TRACE(schema::logical_type_kind_name(kind));
    const schema::LogicalType logical_type = kind == LogicalTypeKind::kDecimal
                                                 ? schema::LogicalType::decimal(38U, 10U).value()
                                                 : test::type(kind);
    std::vector<std::byte> values(width == 0U ? 1U : width * 2U, std::byte{0});
    if (kind == LogicalTypeKind::kBool) {
      values[0] = std::byte{0x02};
    } else if (kind != LogicalTypeKind::kDecimal) {
      values[width] = std::byte{0x5a};
    }
    const common::Result<ColumnVectorView> vector = ColumnVectorView::create(
        ColumnVectorMetadata{.column_id = test::id<schema::ColumnId>(1),
                             .type = logical_type,
                             .nullable = false,
                             .row_count = 2U,
                             .null_count = 0U},
        ColumnVectorBufferView{.validity = {}, .offsets = {}, .values = values});
    ASSERT_TRUE(vector.has_value()) << vector.error().to_string();
    const common::Result<ColumnCellView> second = vector->cell(1U);
    ASSERT_TRUE(second.has_value());
    if (kind == LogicalTypeKind::kBool) {
      EXPECT_TRUE(second->boolean().value());
    } else {
      EXPECT_EQ(second->bytes().value().size(), width);
      EXPECT_EQ(second->bytes().value()[0], values[width]);
    }
  }
}

TEST(ColumnVectorTest, EnforcesCanonicalValidityAndNullFixedSlots) {
  const std::vector<std::byte> values(9U * sizeof(std::int32_t), std::byte{0});
  ASSERT_TRUE(view(LogicalTypeKind::kInt32, true, 9U, 1U,
                   std::array{std::byte{0xfe}, std::byte{0x01}}, {}, values)
                  .has_value());
  EXPECT_FALSE(view(LogicalTypeKind::kInt32, true, 9U, 0U, {}, {}, values).has_value());
  EXPECT_FALSE(view(LogicalTypeKind::kInt32, true, 9U, 2U,
                    std::array{std::byte{0xfe}, std::byte{0x01}}, {}, values)
                   .has_value());
  EXPECT_FALSE(view(LogicalTypeKind::kInt32, true, 9U, 1U,
                    std::array{std::byte{0xfe}, std::byte{0x81}}, {}, values)
                   .has_value());
  EXPECT_FALSE(view(LogicalTypeKind::kInt32, false, 9U, 0U, std::array{std::byte{0xff}}, {}, values)
                   .has_value());
  EXPECT_FALSE(view(LogicalTypeKind::kInt32, false, 0U, 0U, {}, {}, {}).has_value());

  std::vector<std::byte> bad_null_slot = values;
  bad_null_slot[0] = std::byte{1};
  EXPECT_FALSE(view(LogicalTypeKind::kInt32, true, 9U, 1U,
                    std::array{std::byte{0xfe}, std::byte{0x01}}, {}, bad_null_slot)
                   .has_value());
}

TEST(ColumnVectorTest, EnforcesCanonicalPackedBooleanBits) {
  EXPECT_TRUE(view(LogicalTypeKind::kBool, true, 3U, 1U, std::array{std::byte{0x05}}, {},
                   std::array{std::byte{0x04}})
                  .has_value());
  EXPECT_FALSE(view(LogicalTypeKind::kBool, true, 3U, 1U, std::array{std::byte{0x05}}, {},
                    std::array{std::byte{0x06}})
                   .has_value());
  EXPECT_FALSE(
      view(LogicalTypeKind::kBool, false, 3U, 0U, {}, {}, std::array{std::byte{0x81}}).has_value());
}

TEST(ColumnVectorTest, ValidatesOffsetsNullsUtf8AndSafeRowSlices) {
  std::vector<std::byte> offsets;
  for (const std::uint32_t offset : {0U, 2U, 2U, 5U}) {
    test::append_u32(offsets, offset);
  }
  const std::vector<std::byte> values{std::byte{'o'}, std::byte{'k'}, std::byte{'x'},
                                      std::byte{'y'}, std::byte{'z'}};
  const std::array validity{std::byte{0x05}};
  const auto vector = view(LogicalTypeKind::kString, true, 3U, 1U, validity, offsets, values);
  ASSERT_TRUE(vector.has_value()) << vector.error().to_string();
  EXPECT_EQ(vector->cell(0U)->bytes()->size(), 2U);
  EXPECT_TRUE(vector->cell(1U)->is_null());
  EXPECT_EQ(vector->cell(2U)->bytes()->size(), 3U);
  EXPECT_EQ(vector->cell(3U).error().code(), common::StatusCode::kOutOfRange);

  std::vector<std::byte> decreasing = offsets;
  decreasing[8] = std::byte{1};
  EXPECT_FALSE(
      view(LogicalTypeKind::kBinary, true, 3U, 1U, std::array{std::byte{0x05}}, decreasing, values)
          .has_value());

  std::vector<std::byte> nonempty_null = offsets;
  nonempty_null[8] = std::byte{3};
  EXPECT_FALSE(view(LogicalTypeKind::kBinary, true, 3U, 1U, std::array{std::byte{0x05}},
                    nonempty_null, values)
                   .has_value());

  std::vector<std::byte> utf8_offsets;
  test::append_u32(utf8_offsets, 0U);
  test::append_u32(utf8_offsets, 1U);
  EXPECT_FALSE(
      view(LogicalTypeKind::kString, false, 1U, 0U, {}, utf8_offsets, std::array{std::byte{0x80}})
          .has_value());
  EXPECT_TRUE(
      view(LogicalTypeKind::kBinary, false, 1U, 0U, {}, utf8_offsets, std::array{std::byte{0x80}})
          .has_value());
}

TEST(ColumnVectorTest, DecimalMagnitudeMustBeStrictlyBelowTenToPrecision) {
  const schema::LogicalType decimal = schema::LogicalType::decimal(3U, 1U).value();
  std::vector<std::byte> accepted(16U, std::byte{0});
  accepted[0] = std::byte{0xe7};
  accepted[1] = std::byte{0x03}; // 999
  const ColumnVectorMetadata metadata{.column_id = test::id<schema::ColumnId>(1),
                                      .type = decimal,
                                      .nullable = false,
                                      .row_count = 1U,
                                      .null_count = 0U};
  EXPECT_TRUE(
      ColumnVectorView::create(
          metadata, ColumnVectorBufferView{.validity = {}, .offsets = {}, .values = accepted})
          .has_value());

  std::vector<std::byte> rejected = accepted;
  rejected[0] = std::byte{0xe8}; // 1000
  EXPECT_FALSE(
      ColumnVectorView::create(
          metadata, ColumnVectorBufferView{.validity = {}, .offsets = {}, .values = rejected})
          .has_value());

  std::vector<std::byte> negative(16U, std::byte{0xff});
  negative[0] = std::byte{0x19};
  negative[1] = std::byte{0xfc}; // -999
  EXPECT_TRUE(
      ColumnVectorView::create(
          metadata, ColumnVectorBufferView{.validity = {}, .offsets = {}, .values = negative})
          .has_value());
}

TEST(ColumnVectorTest, OwnedVectorKeepsBuffersStableAndExposesOnlyConstViews) {
  OwnedColumnVector owner =
      test::fixed_vector(1, test::type(LogicalTypeKind::kInt16), false, 2U, {}, 0U,
                         {std::byte{1}, std::byte{0}, std::byte{2}, std::byte{0}});
  const ColumnVectorView borrowed = owner.view();
  EXPECT_EQ(borrowed.buffer_bytes(), 4U);
  EXPECT_EQ(borrowed.cell(1U)->bytes().value()[0], std::byte{2});
  EXPECT_FALSE(borrowed.cell(1U)->boolean().has_value());
}

} // namespace
} // namespace chronos::columnar
