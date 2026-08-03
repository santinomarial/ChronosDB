#include "chronos/schema/column_definition.hpp"
#include "schema_test_support.hpp"

#include <gtest/gtest.h>
#include <string>
#include <type_traits>

namespace chronos::schema {
namespace {

static_assert(!std::is_default_constructible_v<ColumnDefinition>);

TEST(ColumnDefinitionTest, OwnsValidatedImmutableDefinition) {
  std::string name = "café";
  const common::Result<ColumnDefinition> column = ColumnDefinition::create(
      test::make_id<ColumnId>(1), name, test::make_type(LogicalTypeKind::kString), true);
  ASSERT_TRUE(column.has_value());
  name.assign("changed");

  EXPECT_EQ(column->id(), test::make_id<ColumnId>(1));
  EXPECT_EQ(column->name(), "café");
  EXPECT_EQ(column->type().kind(), LogicalTypeKind::kString);
  EXPECT_TRUE(column->nullable());
}

TEST(ColumnDefinitionTest, RejectsEmptyInvalidUtf8AndEmbeddedNullNames) {
  const ColumnId id = test::make_id<ColumnId>(1);
  const LogicalType type = test::make_type(LogicalTypeKind::kInt64);

  const common::Result<ColumnDefinition> empty = ColumnDefinition::create(id, {}, type, false);
  ASSERT_FALSE(empty.has_value());
  EXPECT_EQ(empty.error().code(), common::StatusCode::kInvalidArgument);

  const common::Result<ColumnDefinition> invalid = ColumnDefinition::create(
      id, std::string{static_cast<char>(0xc0), static_cast<char>(0x80)}, type, false);
  ASSERT_FALSE(invalid.has_value());
  EXPECT_EQ(invalid.error().code(), common::StatusCode::kInvalidArgument);

  const common::Result<ColumnDefinition> embedded_null =
      ColumnDefinition::create(id, std::string{"a\0b", 3}, type, false);
  ASSERT_FALSE(embedded_null.has_value());
  EXPECT_EQ(embedded_null.error().code(), common::StatusCode::kInvalidArgument);
}

TEST(ColumnDefinitionTest, UsesExactUtf8BytesRatherThanNormalization) {
  const std::string composed = "é";
  const std::string decomposed = "e\xcc\x81";
  const ColumnDefinition first =
      ColumnDefinition::create(test::make_id<ColumnId>(1), composed,
                               test::make_type(LogicalTypeKind::kString), false)
          .value();
  const ColumnDefinition second =
      ColumnDefinition::create(test::make_id<ColumnId>(2), decomposed,
                               test::make_type(LogicalTypeKind::kString), false)
          .value();
  EXPECT_NE(first.name(), second.name());
}

} // namespace
} // namespace chronos::schema
