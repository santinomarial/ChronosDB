#include "chronos/columnar/columnar_batch.hpp"
#include "columnar/columnar_test_support.hpp"

#include <cstddef>
#include <gtest/gtest.h>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

namespace chronos::columnar {
namespace {

static_assert(!std::is_default_constructible_v<OwnedColumnVector>);
static_assert(!std::is_default_constructible_v<OwnedColumnarBatch>);
static_assert(!std::is_copy_constructible_v<OwnedColumnVector>);
static_assert(!std::is_copy_constructible_v<OwnedColumnarBatch>);
static_assert(std::is_nothrow_move_constructible_v<OwnedColumnVector>);
static_assert(std::is_nothrow_move_constructible_v<OwnedColumnarBatch>);

TEST(ColumnarBatchTest, PinsSchemaAndOwnsExactSchemaOrdinalVectors) {
  std::shared_ptr<const schema::TableSchema> schema = test::batch_schema();
  const auto batch = OwnedColumnarBatch::create(schema, test::batch_columns());
  ASSERT_TRUE(batch.has_value()) << batch.error().to_string();
  EXPECT_EQ(batch->schema_ptr(), schema);
  EXPECT_EQ(batch->row_count(), 2U);
  EXPECT_EQ(batch->columns().size(), 3U);
  EXPECT_EQ(batch->buffer_bytes(), 16U + 1U + 12U + 1U + 1U);
  EXPECT_GE(batch->retained_buffer_bytes(), batch->buffer_bytes());
  ASSERT_NE(batch->column(1U), nullptr);
  EXPECT_EQ(batch->column(1U)->column_id(), schema->columns()[1].id());
  EXPECT_EQ(batch->column(3U), nullptr);
  EXPECT_EQ(batch->cell({.column_ordinal = 0U, .row = 1U})->bytes()->size(), 8U);
  EXPECT_TRUE(batch->cell({.column_ordinal = 1U, .row = 1U})->is_null());
  EXPECT_TRUE(batch->cell({.column_ordinal = 2U, .row = 0U})->boolean().value());
  EXPECT_EQ(batch->cell({.column_ordinal = 3U, .row = 0U}).error().code(),
            common::StatusCode::kOutOfRange);
}

TEST(ColumnarBatchTest, RejectsMissingReorderedOrSchemaMismatchedColumns) {
  const auto schema = test::batch_schema();
  EXPECT_FALSE(OwnedColumnarBatch::create(nullptr, test::batch_columns()).has_value());
  std::vector<OwnedColumnVector> missing = test::batch_columns();
  missing.pop_back();
  EXPECT_FALSE(OwnedColumnarBatch::create(schema, std::move(missing)).has_value());

  std::vector<OwnedColumnVector> reordered = test::batch_columns();
  std::swap(reordered[0], reordered[1]);
  EXPECT_FALSE(OwnedColumnarBatch::create(schema, std::move(reordered)).has_value());

  std::vector<OwnedColumnVector> wrong_rows = test::batch_columns();
  wrong_rows[2] = test::fixed_vector(3, test::type(schema::LogicalTypeKind::kBool), false, 1U, {},
                                     0U, {std::byte{1}});
  EXPECT_FALSE(OwnedColumnarBatch::create(schema, std::move(wrong_rows)).has_value());

  std::vector<OwnedColumnVector> wrong_type = test::batch_columns();
  wrong_type[0] = test::fixed_vector(1, test::type(schema::LogicalTypeKind::kInt64), false, 2U, {},
                                     0U, std::vector<std::byte>(16U));
  EXPECT_FALSE(OwnedColumnarBatch::create(schema, std::move(wrong_type)).has_value());
}

TEST(ColumnarBatchTest, AppliesConfiguredRowColumnAndExactBufferByteBounds) {
  const auto schema = test::batch_schema();
  const auto row_limited = OwnedColumnarBatch::create(
      schema, test::batch_columns(),
      ColumnarBatchLimits{.max_rows = 1U, .max_columns = 3U, .max_buffer_bytes = 100U});
  ASSERT_FALSE(row_limited.has_value());
  EXPECT_EQ(row_limited.error().code(), common::StatusCode::kResourceExhausted);

  const auto column_limited = OwnedColumnarBatch::create(
      schema, test::batch_columns(),
      ColumnarBatchLimits{.max_rows = 2U, .max_columns = 2U, .max_buffer_bytes = 100U});
  ASSERT_FALSE(column_limited.has_value());
  EXPECT_EQ(column_limited.error().code(), common::StatusCode::kResourceExhausted);

  const auto byte_limited = OwnedColumnarBatch::create(
      schema, test::batch_columns(),
      ColumnarBatchLimits{.max_rows = 2U, .max_columns = 3U, .max_buffer_bytes = 30U});
  ASSERT_FALSE(byte_limited.has_value());
  EXPECT_EQ(byte_limited.error().code(), common::StatusCode::kResourceExhausted);

  EXPECT_FALSE(OwnedColumnarBatch::create(
                   schema, test::batch_columns(),
                   ColumnarBatchLimits{.max_rows = 0U, .max_columns = 3U, .max_buffer_bytes = 100U})
                   .has_value());
  EXPECT_FALSE(OwnedColumnarBatch::create(
                   schema, test::batch_columns(),
                   ColumnarBatchLimits{.max_rows = 2U,
                                       .max_columns = schema::kMaximumSchemaColumnCount + 1U,
                                       .max_buffer_bytes = kMaximumV1BatchLength})
                   .has_value());

  std::vector<OwnedColumnVector> spare_capacity = test::batch_columns();
  std::vector<std::byte> timestamp_values;
  timestamp_values.reserve(128U);
  timestamp_values.resize(16U);
  spare_capacity[0] = test::fixed_vector(1, test::type(schema::LogicalTypeKind::kTimestampNs),
                                         false, 2U, {}, 0U, std::move(timestamp_values));
  const auto retained_limited =
      OwnedColumnarBatch::create(schema, std::move(spare_capacity),
                                 ColumnarBatchLimits{.max_rows = 2U,
                                                     .max_columns = 3U,
                                                     .max_buffer_bytes = 100U,
                                                     .max_retained_buffer_bytes = 100U});
  ASSERT_FALSE(retained_limited.has_value());
  EXPECT_EQ(retained_limited.error().code(), common::StatusCode::kResourceExhausted);
}

} // namespace
} // namespace chronos::columnar
