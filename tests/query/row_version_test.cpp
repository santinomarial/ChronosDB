#include "chronos/common/status.hpp"
#include "chronos/query/row_version.hpp"

#include <cstddef>
#include <gtest/gtest.h>
#include <limits>

namespace chronos::query {
namespace {

TEST(VectorRowVersionLayoutTest, FreezesSuffixOrderTypesAndCheckedColumnCounts) {
  const auto layout = vector_row_version_layout(7U).value();
  EXPECT_EQ(layout.first_column_ordinal(), 7U);
  EXPECT_EQ(layout.wal_id_column_ordinal(), 7U);
  EXPECT_EQ(layout.record_sequence_column_ordinal(), 8U);
  EXPECT_EQ(layout.row_ordinal_column_ordinal(), 9U);
  EXPECT_EQ(layout.operation_column_ordinal(), 10U);
  EXPECT_EQ(layout.total_column_count(), 11U);
  EXPECT_EQ(scan_output_column_count(7U, RowVersionScanMode::kOmit).value(), 7U);
  EXPECT_EQ(scan_output_column_count(7U, RowVersionScanMode::kAppend).value(), 11U);

  EXPECT_EQ(vector_row_version_column_type(VectorRowVersionColumnKind::kWalId)->kind(),
            schema::LogicalTypeKind::kUuid);
  EXPECT_EQ(vector_row_version_column_type(VectorRowVersionColumnKind::kRecordSequence)->kind(),
            schema::LogicalTypeKind::kUInt64);
  EXPECT_EQ(vector_row_version_column_type(VectorRowVersionColumnKind::kRowOrdinal)->kind(),
            schema::LogicalTypeKind::kUInt32);
  EXPECT_EQ(vector_row_version_column_type(VectorRowVersionColumnKind::kOperation)->kind(),
            schema::LogicalTypeKind::kUInt8);

  EXPECT_EQ(vector_row_version_layout(std::numeric_limits<std::size_t>::max()).error().code(),
            common::StatusCode::kResourceExhausted);
  // Deliberately exercises defensive validation for values crossing an unsafe integration edge.
  // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
  const auto invalid_mode = static_cast<RowVersionScanMode>(255U);
  // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
  const auto invalid_kind = static_cast<VectorRowVersionColumnKind>(255U);
  EXPECT_EQ(scan_output_column_count(1U, invalid_mode).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(vector_row_version_column_type(invalid_kind).error().code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::query
