#include "chronos/cseg/temporal_format.hpp"

#include <gtest/gtest.h>

namespace chronos::cseg::temporal_format {
namespace {

TEST(TemporalFormatTest, FreezesVersionAndSystemColumnRegistry) {
  EXPECT_EQ(kFormatMajor, 2U);
  EXPECT_EQ(kFormatMinor, 0U);
  EXPECT_EQ(kFormatMinor, format::kFormatMinor);
  EXPECT_EQ(kMaximumLogicalIdentityBytes, 1024U);
  ASSERT_EQ(kSystemColumns.size(), 8U);
  EXPECT_EQ(kSystemColumns.front().storage_kind, kCommitSourceStorageKind);
  EXPECT_EQ(kSystemColumns.front().logical_type, schema::LogicalTypeKind::kUInt8);
  EXPECT_EQ(kSystemColumns[1].storage_kind, kSourceIdStorageKind);
  EXPECT_EQ(kSystemColumns[1].logical_type, schema::LogicalTypeKind::kUuid);
  EXPECT_EQ(kSystemColumns[4].storage_kind, kOperationStorageKind);
  EXPECT_EQ(kSystemColumns[5].storage_kind, kLogicalIdentityStorageKind);
  EXPECT_EQ(kSystemColumns[5].logical_type, schema::LogicalTypeKind::kBinary);
  EXPECT_EQ(kSystemColumns.back().storage_kind, kSystemCommitTimeStorageKind);
  EXPECT_EQ(kSystemColumns.back().logical_type, schema::LogicalTypeKind::kTimestampNs);
  EXPECT_EQ(static_cast<std::uint8_t>(CommitSource::kWal), 1U);
  EXPECT_EQ(static_cast<std::uint8_t>(CommitSource::kRaft), 2U);
  EXPECT_EQ(static_cast<std::uint8_t>(Operation::kTombstone), 4U);
}

} // namespace
} // namespace chronos::cseg::temporal_format
