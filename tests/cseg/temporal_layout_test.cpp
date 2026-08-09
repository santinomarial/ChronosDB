#include "chronos/cseg/format.hpp"
#include "chronos/cseg/temporal_format.hpp"
#include "chronos/cseg/temporal_layout.hpp"

#include <cstdint>
#include <gtest/gtest.h>
#include <vector>

namespace chronos::cseg {
namespace {

TEST(TemporalLayoutTest, PlansExpandedCanonicalMetadataAndPages) {
  auto metadata =
      plan_cseg_v2_temporal_metadata_layout({.user_column_count = 1U, .granule_count = 2U});
  ASSERT_TRUE(metadata.has_value()) << metadata.error().to_string();
  EXPECT_EQ(metadata->stored_column_count, 1U + temporal_format::kSystemColumnCount);
  EXPECT_EQ(metadata->page_count, 18U);
  EXPECT_EQ(metadata->columns_offset, format::kFileHeaderLength);
  EXPECT_EQ(metadata->granules_offset,
            format::kFileHeaderLength + 9U * format::kColumnDescriptorLength);
  EXPECT_EQ(metadata->pages_offset,
            metadata->granules_offset + 2U * format::kGranuleDescriptorLength);
  EXPECT_EQ(metadata->metadata_trailer_offset,
            metadata->pages_offset + 18U * format::kPageDescriptorLength);
  EXPECT_EQ(metadata->metadata_length,
            metadata->metadata_trailer_offset + format::kMetadataTrailerLength);

  const std::vector<std::uint64_t> stored_lengths(18U, 9U);
  auto file =
      plan_cseg_v2_temporal_layout({.user_column_count = 1U, .granule_count = 2U}, stored_lengths);
  ASSERT_TRUE(file.has_value()) << file.error().to_string();
  EXPECT_EQ(file->metadata, *metadata);
  EXPECT_GT(file->total_length, metadata->metadata_length + 18U * 9U);
  EXPECT_EQ(file->total_length % format::kAlignment, 0U);
}

TEST(TemporalLayoutTest, RejectsV1PageCountAndZeroDimensions) {
  EXPECT_EQ(plan_cseg_v2_temporal_metadata_layout({}).error().code(),
            common::StatusCode::kInvalidArgument);
  const std::vector<std::uint64_t> v1_page_count(format::kSystemColumnCount + 1U, 8U);
  EXPECT_EQ(
      plan_cseg_v2_temporal_layout({.user_column_count = 1U, .granule_count = 1U}, v1_page_count)
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::cseg
