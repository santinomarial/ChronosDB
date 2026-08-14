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
  EXPECT_GT(file->total_length, metadata->metadata_length + 18ULL * 9ULL);
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

TEST(TemporalLayoutPropertyTest, DescriptorArithmeticMatchesAnIndependentSmallGrid) {
  for (std::uint32_t user_columns = 1U; user_columns <= 32U; ++user_columns) {
    for (std::uint32_t granules = 1U; granules <= 32U; ++granules) {
      const auto layout = plan_cseg_v2_temporal_metadata_layout(
          {.user_column_count = user_columns, .granule_count = granules});
      ASSERT_TRUE(layout.has_value()) << layout.error().to_string();

      const std::uint64_t stored_columns =
          static_cast<std::uint64_t>(user_columns) + temporal_format::kSystemColumnCount;
      const std::uint64_t page_count = stored_columns * granules;
      const std::uint64_t granules_offset =
          format::kFileHeaderLength + stored_columns * format::kColumnDescriptorLength;
      const std::uint64_t pages_offset =
          granules_offset + static_cast<std::uint64_t>(granules) * format::kGranuleDescriptorLength;
      const std::uint64_t trailer_offset =
          pages_offset + page_count * format::kPageDescriptorLength;

      EXPECT_EQ(layout->stored_column_count, stored_columns);
      EXPECT_EQ(layout->page_count, page_count);
      EXPECT_EQ(layout->granules_offset, granules_offset);
      EXPECT_EQ(layout->pages_offset, pages_offset);
      EXPECT_EQ(layout->metadata_trailer_offset, trailer_offset);
      EXPECT_EQ(layout->metadata_length, trailer_offset + format::kMetadataTrailerLength);
    }
  }
}

} // namespace
} // namespace chronos::cseg
