#include "chronos/common/status.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/cseg/layout.hpp"

#include <cstdint>
#include <gtest/gtest.h>
#include <vector>

namespace chronos::cseg {
namespace {

TEST(CsegLayoutTest, PlansTheExactMinimumMetadataPrefix) {
  const common::Result<CsegMetadataLayout> layout =
      plan_cseg_v1_metadata_layout({.user_column_count = 1U, .granule_count = 1U});
  ASSERT_TRUE(layout.has_value());
  EXPECT_EQ(layout->stored_column_count, 5U);
  EXPECT_EQ(layout->page_count, 5U);
  EXPECT_EQ(layout->columns_offset, 256U);
  EXPECT_EQ(layout->granules_offset, 736U);
  EXPECT_EQ(layout->pages_offset, 800U);
  EXPECT_EQ(layout->metadata_trailer_offset, 1'200U);
  EXPECT_EQ(layout->metadata_length, 1'208U);
}

TEST(CsegLayoutTest, PlansEveryPageInCanonicalGranuleMajorOrder) {
  const std::vector<std::uint64_t> page_lengths(14U, 8U);
  const common::Result<CsegFileLayout> layout =
      plan_cseg_v1_layout({.user_column_count = 3U, .granule_count = 2U}, page_lengths);
  ASSERT_TRUE(layout.has_value());
  EXPECT_EQ(layout->metadata.stored_column_count, 7U);
  EXPECT_EQ(layout->metadata.page_count, 14U);
  EXPECT_EQ(layout->metadata.columns_offset, 256U);
  EXPECT_EQ(layout->metadata.granules_offset, 928U);
  EXPECT_EQ(layout->metadata.pages_offset, 1'056U);
  EXPECT_EQ(layout->metadata.metadata_trailer_offset, 2'176U);
  EXPECT_EQ(layout->metadata.metadata_length, 2'184U);
  EXPECT_EQ(layout->total_length, 2'296U);
}

TEST(CsegLayoutTest, AlignmentIsExactAndAddsNoEmptyGap) {
  const common::Result<CsegPageLayout> first = plan_cseg_v1_page_layout(1'208U, 1U);
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(
      *first,
      (CsegPageLayout{
          .offset = 1'208U, .stored_length = 1U, .padding_length = 7U, .next_offset = 1'216U}));
  const common::Result<CsegPageLayout> aligned = plan_cseg_v1_page_layout(first->next_offset, 8U);
  ASSERT_TRUE(aligned.has_value());
  EXPECT_EQ(aligned->padding_length, 0U);
  EXPECT_EQ(aligned->next_offset, 1'224U);

  const std::vector<std::uint64_t> page_lengths{1U, 2U, 7U, 8U, 9U};
  const common::Result<CsegFileLayout> layout =
      plan_cseg_v1_layout({.user_column_count = 1U, .granule_count = 1U}, page_lengths);
  ASSERT_TRUE(layout.has_value());
  EXPECT_EQ(layout->total_length, 1'256U);
  EXPECT_EQ(layout->total_length % format::kAlignment, 0U);
}

TEST(CsegLayoutTest, PagePlacementRejectsUnalignedAndUnrepresentableRanges) {
  const auto unaligned = plan_cseg_v1_page_layout(1'209U, 1U);
  ASSERT_FALSE(unaligned.has_value());
  EXPECT_EQ(unaligned.error().code(), common::StatusCode::kInvalidArgument);

  const auto offset = plan_cseg_v1_page_layout(format::kMaximumFileLength + 8U, 1U);
  ASSERT_FALSE(offset.has_value());
  EXPECT_EQ(offset.error().code(), common::StatusCode::kResourceExhausted);

  const auto end = plan_cseg_v1_page_layout(format::kMaximumFileLength, 1U);
  ASSERT_FALSE(end.has_value());
  EXPECT_EQ(end.error().code(), common::StatusCode::kResourceExhausted);
}

TEST(CsegLayoutTest, RejectsCountsOutsideTheFrozenRegistry) {
  for (const CsegMetadataLayoutInput input :
       {CsegMetadataLayoutInput{.user_column_count = 0U, .granule_count = 1U},
        CsegMetadataLayoutInput{.user_column_count = 1U, .granule_count = 0U},
        CsegMetadataLayoutInput{.user_column_count = 4'097U, .granule_count = 1U},
        CsegMetadataLayoutInput{.user_column_count = 1U, .granule_count = 1'048'577U}}) {
    const common::Result<CsegMetadataLayout> layout = plan_cseg_v1_metadata_layout(input);
    ASSERT_FALSE(layout.has_value());
    EXPECT_EQ(layout.error().code(), common::StatusCode::kInvalidArgument);
  }
}

TEST(CsegLayoutTest, RejectsPageCountAndMetadataThatCannotFitTheFormat) {
  const common::Result<CsegMetadataLayout> page_count =
      plan_cseg_v1_metadata_layout({.user_column_count = format::kMaximumUserColumnCount,
                                    .granule_count = format::kMaximumGranuleCount});
  ASSERT_FALSE(page_count.has_value());
  EXPECT_EQ(page_count.error().code(), common::StatusCode::kResourceExhausted);

  const common::Result<CsegMetadataLayout> metadata = plan_cseg_v1_metadata_layout(
      {.user_column_count = 815U, .granule_count = format::kMaximumGranuleCount});
  ASSERT_FALSE(metadata.has_value());
  EXPECT_EQ(metadata.error().code(), common::StatusCode::kResourceExhausted);
}

TEST(CsegLayoutTest, RequiresOneBoundedStoredLengthPerCanonicalPage) {
  const std::vector<std::uint64_t> too_few(4U, 1U);
  const auto missing = plan_cseg_v1_layout({.user_column_count = 1U, .granule_count = 1U}, too_few);
  ASSERT_FALSE(missing.has_value());
  EXPECT_EQ(missing.error().code(), common::StatusCode::kInvalidArgument);

  std::vector<std::uint64_t> invalid(5U, 1U);
  invalid[2] = 0U;
  const auto zero = plan_cseg_v1_layout({.user_column_count = 1U, .granule_count = 1U}, invalid);
  ASSERT_FALSE(zero.has_value());
  EXPECT_EQ(zero.error().code(), common::StatusCode::kInvalidArgument);

  invalid[2] = format::kMaximumStoredPageLength + 1U;
  const auto oversized =
      plan_cseg_v1_layout({.user_column_count = 1U, .granule_count = 1U}, invalid);
  ASSERT_FALSE(oversized.has_value());
  EXPECT_EQ(oversized.error().code(), common::StatusCode::kResourceExhausted);
}

TEST(CsegLayoutTest, RejectsAFileWhoseAlignedPagesExceedSixtyFourGiB) {
  constexpr std::uint32_t granules = 205U;
  std::vector<std::uint64_t> page_lengths(static_cast<std::size_t>(granules) *
                                              (1U + format::kSystemColumnCount),
                                          format::kMaximumStoredPageLength);
  const auto layout =
      plan_cseg_v1_layout({.user_column_count = 1U, .granule_count = granules}, page_lengths);
  ASSERT_FALSE(layout.has_value());
  EXPECT_EQ(layout.error().code(), common::StatusCode::kResourceExhausted);
}

TEST(CsegLayoutPropertyTest, DescriptorArithmeticMatchesAnIndependentSmallGrid) {
  for (std::uint32_t users = 1U; users <= 32U; ++users) {
    for (std::uint32_t granules = 1U; granules <= 32U; ++granules) {
      const common::Result<CsegMetadataLayout> layout =
          plan_cseg_v1_metadata_layout({.user_column_count = users, .granule_count = granules});
      ASSERT_TRUE(layout.has_value());
      const std::uint64_t stored = static_cast<std::uint64_t>(users) + 4U;
      const std::uint64_t pages = stored * granules;
      EXPECT_EQ(layout->stored_column_count, stored);
      EXPECT_EQ(layout->page_count, pages);
      EXPECT_EQ(layout->granules_offset, 256ULL + 96ULL * stored);
      EXPECT_EQ(layout->pages_offset,
                256ULL + 96ULL * stored + 64ULL * static_cast<std::uint64_t>(granules));
      EXPECT_EQ(layout->metadata_trailer_offset, 256ULL + 96ULL * stored +
                                                     64ULL * static_cast<std::uint64_t>(granules) +
                                                     80ULL * pages);
      EXPECT_EQ(layout->metadata_length, layout->metadata_trailer_offset + 8U);
    }
  }
}

} // namespace
} // namespace chronos::cseg
