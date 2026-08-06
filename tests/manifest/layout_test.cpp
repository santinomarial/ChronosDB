#include "chronos/common/status.hpp"
#include "chronos/manifest/format.hpp"
#include "chronos/manifest/layout.hpp"

#include <cstdint>
#include <gtest/gtest.h>

namespace chronos::manifest {
namespace {

TEST(ManifestLayoutTest, PlansTheExactEmptyGeneration) {
  const common::Result<ManifestLayout> layout = plan_manifest_v1_layout({});
  ASSERT_TRUE(layout.has_value());
  EXPECT_EQ(*layout, (ManifestLayout{.tablets_offset = 256U,
                                     .parts_offset = 256U,
                                     .retries_offset = 256U,
                                     .trailer_offset = 256U,
                                     .total_length = 264U}));
}

TEST(ManifestLayoutTest, PlansDescriptorArraysInCanonicalOrderWithoutPadding) {
  const common::Result<ManifestLayout> layout =
      plan_manifest_v1_layout({.tablet_count = 2U, .part_count = 3U, .retry_count = 4U});
  ASSERT_TRUE(layout.has_value());
  EXPECT_EQ(layout->tablets_offset, 256U);
  EXPECT_EQ(layout->parts_offset, 448U);
  EXPECT_EQ(layout->retries_offset, 832U);
  EXPECT_EQ(layout->trailer_offset, 1'344U);
  EXPECT_EQ(layout->total_length, 1'352U);
  EXPECT_EQ(layout->total_length % format::kAlignment, 0U);
}

TEST(ManifestLayoutTest, RejectsEachCountOutsideTheFrozenRegistry) {
  for (const ManifestLayoutInput input :
       {ManifestLayoutInput{.tablet_count = format::kMaximumDescriptorCount + 1U},
        ManifestLayoutInput{.part_count = format::kMaximumDescriptorCount + 1U},
        ManifestLayoutInput{.retry_count = format::kMaximumDescriptorCount + 1U}}) {
    const common::Result<ManifestLayout> layout = plan_manifest_v1_layout(input);
    ASSERT_FALSE(layout.has_value());
    EXPECT_EQ(layout.error().code(), common::StatusCode::kInvalidArgument);
  }
}

TEST(ManifestLayoutTest, RejectsACombinedLayoutBeyondOneGiB) {
  const common::Result<ManifestLayout> layout = plan_manifest_v1_layout(
      {.tablet_count = format::kMaximumDescriptorCount, .part_count = 1U, .retry_count = 0U});
  ASSERT_TRUE(layout.has_value());

  const common::Result<ManifestLayout> oversized = plan_manifest_v1_layout(
      {.tablet_count = 1U, .part_count = format::kMaximumDescriptorCount, .retry_count = 1U});
  ASSERT_FALSE(oversized.has_value());
  EXPECT_EQ(oversized.error().code(), common::StatusCode::kResourceExhausted);
}

TEST(ManifestLayoutPropertyTest, ArithmeticMatchesAnIndependentSmallGrid) {
  for (std::uint64_t tablets = 0U; tablets <= 24U; ++tablets) {
    for (std::uint64_t parts = 0U; parts <= 24U; ++parts) {
      for (std::uint64_t retries = 0U; retries <= 24U; ++retries) {
        const common::Result<ManifestLayout> layout = plan_manifest_v1_layout(
            {.tablet_count = tablets, .part_count = parts, .retry_count = retries});
        ASSERT_TRUE(layout.has_value());
        EXPECT_EQ(layout->parts_offset, 256U + tablets * 96U);
        EXPECT_EQ(layout->retries_offset, 256U + tablets * 96U + parts * 128U);
        EXPECT_EQ(layout->trailer_offset, 256U + tablets * 96U + parts * 128U + retries * 128U);
        EXPECT_EQ(layout->total_length, layout->trailer_offset + 8U);
      }
    }
  }
}

} // namespace
} // namespace chronos::manifest
