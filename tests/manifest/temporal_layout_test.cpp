#include "chronos/common/status.hpp"
#include "chronos/manifest/format.hpp"
#include "chronos/manifest/temporal_layout.hpp"

#include <cstdint>
#include <gtest/gtest.h>

namespace chronos::manifest {
namespace {

TEST(TemporalManifestLayoutTest, PlansExpandedCanonicalDescriptorTables) {
  const auto empty = plan_manifest_v2_temporal_layout({});
  ASSERT_TRUE(empty.has_value());
  EXPECT_EQ(empty->total_length, 264U);

  const auto layout =
      plan_manifest_v2_temporal_layout({.tablet_count = 2U, .part_count = 3U, .retry_count = 4U});
  ASSERT_TRUE(layout.has_value());
  EXPECT_EQ(layout->tablets_offset, 256U);
  EXPECT_EQ(layout->parts_offset, 512U);
  EXPECT_EQ(layout->retries_offset, 1'184U);
  EXPECT_EQ(layout->trailer_offset, 1'760U);
  EXPECT_EQ(layout->total_length, 1'768U);
  EXPECT_EQ(layout->total_length % format::kAlignment, 0U);
}

TEST(TemporalManifestLayoutTest, MapsEachCountToItsCanonicalDescriptorLength) {
  const auto tablets = plan_manifest_v2_temporal_layout({.tablet_count = 1U});
  const auto parts = plan_manifest_v2_temporal_layout({.part_count = 1U});
  const auto retries = plan_manifest_v2_temporal_layout({.retry_count = 1U});
  ASSERT_TRUE(tablets.has_value());
  ASSERT_TRUE(parts.has_value());
  ASSERT_TRUE(retries.has_value());
  EXPECT_EQ(tablets->total_length, 392U);
  EXPECT_EQ(parts->total_length, 488U);
  EXPECT_EQ(retries->total_length, 408U);
}

TEST(TemporalManifestLayoutTest, RejectsCountsAndCombinedBytesOutsideBounds) {
  const auto count =
      plan_manifest_v2_temporal_layout({.tablet_count = format::kMaximumDescriptorCount + 1U});
  ASSERT_FALSE(count.has_value());
  EXPECT_EQ(count.error().code(), common::StatusCode::kInvalidArgument);

  const auto bytes = plan_manifest_v2_temporal_layout(
      {.tablet_count = 1U, .part_count = format::kMaximumDescriptorCount, .retry_count = 1U});
  ASSERT_FALSE(bytes.has_value());
  EXPECT_EQ(bytes.error().code(), common::StatusCode::kResourceExhausted);
}

TEST(TemporalManifestLayoutPropertyTest, ArithmeticMatchesIndependentSmallGrid) {
  for (std::uint64_t tablets = 0U; tablets <= 8U; ++tablets) {
    for (std::uint64_t parts = 0U; parts <= 8U; ++parts) {
      for (std::uint64_t retries = 0U; retries <= 8U; ++retries) {
        const auto layout = plan_manifest_v2_temporal_layout(
            {.tablet_count = tablets, .part_count = parts, .retry_count = retries});
        ASSERT_TRUE(layout.has_value());
        EXPECT_EQ(layout->parts_offset, 256U + tablets * 128U);
        EXPECT_EQ(layout->retries_offset, 256U + tablets * 128U + parts * 224U);
        EXPECT_EQ(layout->trailer_offset, 256U + tablets * 128U + parts * 224U + retries * 144U);
        EXPECT_EQ(layout->total_length, layout->trailer_offset + 8U);
      }
    }
  }
}

} // namespace
} // namespace chronos::manifest
