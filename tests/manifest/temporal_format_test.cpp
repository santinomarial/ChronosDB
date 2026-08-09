#include "chronos/manifest/format.hpp"
#include "chronos/manifest/temporal_format.hpp"

#include <gtest/gtest.h>

namespace chronos::manifest::temporal_format {
namespace {

TEST(TemporalManifestFormatTest, FreezesVersionDescriptorSizesAndSourceFields) {
  EXPECT_EQ(kFormatMajor, 2U);
  EXPECT_EQ(kFormatMinor, 0U);
  EXPECT_EQ(kTabletDescriptorLength, 128U);
  EXPECT_EQ(kPartDescriptorLength, 224U);
  EXPECT_EQ(kRetryDescriptorLength, 144U);
  EXPECT_EQ(kTabletCommitSourceOffset, 112U);
  EXPECT_EQ(kPartContentSha256Offset, 152U);
  EXPECT_EQ(kPartCsegFormatMajorOffset, 184U);
  EXPECT_EQ(kRetryCommitSourceOffset, 124U);
  EXPECT_EQ(kHasWalReclaimCheckpointFlag, 1U);
  EXPECT_EQ(format::kFileHeaderLength, 256U);
}

} // namespace
} // namespace chronos::manifest::temporal_format
