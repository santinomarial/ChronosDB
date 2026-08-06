#include "chronos/manifest/format.hpp"

#include <array>
#include <gtest/gtest.h>

namespace chronos::manifest::format {
namespace {

TEST(ManifestFormatTest, FrozenHeaderAndDescriptorLocationsMatchTheSpecification) {
  EXPECT_EQ(kMagic, (std::array<std::byte, 8>{std::byte{0x43}, std::byte{0x48}, std::byte{0x52},
                                              std::byte{0x4e}, std::byte{0x4d}, std::byte{0x46},
                                              std::byte{0x53}, std::byte{0x54}}));
  EXPECT_EQ(kFormatMajor, 1U);
  EXPECT_EQ(kFormatMinor, 0U);
  EXPECT_EQ(kFileHeaderLength, 256U);
  EXPECT_EQ(kTabletDescriptorLength, 96U);
  EXPECT_EQ(kPartDescriptorLength, 128U);
  EXPECT_EQ(kRetryDescriptorLength, 128U);
  EXPECT_EQ(kHeaderCrc32cOffset, 248U);
  EXPECT_EQ(kTabletReservedOffset, 92U);
  EXPECT_EQ(kPartReservedOffset, 124U);
  EXPECT_EQ(kRetryFlagsOffset, 124U);
}

TEST(ManifestFormatTest, FrozenLimitsAndTrailerRemainAligned) {
  EXPECT_EQ(kMaximumFileLength, 1ULL * 1'024ULL * 1'024ULL * 1'024ULL);
  EXPECT_EQ(kMaximumDescriptorCount, 8'388'605U);
  EXPECT_EQ(kTrailerPaddingLength, 4U);
  EXPECT_EQ(kFileCrc32cLength, 4U);
  EXPECT_EQ(kTrailerLength, 8U);
  EXPECT_EQ(kAlignment, 8U);
}

} // namespace
} // namespace chronos::manifest::format
