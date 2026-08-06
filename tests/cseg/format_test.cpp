#include "chronos/cseg/format.hpp"

#include <gtest/gtest.h>

namespace chronos::cseg::format {
namespace {

TEST(CsegFormatTest, FrozenHeaderAndDescriptorLocationsMatchTheSpecification) {
  EXPECT_EQ(kMagic, (std::array<std::byte, 8>{std::byte{0x43}, std::byte{0x48}, std::byte{0x52},
                                              std::byte{0x4e}, std::byte{0x43}, std::byte{0x53},
                                              std::byte{0x45}, std::byte{0x47}}));
  EXPECT_EQ(kFormatMajor, 1U);
  EXPECT_EQ(kFormatMinor, 0U);
  EXPECT_EQ(kFileHeaderLength, 256U);
  EXPECT_EQ(kColumnDescriptorLength, 96U);
  EXPECT_EQ(kGranuleDescriptorLength, 64U);
  EXPECT_EQ(kPageDescriptorLength, 80U);
  EXPECT_EQ(kHeaderCrc32cOffset, 248U);
  EXPECT_EQ(kColumnReservedOffset, 36U);
  EXPECT_EQ(kGranuleReservedOffset, 40U);
  EXPECT_EQ(kPageCrc32cOffset, 72U);
}

TEST(CsegFormatTest, FrozenRegistriesAndLimitsRemainDistinct) {
  EXPECT_EQ(kSystemColumnCount, 4U);
  EXPECT_EQ(kMaximumUserColumnCount, 4'096U);
  EXPECT_EQ(kMaximumStoredColumnCount, 4'100U);
  EXPECT_EQ(kMaximumGranuleRowCount, 65'536U);
  EXPECT_EQ(kMaximumGranuleCount, 1'048'576U);
  EXPECT_EQ(kMaximumUncompressedPageLength, 64U * 1'024U * 1'024U);
  EXPECT_EQ(kMaximumStoredPageLength, 64U * 1'024U * 1'024U);
  EXPECT_EQ(kMaximumFileLength, 64ULL * 1'024ULL * 1'024ULL * 1'024ULL);
  EXPECT_EQ(kUserStorageKind, 1U);
  EXPECT_EQ(kWalIdStorageKind, 2U);
  EXPECT_EQ(kRecordSequenceStorageKind, 3U);
  EXPECT_EQ(kRowOrdinalStorageKind, 4U);
  EXPECT_EQ(kOperationStorageKind, 5U);
  EXPECT_EQ(kPlainPhysicalEncoding, 1U);
  EXPECT_EQ(kNoCompression, 1U);
  EXPECT_EQ(kZstdCompression, 2U);
  EXPECT_EQ(kAppendRowsOperation, 1U);
}

} // namespace
} // namespace chronos::cseg::format
