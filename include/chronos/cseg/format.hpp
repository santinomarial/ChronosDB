#ifndef CHRONOS_CSEG_FORMAT_HPP_
#define CHRONOS_CSEG_FORMAT_HPP_

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>

namespace chronos::cseg::format {

inline constexpr std::array<std::byte, 8> kMagic{
    std::byte{0x43}, std::byte{0x48}, std::byte{0x52}, std::byte{0x4e},
    std::byte{0x43}, std::byte{0x53}, std::byte{0x45}, std::byte{0x47},
};

inline constexpr std::uint16_t kFormatMajor = 1U;
inline constexpr std::uint16_t kFormatMinor = 0U;
inline constexpr std::size_t kFileHeaderLength = 256U;
inline constexpr std::size_t kColumnDescriptorLength = 96U;
inline constexpr std::size_t kGranuleDescriptorLength = 64U;
inline constexpr std::size_t kPageDescriptorLength = 80U;
inline constexpr std::size_t kMetadataTrailerPaddingLength = 4U;
inline constexpr std::size_t kMetadataCrc32cLength = 4U;
inline constexpr std::size_t kMetadataTrailerLength =
    kMetadataTrailerPaddingLength + kMetadataCrc32cLength;
inline constexpr std::uint64_t kAlignment = 8U;

inline constexpr std::uint64_t kMaximumFileLength = 68'719'476'736ULL;
inline constexpr std::uint32_t kMaximumUserColumnCount = 4'096U;
inline constexpr std::uint32_t kSystemColumnCount = 4U;
inline constexpr std::uint32_t kMaximumStoredColumnCount =
    kMaximumUserColumnCount + kSystemColumnCount;
inline constexpr std::uint32_t kMaximumGranuleRowCount = 65'536U;
inline constexpr std::uint32_t kMaximumGranuleCount = 1'048'576U;
inline constexpr std::uint64_t kMaximumUncompressedPageLength = 67'108'864U;
inline constexpr std::uint64_t kMaximumStoredPageLength = 67'108'864U;
inline constexpr std::uint64_t kMaximumZstdWindowSize = 67'108'864U;
inline constexpr std::uint64_t kMaximumRowCount = 68'719'476'736ULL;
inline constexpr std::uint32_t kMaximumPageCount = std::numeric_limits<std::uint32_t>::max();

inline constexpr std::size_t kFormatMajorOffset = 8U;
inline constexpr std::size_t kFormatMinorOffset = 10U;
inline constexpr std::size_t kHeaderLengthOffset = 12U;
inline constexpr std::size_t kFileFlagsOffset = 16U;
inline constexpr std::size_t kHeaderReserved0Offset = 20U;
inline constexpr std::size_t kTotalLengthOffset = 24U;
inline constexpr std::size_t kMetadataLengthOffset = 32U;
inline constexpr std::size_t kRowCountOffset = 40U;
inline constexpr std::size_t kUserColumnCountOffset = 48U;
inline constexpr std::size_t kStoredColumnCountOffset = 52U;
inline constexpr std::size_t kGranuleCountOffset = 56U;
inline constexpr std::size_t kPageCountOffset = 60U;
inline constexpr std::size_t kPartIdOffset = 64U;
inline constexpr std::size_t kTableIdOffset = 80U;
inline constexpr std::size_t kTabletIdOffset = 96U;
inline constexpr std::size_t kSchemaIdOffset = 112U;
inline constexpr std::size_t kSchemaVersionOffset = 128U;
inline constexpr std::size_t kColumnsOffsetFieldOffset = 136U;
inline constexpr std::size_t kGranulesOffsetFieldOffset = 144U;
inline constexpr std::size_t kPagesOffsetFieldOffset = 152U;
inline constexpr std::size_t kPageDataOffsetFieldOffset = 160U;
inline constexpr std::size_t kEventTimeColumnOrdinalOffset = 168U;
inline constexpr std::size_t kOrderingColumnCountOffset = 172U;
inline constexpr std::size_t kMinimumEventTimeOffset = 176U;
inline constexpr std::size_t kMaximumEventTimeOffset = 184U;
inline constexpr std::size_t kHeaderReserved1Offset = 192U;
inline constexpr std::size_t kHeaderCrc32cOffset = 248U;
inline constexpr std::size_t kHeaderReserved2Offset = 252U;
inline constexpr std::uint64_t kColumnsOffset = kFileHeaderLength;

inline constexpr std::size_t kColumnIdOffset = 0U;
inline constexpr std::size_t kStorageKindOffset = 16U;
inline constexpr std::size_t kLogicalTypeOffset = 18U;
inline constexpr std::size_t kTypeParameter0Offset = 20U;
inline constexpr std::size_t kTypeParameter1Offset = 22U;
inline constexpr std::size_t kColumnFlagsOffset = 24U;
inline constexpr std::size_t kSchemaOrdinalOffset = 28U;
inline constexpr std::size_t kOrderingOrdinalOffset = 32U;
inline constexpr std::size_t kColumnReservedOffset = 36U;

inline constexpr std::uint16_t kUserStorageKind = 1U;
inline constexpr std::uint16_t kWalIdStorageKind = 2U;
inline constexpr std::uint16_t kRecordSequenceStorageKind = 3U;
inline constexpr std::uint16_t kRowOrdinalStorageKind = 4U;
inline constexpr std::uint16_t kOperationStorageKind = 5U;
inline constexpr std::uint32_t kNullableColumnFlag = 1U << 0U;
inline constexpr std::uint32_t kEventTimeColumnFlag = 1U << 1U;
inline constexpr std::uint32_t kPhysicalOrderingColumnFlag = 1U << 2U;
inline constexpr std::uint32_t kAbsentOrdinal = std::numeric_limits<std::uint32_t>::max();

inline constexpr std::size_t kGranuleFirstRowOffset = 0U;
inline constexpr std::size_t kGranuleRowCountOffset = 8U;
inline constexpr std::size_t kGranulePageCountOffset = 12U;
inline constexpr std::size_t kGranuleFirstPageIndexOffset = 16U;
inline constexpr std::size_t kGranuleMinimumEventTimeOffset = 24U;
inline constexpr std::size_t kGranuleMaximumEventTimeOffset = 32U;
inline constexpr std::size_t kGranuleReservedOffset = 40U;

inline constexpr std::size_t kPageGranuleOrdinalOffset = 0U;
inline constexpr std::size_t kPageStoredColumnOrdinalOffset = 4U;
inline constexpr std::size_t kPagePhysicalEncodingOffset = 8U;
inline constexpr std::size_t kPageCompressionOffset = 10U;
inline constexpr std::size_t kPageFlagsOffset = 12U;
inline constexpr std::size_t kPageRowCountOffset = 16U;
inline constexpr std::size_t kPageNullCountOffset = 20U;
inline constexpr std::size_t kPageOffsetFieldOffset = 24U;
inline constexpr std::size_t kPageStoredLengthOffset = 32U;
inline constexpr std::size_t kPageUncompressedLengthOffset = 40U;
inline constexpr std::size_t kPageValidityLengthOffset = 48U;
inline constexpr std::size_t kPageOffsetsLengthOffset = 56U;
inline constexpr std::size_t kPageValuesLengthOffset = 64U;
inline constexpr std::size_t kPageCrc32cOffset = 72U;
inline constexpr std::size_t kPageReservedOffset = 76U;

inline constexpr std::uint16_t kPlainPhysicalEncoding = 1U;
inline constexpr std::uint16_t kNoCompression = 1U;
inline constexpr std::uint16_t kZstdCompression = 2U;
inline constexpr std::uint8_t kAppendRowsOperation = 1U;

static_assert(kHeaderCrc32cOffset + sizeof(std::uint32_t) == kHeaderReserved2Offset);
static_assert(kHeaderReserved2Offset + sizeof(std::uint32_t) == kFileHeaderLength);
static_assert(kColumnReservedOffset + 60U == kColumnDescriptorLength);
static_assert(kGranuleReservedOffset + 24U == kGranuleDescriptorLength);
static_assert(kPageReservedOffset + sizeof(std::uint32_t) == kPageDescriptorLength);
static_assert(kMetadataTrailerLength == kAlignment);
static_assert((kMaximumFileLength % kAlignment) == 0U);

} // namespace chronos::cseg::format

#endif // CHRONOS_CSEG_FORMAT_HPP_
