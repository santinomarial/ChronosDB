#ifndef CHRONOS_MANIFEST_FORMAT_HPP_
#define CHRONOS_MANIFEST_FORMAT_HPP_

#include <array>
#include <cstddef>
#include <cstdint>

namespace chronos::manifest::format {

inline constexpr std::array<std::byte, 8> kMagic{
    std::byte{0x43}, std::byte{0x48}, std::byte{0x52}, std::byte{0x4e},
    std::byte{0x4d}, std::byte{0x46}, std::byte{0x53}, std::byte{0x54},
};

inline constexpr std::uint16_t kFormatMajor = 1U;
inline constexpr std::uint16_t kFormatMinor = 0U;
inline constexpr std::size_t kFileHeaderLength = 256U;
inline constexpr std::size_t kTabletDescriptorLength = 96U;
inline constexpr std::size_t kPartDescriptorLength = 128U;
inline constexpr std::size_t kRetryDescriptorLength = 128U;
inline constexpr std::size_t kTrailerPaddingLength = 4U;
inline constexpr std::size_t kFileCrc32cLength = 4U;
inline constexpr std::size_t kTrailerLength = kTrailerPaddingLength + kFileCrc32cLength;
inline constexpr std::uint64_t kAlignment = 8U;

inline constexpr std::uint64_t kMaximumFileLength = 1'073'741'824ULL;
inline constexpr std::uint64_t kMaximumDescriptorCount = 8'388'605ULL;

inline constexpr std::size_t kFormatMajorOffset = 8U;
inline constexpr std::size_t kFormatMinorOffset = 10U;
inline constexpr std::size_t kHeaderLengthOffset = 12U;
inline constexpr std::size_t kFileFlagsOffset = 16U;
inline constexpr std::size_t kHeaderReserved0Offset = 20U;
inline constexpr std::size_t kTotalLengthOffset = 24U;
inline constexpr std::size_t kGenerationOffset = 32U;
inline constexpr std::size_t kPreviousGenerationOffset = 40U;
inline constexpr std::size_t kTabletCountOffset = 48U;
inline constexpr std::size_t kPartCountOffset = 56U;
inline constexpr std::size_t kRetryCountOffset = 64U;
inline constexpr std::size_t kDatabaseIdOffset = 72U;
inline constexpr std::size_t kWalIdOffset = 88U;
inline constexpr std::size_t kReclaimRecordSequenceOffset = 104U;
inline constexpr std::size_t kReclaimSegmentNumberOffset = 112U;
inline constexpr std::size_t kReclaimByteOffsetOffset = 120U;
inline constexpr std::size_t kTabletsOffsetFieldOffset = 128U;
inline constexpr std::size_t kPartsOffsetFieldOffset = 136U;
inline constexpr std::size_t kRetriesOffsetFieldOffset = 144U;
inline constexpr std::size_t kTrailerOffsetFieldOffset = 152U;
inline constexpr std::size_t kHeaderReserved1Offset = 160U;
inline constexpr std::size_t kHeaderCrc32cOffset = 248U;
inline constexpr std::size_t kHeaderReserved2Offset = 252U;
inline constexpr std::uint64_t kTabletsOffset = kFileHeaderLength;

inline constexpr std::size_t kTabletTableIdOffset = 0U;
inline constexpr std::size_t kTabletIdOffset = 16U;
inline constexpr std::size_t kTabletRecoverySchemaIdOffset = 32U;
inline constexpr std::size_t kTabletRecoverySchemaVersionOffset = 48U;
inline constexpr std::size_t kTabletDurableRecordSequenceOffset = 56U;
inline constexpr std::size_t kTabletFirstPartIndexOffset = 64U;
inline constexpr std::size_t kTabletPartCountOffset = 72U;
inline constexpr std::size_t kTabletDurableRowCountOffset = 80U;
inline constexpr std::size_t kTabletFlagsOffset = 88U;
inline constexpr std::size_t kTabletReservedOffset = 92U;

inline constexpr std::size_t kPartIdOffset = 0U;
inline constexpr std::size_t kPartTableIdOffset = 16U;
inline constexpr std::size_t kPartTabletIdOffset = 32U;
inline constexpr std::size_t kPartSchemaIdOffset = 48U;
inline constexpr std::size_t kPartSchemaVersionOffset = 64U;
inline constexpr std::size_t kPartFileLengthOffset = 72U;
inline constexpr std::size_t kPartRowCountOffset = 80U;
inline constexpr std::size_t kPartMinimumRecordSequenceOffset = 88U;
inline constexpr std::size_t kPartMaximumRecordSequenceOffset = 96U;
inline constexpr std::size_t kPartMinimumEventTimeOffset = 104U;
inline constexpr std::size_t kPartMaximumEventTimeOffset = 112U;
inline constexpr std::size_t kPartFlagsOffset = 120U;
inline constexpr std::size_t kPartReservedOffset = 124U;

inline constexpr std::size_t kRetryClientIdOffset = 0U;
inline constexpr std::size_t kRetryClientBatchIdOffset = 16U;
inline constexpr std::size_t kRetryTableIdOffset = 32U;
inline constexpr std::size_t kRetryTabletIdOffset = 48U;
inline constexpr std::size_t kRetryRequestDigestOffset = 64U;
inline constexpr std::size_t kRetryWalIdOffset = 96U;
inline constexpr std::size_t kRetryRecordSequenceOffset = 112U;
inline constexpr std::size_t kRetryAppliedRowCountOffset = 120U;
inline constexpr std::size_t kRetryFlagsOffset = 124U;

static_assert(kHeaderCrc32cOffset + sizeof(std::uint32_t) == kHeaderReserved2Offset);
static_assert(kHeaderReserved2Offset + sizeof(std::uint32_t) == kFileHeaderLength);
static_assert(kTabletReservedOffset + sizeof(std::uint32_t) == kTabletDescriptorLength);
static_assert(kPartReservedOffset + sizeof(std::uint32_t) == kPartDescriptorLength);
static_assert(kRetryFlagsOffset + sizeof(std::uint32_t) == kRetryDescriptorLength);
static_assert(kTrailerLength == kAlignment);
static_assert((kMaximumFileLength % kAlignment) == 0U);

} // namespace chronos::manifest::format

#endif // CHRONOS_MANIFEST_FORMAT_HPP_
