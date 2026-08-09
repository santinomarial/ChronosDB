#ifndef CHRONOS_MANIFEST_TEMPORAL_FORMAT_HPP_
#define CHRONOS_MANIFEST_TEMPORAL_FORMAT_HPP_

#include "chronos/manifest/format.hpp"

#include <cstddef>
#include <cstdint>

namespace chronos::manifest::temporal_format {

inline constexpr std::uint16_t kFormatMajor = 2U;
inline constexpr std::uint16_t kFormatMinor = 0U;
inline constexpr std::size_t kTabletDescriptorLength = 128U;
inline constexpr std::size_t kPartDescriptorLength = 224U;
inline constexpr std::size_t kRetryDescriptorLength = 144U;
inline constexpr std::uint32_t kHasWalReclaimCheckpointFlag = 1U << 0U;

inline constexpr std::size_t kTabletTableIdOffset = 0U;
inline constexpr std::size_t kTabletIdOffset = 16U;
inline constexpr std::size_t kTabletRecoverySchemaIdOffset = 32U;
inline constexpr std::size_t kTabletRecoverySchemaVersionOffset = 48U;
inline constexpr std::size_t kTabletSourceIdOffset = 56U;
inline constexpr std::size_t kTabletDurablePositionOffset = 72U;
inline constexpr std::size_t kTabletReclaimPositionOffset = 80U;
inline constexpr std::size_t kTabletFirstPartIndexOffset = 88U;
inline constexpr std::size_t kTabletPartCountOffset = 96U;
inline constexpr std::size_t kTabletDurableVersionCountOffset = 104U;
inline constexpr std::size_t kTabletCommitSourceOffset = 112U;
inline constexpr std::size_t kTabletReserved0Offset = 113U;
inline constexpr std::size_t kTabletFlagsOffset = 116U;
inline constexpr std::size_t kTabletReserved1Offset = 120U;

inline constexpr std::size_t kPartIdOffset = 0U;
inline constexpr std::size_t kPartTableIdOffset = 16U;
inline constexpr std::size_t kPartTabletIdOffset = 32U;
inline constexpr std::size_t kPartSchemaIdOffset = 48U;
inline constexpr std::size_t kPartSchemaVersionOffset = 64U;
inline constexpr std::size_t kPartFileLengthOffset = 72U;
inline constexpr std::size_t kPartRowCountOffset = 80U;
inline constexpr std::size_t kPartMinimumCommitPositionOffset = 88U;
inline constexpr std::size_t kPartMaximumCommitPositionOffset = 96U;
inline constexpr std::size_t kPartMinimumEventTimeOffset = 104U;
inline constexpr std::size_t kPartMaximumEventTimeOffset = 112U;
inline constexpr std::size_t kPartMinimumSystemTimeOffset = 120U;
inline constexpr std::size_t kPartMaximumSystemTimeOffset = 128U;
inline constexpr std::size_t kPartSourceIdOffset = 136U;
inline constexpr std::size_t kPartContentSha256Offset = 152U;
inline constexpr std::size_t kPartCsegFormatMajorOffset = 184U;
inline constexpr std::size_t kPartCsegFormatMinorOffset = 186U;
inline constexpr std::size_t kPartCommitSourceOffset = 188U;
inline constexpr std::size_t kPartReserved0Offset = 189U;
inline constexpr std::size_t kPartFlagsOffset = 192U;
inline constexpr std::size_t kPartReserved1Offset = 196U;

inline constexpr std::size_t kRetryClientIdOffset = 0U;
inline constexpr std::size_t kRetryClientBatchIdOffset = 16U;
inline constexpr std::size_t kRetryTableIdOffset = 32U;
inline constexpr std::size_t kRetryTabletIdOffset = 48U;
inline constexpr std::size_t kRetryRequestDigestOffset = 64U;
inline constexpr std::size_t kRetrySourceIdOffset = 96U;
inline constexpr std::size_t kRetryCommitPositionOffset = 112U;
inline constexpr std::size_t kRetryAppliedRowCountOffset = 120U;
inline constexpr std::size_t kRetryCommitSourceOffset = 124U;
inline constexpr std::size_t kRetryReserved0Offset = 125U;
inline constexpr std::size_t kRetryFlagsOffset = 128U;
inline constexpr std::size_t kRetryReserved1Offset = 132U;

static_assert(kFormatMajor != format::kFormatMajor);
static_assert(kTabletReserved1Offset + 8U == kTabletDescriptorLength);
static_assert(kPartReserved1Offset + 28U == kPartDescriptorLength);
static_assert(kRetryReserved1Offset + 12U == kRetryDescriptorLength);
static_assert(kTabletDescriptorLength % format::kAlignment == 0U);
static_assert(kPartDescriptorLength % format::kAlignment == 0U);
static_assert(kRetryDescriptorLength % format::kAlignment == 0U);

} // namespace chronos::manifest::temporal_format

#endif // CHRONOS_MANIFEST_TEMPORAL_FORMAT_HPP_
