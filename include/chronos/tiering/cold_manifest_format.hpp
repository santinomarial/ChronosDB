#ifndef CHRONOS_TIERING_COLD_MANIFEST_FORMAT_HPP_
#define CHRONOS_TIERING_COLD_MANIFEST_FORMAT_HPP_

#include <array>
#include <cstddef>
#include <cstdint>

namespace chronos::tiering::cold_manifest_format {

inline constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'C'},
                                                  std::byte{'L'}, std::byte{'D'}, std::byte{'M'},
                                                  std::byte{'F'}, std::byte{'1'}};
inline constexpr std::uint16_t kFormatMajor = 1U;
inline constexpr std::uint16_t kFormatMinor = 0U;
inline constexpr std::size_t kHeaderLength = 256U;
inline constexpr std::size_t kDescriptorLength = 96U;
inline constexpr std::size_t kDescriptorCrc32cOffset = 92U;
inline constexpr std::size_t kTrailerLength = 8U;
inline constexpr std::size_t kFileCrc32cLength = 4U;
inline constexpr std::uint64_t kAlignment = 8U;
inline constexpr std::uint64_t kMaximumFileLength = 1'073'741'824ULL;
inline constexpr std::uint64_t kMaximumLocationCount = 1'048'576ULL;
inline constexpr std::uint64_t kMaximumKeyBytes = 67'108'864ULL;
inline constexpr std::size_t kMaximumObjectKeyLength = 1'024U;

inline constexpr std::size_t kFormatMajorOffset = 8U;
inline constexpr std::size_t kFormatMinorOffset = 10U;
inline constexpr std::size_t kHeaderLengthOffset = 12U;
inline constexpr std::size_t kFileFlagsOffset = 16U;
inline constexpr std::size_t kHeaderReserved0Offset = 20U;
inline constexpr std::size_t kTotalLengthOffset = 24U;
inline constexpr std::size_t kGenerationOffset = 32U;
inline constexpr std::size_t kPreviousGenerationOffset = 40U;
inline constexpr std::size_t kBaseManifestGenerationOffset = 48U;
inline constexpr std::size_t kDatabaseIdOffset = 56U;
inline constexpr std::size_t kObjectStoreIdOffset = 72U;
inline constexpr std::size_t kLocationCountOffset = 88U;
inline constexpr std::size_t kLocationsOffsetFieldOffset = 96U;
inline constexpr std::size_t kKeysOffsetFieldOffset = 104U;
inline constexpr std::size_t kKeysLengthOffset = 112U;
inline constexpr std::size_t kTrailerOffsetFieldOffset = 120U;
inline constexpr std::size_t kHeaderReserved1Offset = 128U;
inline constexpr std::size_t kHeaderCrc32cOffset = 248U;
inline constexpr std::size_t kHeaderReserved2Offset = 252U;

inline constexpr std::size_t kDescriptorPartIdOffset = 0U;
inline constexpr std::size_t kDescriptorFileLengthOffset = 16U;
inline constexpr std::size_t kDescriptorContentSha256Offset = 24U;
inline constexpr std::size_t kDescriptorKeyOffsetOffset = 56U;
inline constexpr std::size_t kDescriptorKeyLengthOffset = 64U;
inline constexpr std::size_t kDescriptorFlagsOffset = 68U;
inline constexpr std::size_t kDescriptorReservedOffset = 72U;

static_assert(kHeaderCrc32cOffset + 8U == kHeaderLength);
static_assert(kDescriptorReservedOffset + 20U == kDescriptorCrc32cOffset);
static_assert(kDescriptorCrc32cOffset + 4U == kDescriptorLength);
static_assert(kTrailerLength == kAlignment);
static_assert((kMaximumFileLength % kAlignment) == 0U);

} // namespace chronos::tiering::cold_manifest_format

#endif // CHRONOS_TIERING_COLD_MANIFEST_FORMAT_HPP_
