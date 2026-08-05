#ifndef CHRONOS_COLUMNAR_COLUMNAR_BATCH_FORMAT_HPP_
#define CHRONOS_COLUMNAR_COLUMNAR_BATCH_FORMAT_HPP_

#include <array>
#include <cstddef>
#include <cstdint>

namespace chronos::columnar::format {

inline constexpr std::array<std::byte, 8> kMagic{std::byte{0x43}, std::byte{0x48}, std::byte{0x52},
                                                 std::byte{0x4e}, std::byte{0x43}, std::byte{0x42},
                                                 std::byte{0x31}, std::byte{0x00}};

inline constexpr std::uint16_t kFormatMajor = 1U;
inline constexpr std::uint16_t kFormatMinor = 0U;
inline constexpr std::size_t kBatchHeaderLength = 96U;
inline constexpr std::size_t kColumnDescriptorLength = 80U;
inline constexpr std::size_t kAlignment = 8U;
inline constexpr std::size_t kTerminalPaddingLength = 4U;
inline constexpr std::size_t kBatchTrailerLength = 4U;
inline constexpr std::size_t kFormatMajorOffset = 8U;
inline constexpr std::size_t kFormatMinorOffset = 10U;
inline constexpr std::size_t kHeaderLengthOffset = 12U;
inline constexpr std::size_t kBatchFlagsOffset = 16U;
inline constexpr std::size_t kRowCountOffset = 20U;
inline constexpr std::size_t kColumnCountOffset = 24U;
inline constexpr std::size_t kColumnDescriptorLengthOffset = 28U;
inline constexpr std::size_t kTotalLengthOffset = 32U;
inline constexpr std::size_t kTableIdOffset = 40U;
inline constexpr std::size_t kSchemaIdOffset = 56U;
inline constexpr std::size_t kSchemaVersionOffset = 72U;
inline constexpr std::size_t kDescriptorsOffsetFieldOffset = 80U;
inline constexpr std::size_t kHeaderCrc32cOffset = 88U;
inline constexpr std::size_t kHeaderReservedOffset = 92U;
inline constexpr std::size_t kDescriptorsOffset = kBatchHeaderLength;

inline constexpr std::size_t kColumnIdOffset = 0U;
inline constexpr std::size_t kLogicalTypeOffset = 16U;
inline constexpr std::size_t kPhysicalEncodingOffset = 18U;
inline constexpr std::size_t kTypeParameter0Offset = 20U;
inline constexpr std::size_t kTypeParameter1Offset = 22U;
inline constexpr std::size_t kColumnFlagsOffset = 24U;
inline constexpr std::size_t kNullCountOffset = 28U;
inline constexpr std::size_t kValidityOffset = 32U;
inline constexpr std::size_t kValidityLengthOffset = 40U;
inline constexpr std::size_t kOffsetsOffset = 48U;
inline constexpr std::size_t kOffsetsLengthOffset = 56U;
inline constexpr std::size_t kValuesOffset = 64U;
inline constexpr std::size_t kValuesLengthOffset = 72U;

static_assert(kValidityLengthOffset == kValidityOffset + sizeof(std::uint64_t));
static_assert(kOffsetsLengthOffset == kOffsetsOffset + sizeof(std::uint64_t));
static_assert(kValuesLengthOffset == kValuesOffset + sizeof(std::uint64_t));

inline constexpr std::uint32_t kMaximumColumnCount = 4096U;
inline constexpr std::size_t kMaximumEmbeddedBatchLength = 16'776'992U;
inline constexpr std::uint16_t kPlainPhysicalEncoding = 1U;
inline constexpr std::uint32_t kNullableColumnFlag = 1U;

} // namespace chronos::columnar::format

#endif // CHRONOS_COLUMNAR_COLUMNAR_BATCH_FORMAT_HPP_
