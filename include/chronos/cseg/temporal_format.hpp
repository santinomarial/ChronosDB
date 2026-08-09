#ifndef CHRONOS_CSEG_TEMPORAL_FORMAT_HPP_
#define CHRONOS_CSEG_TEMPORAL_FORMAT_HPP_

#include "chronos/cseg/format.hpp"
#include "chronos/schema/logical_type.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>

namespace chronos::cseg::temporal_format {

// CSEG v2 retains the v1 outer magic and fixed descriptor sizes, but its major version prevents a
// v1 reader from interpreting the expanded temporal system-column registry.
inline constexpr std::uint16_t kFormatMajor = 2U;
inline constexpr std::uint16_t kFormatMinor = 0U;
inline constexpr std::uint32_t kSystemColumnCount = 8U;
inline constexpr std::uint32_t kMaximumStoredColumnCount =
    format::kMaximumUserColumnCount + kSystemColumnCount;
inline constexpr std::size_t kMaximumLogicalIdentityBytes = 1024U;

inline constexpr std::uint16_t kUserStorageKind = 1U;
inline constexpr std::uint16_t kCommitSourceStorageKind = 2U;
inline constexpr std::uint16_t kSourceIdStorageKind = 3U;
inline constexpr std::uint16_t kCommitPositionStorageKind = 4U;
inline constexpr std::uint16_t kRowOrdinalStorageKind = 5U;
inline constexpr std::uint16_t kOperationStorageKind = 6U;
inline constexpr std::uint16_t kLogicalIdentityStorageKind = 7U;
inline constexpr std::uint16_t kReceiveTimeStorageKind = 8U;
inline constexpr std::uint16_t kSystemCommitTimeStorageKind = 9U;

enum class CommitSource : std::uint8_t {
  kWal = 1,
  kRaft = 2,
};

enum class Operation : std::uint8_t {
  kOriginal = 1,
  kCorrection = 2,
  kReplacement = 3,
  kTombstone = 4,
};

struct SystemColumnRegistryEntry {
  std::uint16_t storage_kind;
  schema::LogicalTypeKind logical_type;
};

inline constexpr std::array<SystemColumnRegistryEntry, kSystemColumnCount> kSystemColumns{
    SystemColumnRegistryEntry{kCommitSourceStorageKind, schema::LogicalTypeKind::kUInt8},
    SystemColumnRegistryEntry{kSourceIdStorageKind, schema::LogicalTypeKind::kUuid},
    SystemColumnRegistryEntry{kCommitPositionStorageKind, schema::LogicalTypeKind::kUInt64},
    SystemColumnRegistryEntry{kRowOrdinalStorageKind, schema::LogicalTypeKind::kUInt32},
    SystemColumnRegistryEntry{kOperationStorageKind, schema::LogicalTypeKind::kUInt8},
    SystemColumnRegistryEntry{kLogicalIdentityStorageKind, schema::LogicalTypeKind::kBinary},
    SystemColumnRegistryEntry{kReceiveTimeStorageKind, schema::LogicalTypeKind::kTimestampNs},
    SystemColumnRegistryEntry{kSystemCommitTimeStorageKind, schema::LogicalTypeKind::kTimestampNs},
};

static_assert(kFormatMajor != format::kFormatMajor);
static_assert(kSystemColumnCount > format::kSystemColumnCount);
static_assert(kMaximumStoredColumnCount <= std::numeric_limits<std::uint32_t>::max());

} // namespace chronos::cseg::temporal_format

#endif // CHRONOS_CSEG_TEMPORAL_FORMAT_HPP_
