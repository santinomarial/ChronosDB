#ifndef CHRONOS_RAFT_MULTIPLEXED_LOG_HPP_
#define CHRONOS_RAFT_MULTIPLEXED_LOG_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/raft/multi_raft.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace chronos::raft {

inline constexpr std::uint16_t kMultiplexedLogFormatMajor = 1U;
inline constexpr std::uint16_t kMultiplexedLogFormatMinor = 1U;
inline constexpr std::size_t kMultiplexedLogHeaderSize = 64U;
inline constexpr std::size_t kMultiplexedLogTrailerSize = 4U;
inline constexpr std::size_t kMaximumMultiplexedLogRecordSize = std::size_t{16U} * 1024U * 1024U;

struct DecodedGroupPersistentState {
  GroupPersistentState persistent;
  std::size_t encoded_size{};
};

struct MultiplexedLogRecordHeader {
  std::size_t encoded_size{};
  std::size_t payload_size{};
  std::uint64_t physical_sequence{};
  GroupId group_id;
  std::uint16_t format_minor{};
};

// Validates the complete fixed header, including its checksum, before returning allocation-driving
// lengths. The input must contain exactly kMultiplexedLogHeaderSize bytes.
[[nodiscard]] common::Result<MultiplexedLogRecordHeader>
inspect_multiplexed_log_record_header_v1(common::ByteView encoded_header);

// Encodes a full per-group persistence checkpoint into one checksummed node-level physical record.
// Repeating records for different groups permits shared batching/fsync without sharing logical
// indexes. The byte format is independent of native object layout.
[[nodiscard]] common::Result<std::vector<std::byte>>
encode_multiplexed_log_record_v1(const GroupPersistentState& persistent);

[[nodiscard]] common::Result<DecodedGroupPersistentState>
decode_multiplexed_log_record_v1(common::ByteView encoded);

} // namespace chronos::raft

#endif // CHRONOS_RAFT_MULTIPLEXED_LOG_HPP_
