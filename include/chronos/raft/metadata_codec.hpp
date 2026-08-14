#ifndef CHRONOS_RAFT_METADATA_CODEC_HPP_
#define CHRONOS_RAFT_METADATA_CODEC_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/raft/metadata.hpp"

#include <cstddef>
#include <vector>

namespace chronos::raft {

inline constexpr std::size_t kMetadataCommandHeaderSize = 48U;
inline constexpr std::size_t kMetadataCommandTrailerSize = 4U;
inline constexpr std::size_t kMaximumMetadataCommandSize = std::size_t{64U} * 1024U;
inline constexpr std::uint8_t kRaftMetadataCommandEntryType = 2U;

struct MetadataCommandCodecLimits {
  std::size_t maximum_command_bytes{kMaximumMetadataCommandSize};
  std::size_t maximum_endpoint_bytes{4096U};
  std::size_t maximum_replicas{9U};
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_metadata_command_v1(MetadataCommand command, MetadataCommandCodecLimits limits = {});

[[nodiscard]] common::Result<MetadataCommand>
decode_metadata_command_v1(common::ByteView bytes, MetadataCommandCodecLimits limits = {});

} // namespace chronos::raft

#endif // CHRONOS_RAFT_METADATA_CODEC_HPP_
