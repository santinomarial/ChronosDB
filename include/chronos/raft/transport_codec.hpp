#ifndef CHRONOS_RAFT_TRANSPORT_CODEC_HPP_
#define CHRONOS_RAFT_TRANSPORT_CODEC_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/raft/multi_raft.hpp"

#include <cstddef>
#include <vector>

namespace chronos::raft {

inline constexpr std::size_t kRaftTransportHeaderSize = 96U;
inline constexpr std::size_t kRaftTransportTrailerSize = 4U;
inline constexpr std::size_t kMaximumRaftTransportFrameSize = 64U * 1024U * 1024U;

struct RaftTransportCodecLimits {
  std::size_t maximum_frame_bytes{kMaximumRaftTransportFrameSize};
  std::size_t maximum_append_entries{1024U};
  std::size_t maximum_entry_bytes{16U * 1024U * 1024U};
  std::size_t maximum_snapshot_voters{31U};
};

struct RaftTransportEnvelope {
  GroupId group_id;
  NodeId source{};
  NodeId destination{};
  Message message;

  friend bool operator==(const RaftTransportEnvelope&, const RaftTransportEnvelope&) = default;
};

// Canonical group-scoped Raft bytes for an already authenticated cluster carrier. CRC32C detects
// accidental damage but does not authenticate the claimed source. The receiver must authorize the
// transport principal for source and exact-match destination before calling MultiRaftRuntime.
[[nodiscard]] common::Result<std::vector<std::byte>>
encode_raft_transport_envelope_v1(const RaftTransportEnvelope& envelope,
                                  RaftTransportCodecLimits limits = {});

[[nodiscard]] common::Result<RaftTransportEnvelope>
decode_raft_transport_envelope_v1(common::ByteView bytes, RaftTransportCodecLimits limits = {});

} // namespace chronos::raft

#endif // CHRONOS_RAFT_TRANSPORT_CODEC_HPP_
