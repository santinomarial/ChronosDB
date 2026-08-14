#include "chronos/raft/transport_codec.hpp"

static_assert(chronos::raft::kRaftTransportHeaderSize == 96U);
static_assert(chronos::raft::kMaximumRaftTransportFrameSize == std::size_t{64U} * 1024U * 1024U);
static_assert(chronos::raft::RaftTransportCodecLimits{}.maximum_entry_bytes ==
              std::size_t{16U} * 1024U * 1024U);
