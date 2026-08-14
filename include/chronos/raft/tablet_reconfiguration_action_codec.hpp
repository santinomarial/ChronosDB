#ifndef CHRONOS_RAFT_TABLET_RECONFIGURATION_ACTION_CODEC_HPP_
#define CHRONOS_RAFT_TABLET_RECONFIGURATION_ACTION_CODEC_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/raft/tablet_reconfiguration.hpp"

#include <cstddef>
#include <vector>

namespace chronos::raft {

inline constexpr std::size_t kTabletReconfigurationActionHeaderSize = 96U;
inline constexpr std::size_t kTabletReconfigurationActionTrailerSize = 4U;
inline constexpr std::size_t kMaximumTabletReconfigurationActionSize = std::size_t{128U} * 1024U;

struct TabletReconfigurationActionCodecLimits {
  std::size_t maximum_action_bytes{kMaximumTabletReconfigurationActionSize};
  std::size_t maximum_voters{31U};
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_tablet_reconfiguration_action_v1(const TabletReconfigurationAction& action,
                                        TabletReconfigurationActionCodecLimits limits = {});

[[nodiscard]] common::Result<TabletReconfigurationAction>
decode_tablet_reconfiguration_action_v1(common::ByteView bytes,
                                        TabletReconfigurationActionCodecLimits limits = {});

} // namespace chronos::raft

#endif // CHRONOS_RAFT_TABLET_RECONFIGURATION_ACTION_CODEC_HPP_
