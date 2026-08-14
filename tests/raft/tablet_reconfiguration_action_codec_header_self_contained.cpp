#include "chronos/raft/tablet_reconfiguration_action_codec.hpp"

[[maybe_unused]] constexpr auto kReconfigurationActionHeader =
    chronos::raft::kTabletReconfigurationActionHeaderSize;

static_assert(chronos::raft::kMaximumTabletReconfigurationActionSize == std::size_t{128U} * 1024U);
