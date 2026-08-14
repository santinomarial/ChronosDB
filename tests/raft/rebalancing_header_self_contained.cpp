#include "chronos/raft/rebalancing.hpp"

#include <cstddef>

namespace {
[[maybe_unused]] constexpr auto kHeaderIsSelfContained =
    chronos::raft::TabletMovementPhase::kComplete;
}

static_assert(chronos::raft::TabletMovementLimits{}.maximum_chunk_bytes ==
              std::size_t{4U} * 1024U * 1024U);
