#include "chronos/raft/rebalancing.hpp"

namespace {
[[maybe_unused]] constexpr auto kHeaderIsSelfContained =
    chronos::raft::TabletMovementPhase::kComplete;
}
