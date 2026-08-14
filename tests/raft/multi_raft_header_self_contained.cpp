#include "chronos/raft/multi_raft.hpp"

#include <optional>
#include <type_traits>

namespace {
[[maybe_unused]] constexpr auto kHeaderIsSelfContained =
    &chronos::raft::MultiRaftLimits::maximum_groups;
}

static_assert(std::is_same_v<decltype(chronos::raft::MultiRaftTransition{}.read_barrier_ready),
                             std::optional<chronos::raft::GroupReadBarrier>>);
