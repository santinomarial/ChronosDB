#include "chronos/raft/node.hpp"

#include <cstddef>

namespace {
[[maybe_unused]] constexpr auto kHeaderIsSelfContained = chronos::raft::Role::kLeader;
}

static_assert(chronos::raft::RaftLimits{}.maximum_entry_bytes == std::size_t{16U} * 1024U * 1024U);
