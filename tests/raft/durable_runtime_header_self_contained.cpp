#include "chronos/raft/durable_runtime.hpp"

#include <type_traits>

[[maybe_unused]] constexpr auto kBatchLimit =
    chronos::raft::DurableMultiRaftLimits{}.maximum_batch_operations;

static_assert(std::is_move_constructible_v<chronos::raft::DurableMultiRaftRuntime>);
static_assert(!std::is_copy_constructible_v<chronos::raft::DurableMultiRaftRuntime>);
