#include "chronos/raft/async_durable_runtime.hpp"

#include <type_traits>

[[maybe_unused]] constexpr auto kAsyncRaftBatchLimit =
    chronos::raft::AsyncDurableMultiRaftLimits{}.maximum_pending_batches;

static_assert(std::is_move_constructible_v<chronos::raft::AsyncDurableRaftCompletion>);
static_assert(!std::is_copy_constructible_v<chronos::raft::AsyncDurableRaftCompletion>);
