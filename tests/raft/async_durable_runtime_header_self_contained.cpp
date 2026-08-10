#include "chronos/raft/async_durable_runtime.hpp"

[[maybe_unused]] constexpr auto kAsyncRaftBatchLimit =
    chronos::raft::AsyncDurableMultiRaftLimits{}.maximum_pending_batches;
