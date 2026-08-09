#include "chronos/raft/durable_runtime.hpp"

[[maybe_unused]] constexpr auto kBatchLimit =
    chronos::raft::DurableMultiRaftLimits{}.maximum_batch_operations;
