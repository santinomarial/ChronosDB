#include "chronos/raft/persistent_state_budget.hpp"

#include <cstddef>

static_assert(chronos::raft::kRaftPersistentStateFixedSizeV1 == 112U);
static_assert(chronos::raft::kRaftPersistentLogEntryFixedSizeV1 == 32U);
static_assert(chronos::raft::kMaximumRaftPersistentStatePayloadSize ==
              std::size_t{16U} * 1024U * 1024U - 64U - 4U);
