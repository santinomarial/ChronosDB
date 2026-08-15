#include "chronos/raft/persistent_log.hpp"

[[maybe_unused]] chronos::raft::RaftPersistentLog raft_log;

static_assert(chronos::raft::kDefaultRaftSegmentTargetSize == std::uint64_t{64U} * 1024U * 1024U);
