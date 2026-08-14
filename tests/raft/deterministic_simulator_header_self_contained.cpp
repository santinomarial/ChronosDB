#include "chronos/raft/deterministic_simulator.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::raft::DeterministicRaftSimulator>);
static_assert(std::is_move_constructible_v<chronos::raft::DeterministicRaftSimulator>);
static_assert(std::is_aggregate_v<chronos::raft::RaftSeededSimulationSchedule>);
