#include "chronos/raft/runtime_timer_driver.hpp"

#include <type_traits>

static_assert(std::is_abstract_v<chronos::raft::RaftElectionDeadlineSource>);
static_assert(!std::is_copy_constructible_v<chronos::raft::RaftTimerDriver>);
static_assert(std::is_move_constructible_v<chronos::raft::RaftTimerDriver>);
