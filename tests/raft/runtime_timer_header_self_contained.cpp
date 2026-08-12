#include "chronos/raft/runtime_timer.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::raft::RaftTimerRuntime>);
static_assert(std::is_move_constructible_v<chronos::raft::RaftTimerRuntime>);
