#include "chronos/live/raft_subscription_reclaimer.hpp"

#include <type_traits>

static_assert(std::is_move_constructible_v<chronos::live::RaftSubscriptionSourceReclaimer>);
