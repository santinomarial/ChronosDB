#include "chronos/raft/durable_tablet_reconfiguration.hpp"

#include <type_traits>

static_assert(!std::is_aggregate_v<chronos::raft::PreparedTabletReconfigurationDispatch>);
static_assert(
    !std::is_default_constructible_v<chronos::raft::PreparedTabletReconfigurationDispatch>);
static_assert(!std::is_copy_constructible_v<chronos::raft::PreparedTabletReconfigurationDispatch>);
static_assert(std::is_move_constructible_v<chronos::raft::PreparedTabletReconfigurationDispatch>);
static_assert(std::is_move_assignable_v<chronos::raft::PreparedTabletReconfigurationDispatch>);
