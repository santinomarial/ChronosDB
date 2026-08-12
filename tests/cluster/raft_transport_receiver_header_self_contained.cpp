#include "chronos/cluster/raft_transport_receiver.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::cluster::RaftTransportAdmission>);
static_assert(std::is_move_constructible_v<chronos::cluster::RaftTransportAdmission>);
