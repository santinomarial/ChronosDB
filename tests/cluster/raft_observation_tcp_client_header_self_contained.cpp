#include "chronos/cluster/raft_observation_tcp_client.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::cluster::RaftObservationTcpClient>);
static_assert(std::is_move_constructible_v<chronos::cluster::RaftObservationTcpClient>);
