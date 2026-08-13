#include "chronos/cluster/raft_observation_tls_server.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::cluster::RaftObservationTlsServer>);
static_assert(std::is_move_constructible_v<chronos::cluster::RaftObservationTlsServer>);
