#include "chronos/cluster/raft_read_authority_transport.hpp"

#include <type_traits>

static_assert(std::is_abstract_v<chronos::cluster::RaftReadAuthorityService>);
static_assert(!std::is_default_constructible_v<chronos::cluster::RaftReadAuthorityReceiver>);
