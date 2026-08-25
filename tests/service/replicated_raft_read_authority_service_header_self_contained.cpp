#include "chronos/service/replicated_raft_read_authority_service.hpp"

#include <type_traits>

static_assert(std::is_base_of_v<chronos::cluster::RaftReadAuthorityService,
                                chronos::service::ReplicatedRaftReadAuthorityService>);
