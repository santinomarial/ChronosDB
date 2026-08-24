#include "chronos/cluster/distributed_mutable_vector_query_transport.hpp"

#include <type_traits>

static_assert(std::is_move_constructible_v<chronos::cluster::DistributedMutableVectorQuerySender>);
static_assert(std::is_abstract_v<chronos::cluster::DistributedMutableVectorQueryWorkerService>);
