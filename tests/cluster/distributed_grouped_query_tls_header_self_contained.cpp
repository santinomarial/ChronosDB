#include "chronos/cluster/distributed_grouped_query_tls.hpp"

#include <type_traits>

static_assert(std::is_move_constructible_v<chronos::cluster::DistributedGroupedQueryTlsClient>);
static_assert(std::is_move_constructible_v<chronos::cluster::DistributedGroupedQueryTlsServer>);
static_assert(std::is_aggregate_v<chronos::cluster::DistributedGroupedQueryAttempt>);
