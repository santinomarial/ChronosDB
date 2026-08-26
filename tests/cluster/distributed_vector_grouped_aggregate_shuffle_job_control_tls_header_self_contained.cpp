#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_control_tls.hpp"

#include <type_traits>

static_assert(std::is_move_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateShuffleJobControlTlsClient>);
static_assert(std::is_move_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateShuffleJobControlTlsServer>);
