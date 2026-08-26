#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_control_tcp_client.hpp"

#include <type_traits>

static_assert(!std::is_default_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateShuffleJobControlTcpClient>);
static_assert(!std::is_copy_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateShuffleJobControlTcpClient>);
static_assert(std::is_nothrow_move_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateShuffleJobControlTcpClient>);
