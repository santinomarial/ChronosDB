#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_tcp_execution.hpp"

#include <type_traits>

static_assert(std::is_move_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateShuffleTcpExecution>);
static_assert(!std::is_copy_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateShuffleTcpExecution>);
