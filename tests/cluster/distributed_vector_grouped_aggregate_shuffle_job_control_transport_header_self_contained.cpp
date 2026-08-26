#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_control_transport.hpp"

#include <type_traits>

static_assert(std::is_move_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateShuffleJobControlRequestReader>);
static_assert(
    std::is_move_constructible_v<
        chronos::cluster::DistributedVectorGroupedAggregateShuffleJobControlRequestWriteCursor>);
static_assert(
    std::is_move_constructible_v<
        chronos::cluster::DistributedVectorGroupedAggregateShuffleJobControlResponseWriteCursor>);
