#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_stream.hpp"

#include <type_traits>

static_assert(std::is_move_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateShuffleStreamReceiver>);
static_assert(std::is_move_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateShuffleStreamSender>);
static_assert(
    std::is_aggregate_v<chronos::cluster::DistributedVectorGroupedAggregateShuffleCompleteStream>);
