#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_ack.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::cluster::DistributedVectorGroupedAggregateShuffleAckV1>);
static_assert(std::is_move_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateShuffleAckV1WriteCursor>);
