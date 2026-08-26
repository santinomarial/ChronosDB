#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_ack.hpp"

#include <type_traits>

static_assert(std::is_move_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor>);
static_assert(std::is_move_assignable_v<
              chronos::cluster::DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor>);
