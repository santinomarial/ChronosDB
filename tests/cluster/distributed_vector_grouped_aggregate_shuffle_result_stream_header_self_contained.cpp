#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_stream.hpp"

#include <type_traits>

static_assert(std::is_move_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateShuffleResultStreamSender>);
static_assert(std::is_move_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateShuffleResultStreamReceiver>);
