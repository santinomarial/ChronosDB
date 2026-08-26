#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_tls.hpp"

#include <type_traits>

static_assert(std::is_move_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateShuffleResultTlsClient>);
static_assert(std::is_move_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateShuffleResultTlsServer>);
