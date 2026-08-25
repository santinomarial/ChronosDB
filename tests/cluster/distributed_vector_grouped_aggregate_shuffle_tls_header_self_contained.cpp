#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_tls.hpp"

#include <type_traits>

static_assert(std::is_move_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateShuffleTlsClient>);
static_assert(std::is_move_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateShuffleTlsServer>);
