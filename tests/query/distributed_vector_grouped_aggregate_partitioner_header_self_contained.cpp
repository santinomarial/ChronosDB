#include "chronos/query/distributed_vector_grouped_aggregate_partitioner.hpp"

namespace {
[[maybe_unused]] constexpr auto kMaximumPartitions =
    chronos::query::kMaximumDistributedVectorGroupedAggregatePartitions;
}
