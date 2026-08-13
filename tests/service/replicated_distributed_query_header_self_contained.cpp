#include "chronos/service/replicated_distributed_query.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::service::ReplicatedDistributedAggregateQueryConfig>);

namespace {
[[maybe_unused]] const auto kCreateReplicatedDistributedAggregateQuery =
    &chronos::service::create_replicated_distributed_aggregate_query;
} // namespace
