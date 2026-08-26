#include "chronos/cluster/distributed_vector_grouped_aggregate_query_tls_v2.hpp"

#include <type_traits>

static_assert(std::is_move_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateQueryTlsClientV2>);
static_assert(std::is_move_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateQueryTlsServerV2>);
static_assert(
    std::is_aggregate_v<chronos::cluster::DistributedVectorGroupedAggregateQueryAttemptV2>);

namespace {
[[maybe_unused]] const auto kCreateClient =
    &chronos::cluster::DistributedVectorGroupedAggregateQueryTlsClientV2::create;
[[maybe_unused]] const auto kCreateServer =
    &chronos::cluster::DistributedVectorGroupedAggregateQueryTlsServerV2::create;
} // namespace
