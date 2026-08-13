#include "chronos/cluster/distributed_vector_aggregate_query_tls_v2.hpp"

#include <type_traits>

static_assert(
    std::is_move_constructible_v<chronos::cluster::DistributedVectorAggregateQueryTlsClientV2>);
static_assert(
    std::is_move_constructible_v<chronos::cluster::DistributedVectorAggregateQueryTlsServerV2>);
static_assert(std::is_aggregate_v<chronos::cluster::DistributedVectorAggregateQueryAttemptV2>);

namespace {
[[maybe_unused]] const auto kCreateClient =
    &chronos::cluster::DistributedVectorAggregateQueryTlsClientV2::create;
[[maybe_unused]] const auto kCreateServer =
    &chronos::cluster::DistributedVectorAggregateQueryTlsServerV2::create;
} // namespace
