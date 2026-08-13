#include "chronos/cluster/distributed_query_tcp_execution.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::cluster::DistributedQueryTcpExecution>);
static_assert(std::is_nothrow_move_constructible_v<chronos::cluster::DistributedQueryTcpExecution>);

namespace {
[[maybe_unused]] const auto kCreateTcpExecution =
    &chronos::cluster::DistributedQueryTcpExecution::create;
using AggregateRouteResolver =
    chronos::common::Result<std::vector<chronos::cluster::DistributedQueryNodeRoute>> (*)(
        const chronos::raft::MetadataCatalogSnapshot&,
        std::span<const chronos::query::DistributedAggregateFragmentDispatch>,
        std::span<const chronos::cluster::DistributedQueryNodeTlsContext>,
        chronos::cluster::DistributedQueryRouteResolutionLimits);
using VectorRouteResolver =
    chronos::common::Result<std::vector<chronos::cluster::DistributedQueryNodeRoute>> (*)(
        const chronos::raft::MetadataCatalogSnapshot&,
        std::span<const chronos::query::DistributedVectorFragmentDispatch>,
        std::span<const chronos::cluster::DistributedQueryNodeTlsContext>,
        chronos::cluster::DistributedQueryRouteResolutionLimits);
[[maybe_unused]] const AggregateRouteResolver kResolveAggregateRoutes =
    &chronos::cluster::resolve_distributed_query_node_routes;
[[maybe_unused]] const VectorRouteResolver kResolveVectorRoutes =
    &chronos::cluster::resolve_distributed_query_node_routes;
} // namespace
