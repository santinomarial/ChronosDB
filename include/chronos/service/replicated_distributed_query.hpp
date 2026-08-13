#ifndef CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_QUERY_HPP_
#define CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_QUERY_HPP_

#include "chronos/cluster/distributed_query_execution.hpp"
#include "chronos/cluster/distributed_query_tcp_execution.hpp"
#include "chronos/common/result.hpp"
#include "chronos/manifest/temporal_publication.hpp"
#include "chronos/query/distributed.hpp"
#include "chronos/query/distributed_fragment_binding.hpp"
#include "chronos/service/replicated_read_barrier.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <span>

namespace chronos::service {

struct ReplicatedDistributedAggregateQueryConfig {
  raft::NodeId source_node_id{};
  ReplicatedReadBarrier* read_barrier{};
  raft::GroupId metadata_group_id;
  std::reference_wrapper<const raft::MetadataCatalogSnapshot> catalog;
  schema::TableId table_id;
  std::span<const std::uint32_t> destination_column_ordinals;
  std::uint32_t aggregate_input_index{};
  std::optional<cseg::EventTimePredicate> event_time_predicate;
  std::span<const cluster::DistributedQueryNodeTlsContext> tls_contexts;
  network::ConnectionAuthenticator* authenticator{};
  const cluster::ClusterNodePrincipalAuthorizer* node_authorizer{};
  query::DistributedAggregateSnapshotBindingLimits binding_limits;
  cluster::DistributedQueryRouteResolutionLimits route_limits;
  cluster::DistributedQueryExecutionLimits execution_limits;
  cluster::DistributedQueryTlsClientLimits carrier_limits;
  std::chrono::milliseconds connect_timeout{5000};
  std::optional<std::chrono::steady_clock::time_point> execution_deadline;
  std::size_t maximum_rebindings{3U};
};

// Synchronously acquires exact leader-linearizable authority, requires the catalog to cover the
// metadata-group barrier, binds the Manifest snapshot, resolves authenticated routes, and returns
// the single-threaded TCP lifecycle owner. Borrowed authentication/TLS policy must outlive it.
[[nodiscard]] common::Result<cluster::DistributedQueryTcpExecution>
create_replicated_distributed_aggregate_query(
    query::DistributedAggregatePlan plan, manifest::TemporalDatabaseStorageSnapshot snapshot,
    const ReplicatedDistributedAggregateQueryConfig& config);

} // namespace chronos::service

#endif // CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_QUERY_HPP_
