#include "chronos/service/replicated_distributed_query.hpp"

#include <algorithm>
#include <ranges>
#include <utility>

namespace chronos::service {

common::Result<cluster::DistributedQueryTcpExecution> create_replicated_distributed_aggregate_query(
    query::DistributedAggregatePlan plan, manifest::TemporalDatabaseStorageSnapshot snapshot,
    const ReplicatedDistributedAggregateQueryConfig& config) {
  if (config.source_node_id == 0U || config.read_barrier == nullptr ||
      config.metadata_group_id.is_nil() || config.authenticator == nullptr ||
      config.node_authorizer == nullptr) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInvalidArgument,
                       "replicated distributed query owner configuration is invalid"});
  }
  if (plan.read_policy.consistency != query::DistributedReadConsistency::kLeaderLinearizable ||
      plan.read_policy.maximum_staleness_positions.has_value()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInvalidArgument,
                       "replicated distributed query owner requires leader-linearizable policy"});
  }

  auto authority = config.read_barrier->await_authority();
  if (!authority.has_value())
    return common::make_unexpected(authority.error());
  const auto metadata_authority = std::ranges::lower_bound(
      *authority, config.metadata_group_id, {},
      [](const ReplicatedReadAuthority& value) { return value.observation.group_id; });
  if (metadata_authority == authority->end() ||
      metadata_authority->observation.group_id != config.metadata_group_id ||
      metadata_authority->observation.applied_index <
          metadata_authority->barrier.barrier.read_index ||
      config.catalog.get().applied_index < metadata_authority->barrier.barrier.read_index) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kUnavailable,
                       "replicated distributed query catalog lacks applied barrier coverage"});
  }
  if (std::ranges::any_of(config.catalog.get().tablet_group_bindings,
                          [&](const raft::TabletGroupBindingMetadata& binding) {
                            return binding.group_id == config.metadata_group_id;
                          })) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kCorruption,
                       "replicated distributed query metadata group aliases a tablet group"});
  }
  auto compatible = query::bind_group_backed_distributed_aggregate_snapshot(
      plan, std::move(snapshot),
      {.catalog = config.catalog,
       .table_id = config.table_id,
       .group_authorities = *authority,
       .destination_column_ordinals = config.destination_column_ordinals,
       .aggregate_input_index = config.aggregate_input_index,
       .event_time_predicate = config.event_time_predicate},
      config.binding_limits);
  if (!compatible.has_value())
    return common::make_unexpected(compatible.error());
  auto routes = cluster::resolve_distributed_query_node_routes(
      config.catalog.get(), compatible->dispatches(), config.tls_contexts, config.route_limits);
  if (!routes.has_value())
    return common::make_unexpected(routes.error());
  auto execution = cluster::DistributedQueryExecution::create_from_bound_snapshot(
      config.source_node_id, std::move(plan), std::move(*compatible), config.execution_limits);
  if (!execution.has_value())
    return common::make_unexpected(execution.error());
  return cluster::DistributedQueryTcpExecution::create(
      std::move(*execution), {.authenticator = config.authenticator,
                              .node_authorizer = config.node_authorizer,
                              .routes = std::move(*routes),
                              .carrier_limits = config.carrier_limits,
                              .connect_timeout = config.connect_timeout,
                              .execution_deadline = config.execution_deadline,
                              .maximum_rebindings = config.maximum_rebindings});
}

} // namespace chronos::service
