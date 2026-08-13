#include "chronos/service/replicated_distributed_query.hpp"

#include <algorithm>
#include <climits>
#include <memory>
#include <new>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::service {
namespace {

template <typename Config> [[nodiscard]] common::Status validate_config(const Config& config) {
  if (config.source_node_id == 0U || config.read_barrier == nullptr ||
      config.metadata_group_id.is_nil() || config.authenticator == nullptr ||
      config.node_authorizer == nullptr) {
    return {common::StatusCode::kInvalidArgument,
            "replicated distributed query owner configuration is invalid"};
  }
  return common::Status::ok();
}

template <typename Config>
[[nodiscard]] common::Result<std::vector<ReplicatedReadAuthority>>
acquire_catalog_authority(const Config& config) {
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
  return authority;
}

[[nodiscard]] common::Result<std::reference_wrapper<const schema::TableSchema>>
resolve_active_schema(const raft::MetadataCatalogSnapshot& catalog,
                      const schema::TableId& table_id) {
  const auto active = std::ranges::lower_bound(catalog.active_schemas, table_id, {},
                                               &raft::ActiveSchemaMetadata::table_id);
  if (active == catalog.active_schemas.end() || active->table_id != table_id) {
    return common::make_unexpected(common::Status{common::StatusCode::kUnavailable,
                                                  "replicated grouped query has no active schema"});
  }
  const auto definition =
      std::ranges::lower_bound(catalog.schema_definitions, active->schema_id, {},
                               [](const auto& value) { return value.schema->schema_id(); });
  if (definition == catalog.schema_definitions.end() ||
      definition->schema->schema_id() != active->schema_id ||
      definition->schema->table_id() != table_id) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kCorruption,
                       "replicated grouped query active schema definition is inconsistent"});
  }
  return std::cref(*definition->schema);
}

[[nodiscard]] common::Result<cluster::DistributedQueryTcpExecution>
create_tcp_execution(query::DistributedAggregatePlan plan,
                     query::CompatibleDistributedAggregateSnapshot compatible,
                     const ReplicatedDistributedAggregateQueryConfig& config) {
  auto routes = cluster::resolve_distributed_query_node_routes(
      config.catalog.get(), compatible.dispatches(), config.tls_contexts, config.route_limits);
  if (!routes.has_value())
    return common::make_unexpected(routes.error());
  auto execution = cluster::DistributedQueryExecution::create_from_bound_snapshot(
      config.source_node_id, std::move(plan), std::move(compatible), config.execution_limits);
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

[[nodiscard]] common::Result<cluster::DistributedGroupedQueryTcpExecution>
create_grouped_tcp_execution(query::CompatibleDistributedAggregateSnapshot compatible,
                             const ReplicatedDistributedGroupedFloat64QueryConfig& config) {
  auto routes = cluster::resolve_distributed_query_node_routes(
      config.catalog.get(), compatible.dispatches(), config.tls_contexts, config.route_limits);
  if (!routes.has_value())
    return common::make_unexpected(routes.error());
  auto destination_schema = resolve_active_schema(config.catalog.get(), config.table_id);
  if (!destination_schema.has_value())
    return common::make_unexpected(destination_schema.error());
  auto grouped = query::bind_compatible_distributed_grouped_float64_snapshot(
      std::move(compatible), destination_schema->get(), config.group_key_input_index);
  if (!grouped.has_value())
    return common::make_unexpected(grouped.error());
  auto execution = cluster::DistributedGroupedQueryExecution::create(
      config.source_node_id, std::move(*grouped), config.execution_limits);
  if (!execution.has_value())
    return common::make_unexpected(execution.error());
  return cluster::DistributedGroupedQueryTcpExecution::create(
      std::move(*execution), {.authenticator = config.authenticator,
                              .node_authorizer = config.node_authorizer,
                              .routes = std::move(*routes),
                              .carrier_limits = config.carrier_limits,
                              .connect_timeout = config.connect_timeout,
                              .execution_deadline = config.execution_deadline,
                              .maximum_rebindings = config.maximum_rebindings});
}

} // namespace

common::Result<cluster::DistributedQueryTcpExecution> create_replicated_distributed_aggregate_query(
    query::DistributedAggregatePlan plan, manifest::TemporalDatabaseStorageSnapshot snapshot,
    const ReplicatedDistributedAggregateQueryConfig& config) {
  const common::Status config_status = validate_config(config);
  if (!config_status.is_ok())
    return common::make_unexpected(config_status);
  if (plan.read_policy.consistency != query::DistributedReadConsistency::kLeaderLinearizable ||
      plan.read_policy.maximum_staleness_positions.has_value()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInvalidArgument,
                       "replicated distributed query owner requires leader-linearizable policy"});
  }

  auto authority = acquire_catalog_authority(config);
  if (!authority.has_value())
    return common::make_unexpected(authority.error());
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
  return create_tcp_execution(std::move(plan), std::move(*compatible), config);
}

common::Result<cluster::DistributedGroupedQueryTcpExecution>
create_replicated_distributed_grouped_float64_query(
    query::DistributedAggregatePlan plan, manifest::TemporalDatabaseStorageSnapshot snapshot,
    const ReplicatedDistributedGroupedFloat64QueryConfig& config) {
  const common::Status config_status = validate_config(config);
  if (!config_status.is_ok())
    return common::make_unexpected(config_status);
  if (plan.read_policy.consistency != query::DistributedReadConsistency::kLeaderLinearizable ||
      plan.read_policy.maximum_staleness_positions.has_value()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInvalidArgument,
                       "replicated grouped query requires leader-linearizable policy"});
  }

  auto authority = acquire_catalog_authority(config);
  if (!authority.has_value())
    return common::make_unexpected(authority.error());
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
  return create_grouped_tcp_execution(std::move(*compatible), config);
}

common::Result<cluster::DistributedGroupedQueryTcpExecution>
create_replicated_follower_distributed_grouped_float64_query(
    query::DistributedAggregatePlan plan, manifest::TemporalDatabaseStorageSnapshot snapshot,
    const std::span<const query::DistributedAggregateFollowerReadAuthority> follower_authorities,
    const ReplicatedDistributedGroupedFloat64QueryConfig& config) {
  const common::Status config_status = validate_config(config);
  if (!config_status.is_ok())
    return common::make_unexpected(config_status);
  if (plan.read_policy.consistency != query::DistributedReadConsistency::kFollowerBoundedStale ||
      !plan.read_policy.maximum_staleness_positions.has_value()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInvalidArgument,
                       "replicated follower grouped query requires bounded-stale policy"});
  }

  auto metadata_authority = acquire_catalog_authority(config);
  if (!metadata_authority.has_value())
    return common::make_unexpected(metadata_authority.error());
  auto compatible = query::bind_follower_group_backed_distributed_aggregate_snapshot(
      plan, std::move(snapshot),
      {.catalog = config.catalog,
       .table_id = config.table_id,
       .group_authorities = follower_authorities,
       .destination_column_ordinals = config.destination_column_ordinals,
       .aggregate_input_index = config.aggregate_input_index,
       .event_time_predicate = config.event_time_predicate},
      config.binding_limits);
  if (!compatible.has_value())
    return common::make_unexpected(compatible.error());
  return create_grouped_tcp_execution(std::move(*compatible), config);
}

common::Result<cluster::DistributedQueryTcpExecution>
create_replicated_follower_distributed_aggregate_query(
    query::DistributedAggregatePlan plan, manifest::TemporalDatabaseStorageSnapshot snapshot,
    const std::span<const query::DistributedAggregateFollowerReadAuthority> follower_authorities,
    const ReplicatedDistributedAggregateQueryConfig& config) {
  const common::Status config_status = validate_config(config);
  if (!config_status.is_ok())
    return common::make_unexpected(config_status);
  if (plan.read_policy.consistency != query::DistributedReadConsistency::kFollowerBoundedStale ||
      !plan.read_policy.maximum_staleness_positions.has_value()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInvalidArgument,
                       "replicated follower query owner requires bounded-stale policy"});
  }
  auto metadata_authority = acquire_catalog_authority(config);
  if (!metadata_authority.has_value())
    return common::make_unexpected(metadata_authority.error());
  auto compatible = query::bind_follower_group_backed_distributed_aggregate_snapshot(
      plan, std::move(snapshot),
      {.catalog = config.catalog,
       .table_id = config.table_id,
       .group_authorities = follower_authorities,
       .destination_column_ordinals = config.destination_column_ordinals,
       .aggregate_input_index = config.aggregate_input_index,
       .event_time_predicate = config.event_time_predicate},
      config.binding_limits);
  if (!compatible.has_value())
    return common::make_unexpected(compatible.error());
  return create_tcp_execution(std::move(plan), std::move(*compatible), config);
}

class ReplicatedFollowerDistributedAggregateQuery::Impl {
public:
  Impl(query::DistributedAggregatePlan owned_plan,
       manifest::TemporalDatabaseStorageSnapshot owned_snapshot,
       ReplicatedDistributedAggregateQueryConfig configured,
       cluster::RaftObservationTcpBatchAcquisition acquisition) noexcept
      : plan(std::move(owned_plan)), snapshot(std::move(owned_snapshot)), config(configured),
        authority(std::move(acquisition)) {}

  [[nodiscard]] common::Status fail(common::Status failure) {
    if (authority.state() == cluster::RaftObservationTcpBatchAcquisitionState::kRunning)
      static_cast<void>(authority.cancel());
    if (execution.has_value() &&
        execution->state() == cluster::DistributedQueryTcpExecutionState::kRunning) {
      static_cast<void>(execution->cancel());
    }
    owner_failure = std::move(failure);
    owner_state = ReplicatedFollowerDistributedAggregateQueryState::kFailed;
    return owner_failure;
  }

  query::DistributedAggregatePlan plan;
  manifest::TemporalDatabaseStorageSnapshot snapshot;
  ReplicatedDistributedAggregateQueryConfig config;
  cluster::RaftObservationTcpBatchAcquisition authority;
  std::optional<cluster::DistributedQueryTcpExecution> execution;
  ReplicatedFollowerDistributedAggregateQueryState owner_state{
      ReplicatedFollowerDistributedAggregateQueryState::kAcquiringAuthority};
  common::Status owner_failure{common::StatusCode::kInternal,
                               "replicated follower query has not failed"};
};

ReplicatedFollowerDistributedAggregateQuery::
    ReplicatedFollowerDistributedAggregateQuery() noexcept = default;
ReplicatedFollowerDistributedAggregateQuery::ReplicatedFollowerDistributedAggregateQuery(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
ReplicatedFollowerDistributedAggregateQuery::~ReplicatedFollowerDistributedAggregateQuery() =
    default;
ReplicatedFollowerDistributedAggregateQuery::ReplicatedFollowerDistributedAggregateQuery(
    ReplicatedFollowerDistributedAggregateQuery&&) noexcept = default;
ReplicatedFollowerDistributedAggregateQuery& ReplicatedFollowerDistributedAggregateQuery::operator=(
    ReplicatedFollowerDistributedAggregateQuery&&) noexcept = default;

common::Result<ReplicatedFollowerDistributedAggregateQuery>
ReplicatedFollowerDistributedAggregateQuery::create(
    query::DistributedAggregatePlan plan, manifest::TemporalDatabaseStorageSnapshot snapshot,
    cluster::RaftObservationTcpBatchConstructionConfig authority_config,
    ReplicatedDistributedAggregateQueryConfig query_config) {
  const common::Status query_status = validate_config(query_config);
  if (!query_status.is_ok())
    return common::make_unexpected(query_status);
  if (authority_config.source_node_id != query_config.source_node_id ||
      authority_config.authenticator != query_config.authenticator ||
      authority_config.node_authorizer != query_config.node_authorizer) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInvalidArgument,
                       "replicated follower query authority policy differs from execution"});
  }
  auto batch_config = cluster::construct_raft_observation_tcp_batch(
      plan, query_config.catalog.get(), authority_config);
  if (!batch_config.has_value())
    return common::make_unexpected(batch_config.error());
  auto authority = cluster::RaftObservationTcpBatchAcquisition::create(std::move(*batch_config));
  if (!authority.has_value())
    return common::make_unexpected(authority.error());
  try {
    return ReplicatedFollowerDistributedAggregateQuery{std::make_unique<Impl>(
        std::move(plan), std::move(snapshot), query_config, std::move(*authority))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "replicated follower query owner allocation failed"});
  }
}

common::Status ReplicatedFollowerDistributedAggregateQuery::poll_once(
    const std::chrono::milliseconds maximum_wait) {
  if (!implementation_)
    return {common::StatusCode::kInvalidArgument, "replicated follower query owner is empty"};
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX)
    return {common::StatusCode::kInvalidArgument, "replicated follower query poll wait is invalid"};
  Impl& impl = *implementation_;
  if (impl.owner_state == ReplicatedFollowerDistributedAggregateQueryState::kFailed ||
      impl.owner_state == ReplicatedFollowerDistributedAggregateQueryState::kCancelled) {
    return impl.owner_failure;
  }
  if (impl.owner_state == ReplicatedFollowerDistributedAggregateQueryState::kComplete)
    return common::Status::ok();
  if (impl.owner_state == ReplicatedFollowerDistributedAggregateQueryState::kAcquiringAuthority) {
    const common::Status progress = impl.authority.poll_once(maximum_wait);
    if (!progress.is_ok())
      return impl.fail(progress);
    if (impl.authority.state() != cluster::RaftObservationTcpBatchAcquisitionState::kComplete)
      return common::Status::ok();
    auto authorities = impl.authority.result();
    if (!authorities.has_value())
      return impl.fail(authorities.error());
    auto execution = create_replicated_follower_distributed_aggregate_query(
        std::move(impl.plan), std::move(impl.snapshot), *authorities, impl.config);
    if (!execution.has_value())
      return impl.fail(execution.error());
    impl.execution.emplace(std::move(*execution));
    impl.owner_state = ReplicatedFollowerDistributedAggregateQueryState::kExecuting;
    return common::Status::ok();
  }
  const common::Status progress = impl.execution->poll_once(maximum_wait);
  const auto state = impl.execution->state();
  if (state == cluster::DistributedQueryTcpExecutionState::kComplete) {
    impl.owner_state = ReplicatedFollowerDistributedAggregateQueryState::kComplete;
    return common::Status::ok();
  }
  if (state == cluster::DistributedQueryTcpExecutionState::kFailed)
    return impl.fail(impl.execution->failure());
  if (state == cluster::DistributedQueryTcpExecutionState::kCancelled) {
    impl.owner_failure = impl.execution->failure();
    impl.owner_state = ReplicatedFollowerDistributedAggregateQueryState::kCancelled;
    return impl.owner_failure;
  }
  return progress;
}

common::Status ReplicatedFollowerDistributedAggregateQuery::cancel() {
  if (!implementation_)
    return {common::StatusCode::kInvalidArgument, "replicated follower query owner is empty"};
  Impl& impl = *implementation_;
  if (impl.owner_state == ReplicatedFollowerDistributedAggregateQueryState::kFailed ||
      impl.owner_state == ReplicatedFollowerDistributedAggregateQueryState::kCancelled) {
    return impl.owner_failure;
  }
  if (impl.owner_state == ReplicatedFollowerDistributedAggregateQueryState::kComplete) {
    return {common::StatusCode::kInvalidArgument,
            "completed replicated follower query cannot be cancelled"};
  }
  if (impl.owner_state == ReplicatedFollowerDistributedAggregateQueryState::kAcquiringAuthority)
    static_cast<void>(impl.authority.cancel());
  else
    static_cast<void>(impl.execution->cancel());
  impl.owner_failure = {common::StatusCode::kCancelled,
                        "replicated follower distributed query was cancelled"};
  impl.owner_state = ReplicatedFollowerDistributedAggregateQueryState::kCancelled;
  return impl.owner_failure;
}

ReplicatedFollowerDistributedAggregateQueryState
ReplicatedFollowerDistributedAggregateQuery::state() const noexcept {
  return implementation_ ? implementation_->owner_state
                         : ReplicatedFollowerDistributedAggregateQueryState::kFailed;
}

ReplicatedFollowerDistributedAggregateQueryMetrics
ReplicatedFollowerDistributedAggregateQuery::metrics() const noexcept {
  if (!implementation_)
    return {};
  return {.authority = implementation_->authority.metrics(),
          .execution = implementation_->execution.has_value()
                           ? std::optional{implementation_->execution->metrics()}
                           : std::nullopt};
}

common::Result<query::MergeableAggregateState>
ReplicatedFollowerDistributedAggregateQuery::result() const {
  if (!implementation_)
    return common::make_unexpected(common::Status{common::StatusCode::kInvalidArgument,
                                                  "replicated follower query owner is empty"});
  if (implementation_->owner_state == ReplicatedFollowerDistributedAggregateQueryState::kFailed ||
      implementation_->owner_state ==
          ReplicatedFollowerDistributedAggregateQueryState::kCancelled) {
    return common::make_unexpected(implementation_->owner_failure);
  }
  if (implementation_->owner_state != ReplicatedFollowerDistributedAggregateQueryState::kComplete)
    return common::make_unexpected(common::Status{
        common::StatusCode::kInvalidArgument, "replicated follower query result is unavailable"});
  return implementation_->execution->result();
}

const common::Status& ReplicatedFollowerDistributedAggregateQuery::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "replicated follower query owner is empty"};
  return implementation_ ? implementation_->owner_failure : empty;
}

class ReplicatedFollowerDistributedGroupedFloat64Query::Impl {
public:
  Impl(query::DistributedAggregatePlan owned_plan,
       manifest::TemporalDatabaseStorageSnapshot owned_snapshot,
       ReplicatedDistributedGroupedFloat64QueryConfig configured,
       cluster::RaftObservationTcpBatchAcquisition acquisition) noexcept
      : plan(std::move(owned_plan)), snapshot(std::move(owned_snapshot)), config(configured),
        authority(std::move(acquisition)) {}

  [[nodiscard]] common::Status fail(common::Status failure) {
    if (authority.state() == cluster::RaftObservationTcpBatchAcquisitionState::kRunning)
      static_cast<void>(authority.cancel());
    if (execution.has_value() &&
        execution->state() == cluster::DistributedGroupedQueryTcpExecutionState::kRunning) {
      static_cast<void>(execution->cancel());
    }
    owner_failure = std::move(failure);
    owner_state = ReplicatedFollowerDistributedGroupedFloat64QueryState::kFailed;
    return owner_failure;
  }

  query::DistributedAggregatePlan plan;
  manifest::TemporalDatabaseStorageSnapshot snapshot;
  ReplicatedDistributedGroupedFloat64QueryConfig config;
  cluster::RaftObservationTcpBatchAcquisition authority;
  std::optional<cluster::DistributedGroupedQueryTcpExecution> execution;
  ReplicatedFollowerDistributedGroupedFloat64QueryState owner_state{
      ReplicatedFollowerDistributedGroupedFloat64QueryState::kAcquiringAuthority};
  common::Status owner_failure{common::StatusCode::kInternal,
                               "replicated follower grouped query has not failed"};
};

ReplicatedFollowerDistributedGroupedFloat64Query::
    ReplicatedFollowerDistributedGroupedFloat64Query() noexcept = default;
ReplicatedFollowerDistributedGroupedFloat64Query::ReplicatedFollowerDistributedGroupedFloat64Query(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
ReplicatedFollowerDistributedGroupedFloat64Query::
    ~ReplicatedFollowerDistributedGroupedFloat64Query() = default;
ReplicatedFollowerDistributedGroupedFloat64Query::ReplicatedFollowerDistributedGroupedFloat64Query(
    ReplicatedFollowerDistributedGroupedFloat64Query&&) noexcept = default;
ReplicatedFollowerDistributedGroupedFloat64Query&
ReplicatedFollowerDistributedGroupedFloat64Query::operator=(
    ReplicatedFollowerDistributedGroupedFloat64Query&&) noexcept = default;

common::Result<ReplicatedFollowerDistributedGroupedFloat64Query>
ReplicatedFollowerDistributedGroupedFloat64Query::create(
    query::DistributedAggregatePlan plan, manifest::TemporalDatabaseStorageSnapshot snapshot,
    cluster::RaftObservationTcpBatchConstructionConfig authority_config,
    ReplicatedDistributedGroupedFloat64QueryConfig query_config) {
  const common::Status query_status = validate_config(query_config);
  if (!query_status.is_ok())
    return common::make_unexpected(query_status);
  if (plan.read_policy.consistency != query::DistributedReadConsistency::kFollowerBoundedStale ||
      !plan.read_policy.maximum_staleness_positions.has_value() ||
      authority_config.source_node_id != query_config.source_node_id ||
      authority_config.authenticator != query_config.authenticator ||
      authority_config.node_authorizer != query_config.node_authorizer) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInvalidArgument,
                       "replicated follower grouped authority policy differs from execution"});
  }
  auto batch_config = cluster::construct_raft_observation_tcp_batch(
      plan, query_config.catalog.get(), authority_config);
  if (!batch_config.has_value())
    return common::make_unexpected(batch_config.error());
  auto authority = cluster::RaftObservationTcpBatchAcquisition::create(std::move(*batch_config));
  if (!authority.has_value())
    return common::make_unexpected(authority.error());
  try {
    return ReplicatedFollowerDistributedGroupedFloat64Query{std::make_unique<Impl>(
        std::move(plan), std::move(snapshot), query_config, std::move(*authority))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "replicated follower grouped query owner allocation failed"});
  }
}

common::Status ReplicatedFollowerDistributedGroupedFloat64Query::poll_once(
    const std::chrono::milliseconds maximum_wait) {
  if (!implementation_)
    return {common::StatusCode::kInvalidArgument,
            "replicated follower grouped query owner is empty"};
  if (maximum_wait.count() < 0 || maximum_wait.count() > INT_MAX) {
    return {common::StatusCode::kInvalidArgument,
            "replicated follower grouped query poll wait is invalid"};
  }
  Impl& impl = *implementation_;
  if (impl.owner_state == ReplicatedFollowerDistributedGroupedFloat64QueryState::kFailed ||
      impl.owner_state == ReplicatedFollowerDistributedGroupedFloat64QueryState::kCancelled) {
    return impl.owner_failure;
  }
  if (impl.owner_state == ReplicatedFollowerDistributedGroupedFloat64QueryState::kComplete)
    return common::Status::ok();
  if (impl.owner_state ==
      ReplicatedFollowerDistributedGroupedFloat64QueryState::kAcquiringAuthority) {
    const common::Status progress = impl.authority.poll_once(maximum_wait);
    if (!progress.is_ok())
      return impl.fail(progress);
    if (impl.authority.state() != cluster::RaftObservationTcpBatchAcquisitionState::kComplete)
      return common::Status::ok();
    auto authorities = impl.authority.result();
    if (!authorities.has_value())
      return impl.fail(authorities.error());
    auto execution = create_replicated_follower_distributed_grouped_float64_query(
        std::move(impl.plan), std::move(impl.snapshot), *authorities, impl.config);
    if (!execution.has_value())
      return impl.fail(execution.error());
    impl.execution.emplace(std::move(*execution));
    impl.owner_state = ReplicatedFollowerDistributedGroupedFloat64QueryState::kExecuting;
    return common::Status::ok();
  }
  const common::Status progress = impl.execution->poll_once(maximum_wait);
  const auto state = impl.execution->state();
  if (state == cluster::DistributedGroupedQueryTcpExecutionState::kComplete) {
    impl.owner_state = ReplicatedFollowerDistributedGroupedFloat64QueryState::kComplete;
    return common::Status::ok();
  }
  if (state == cluster::DistributedGroupedQueryTcpExecutionState::kFailed)
    return impl.fail(impl.execution->failure());
  if (state == cluster::DistributedGroupedQueryTcpExecutionState::kCancelled) {
    impl.owner_failure = impl.execution->failure();
    impl.owner_state = ReplicatedFollowerDistributedGroupedFloat64QueryState::kCancelled;
    return impl.owner_failure;
  }
  return progress;
}

common::Status ReplicatedFollowerDistributedGroupedFloat64Query::cancel() {
  if (!implementation_)
    return {common::StatusCode::kInvalidArgument,
            "replicated follower grouped query owner is empty"};
  Impl& impl = *implementation_;
  if (impl.owner_state == ReplicatedFollowerDistributedGroupedFloat64QueryState::kFailed ||
      impl.owner_state == ReplicatedFollowerDistributedGroupedFloat64QueryState::kCancelled) {
    return impl.owner_failure;
  }
  if (impl.owner_state == ReplicatedFollowerDistributedGroupedFloat64QueryState::kComplete) {
    return {common::StatusCode::kInvalidArgument,
            "completed replicated follower grouped query cannot be cancelled"};
  }
  if (impl.owner_state ==
      ReplicatedFollowerDistributedGroupedFloat64QueryState::kAcquiringAuthority) {
    static_cast<void>(impl.authority.cancel());
  } else {
    static_cast<void>(impl.execution->cancel());
  }
  impl.owner_failure = {common::StatusCode::kCancelled,
                        "replicated follower distributed grouped query was cancelled"};
  impl.owner_state = ReplicatedFollowerDistributedGroupedFloat64QueryState::kCancelled;
  return impl.owner_failure;
}

ReplicatedFollowerDistributedGroupedFloat64QueryState
ReplicatedFollowerDistributedGroupedFloat64Query::state() const noexcept {
  return implementation_ ? implementation_->owner_state
                         : ReplicatedFollowerDistributedGroupedFloat64QueryState::kFailed;
}

ReplicatedFollowerDistributedGroupedFloat64QueryMetrics
ReplicatedFollowerDistributedGroupedFloat64Query::metrics() const noexcept {
  if (!implementation_)
    return {};
  return {.authority = implementation_->authority.metrics(),
          .execution = implementation_->execution.has_value()
                           ? std::optional{implementation_->execution->metrics()}
                           : std::nullopt};
}

common::Result<std::vector<query::GroupedFloat64AggregateResult>>
ReplicatedFollowerDistributedGroupedFloat64Query::result() const {
  if (!implementation_) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInvalidArgument, "replicated follower grouped query owner is empty"});
  }
  if (implementation_->owner_state ==
          ReplicatedFollowerDistributedGroupedFloat64QueryState::kFailed ||
      implementation_->owner_state ==
          ReplicatedFollowerDistributedGroupedFloat64QueryState::kCancelled) {
    return common::make_unexpected(implementation_->owner_failure);
  }
  if (implementation_->owner_state !=
      ReplicatedFollowerDistributedGroupedFloat64QueryState::kComplete) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInvalidArgument,
                       "replicated follower grouped query result is unavailable"});
  }
  return implementation_->execution->result();
}

const common::Status& ReplicatedFollowerDistributedGroupedFloat64Query::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "replicated follower grouped query owner is empty"};
  return implementation_ ? implementation_->owner_failure : empty;
}

} // namespace chronos::service
