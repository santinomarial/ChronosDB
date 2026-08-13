#include "chronos/query/distributed_fragment_binding.hpp"

#include "chronos/manifest/temporal_codec.hpp"

#include <algorithm>
#include <bitset>
#include <cstddef>
#include <cstdint>
#include <new>
#include <ranges>
#include <set>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status unavailable(const char* message) {
  return {common::StatusCode::kUnavailable, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

template <typename Range, typename Projection>
[[nodiscard]] bool canonical_unique_range(const Range& values, Projection projection) {
  return std::ranges::is_sorted(values, {}, projection) &&
         std::ranges::adjacent_find(values, {}, projection) == values.end();
}

[[nodiscard]] common::Status
validate_metadata_catalog_order(const raft::MetadataCatalogSnapshot& catalog) {
  if (catalog.applied_index == 0U ||
      !canonical_unique_range(catalog.active_schemas, &raft::ActiveSchemaMetadata::table_id) ||
      !canonical_unique_range(catalog.tablet_placements,
                              &raft::TabletPlacementMetadata::tablet_id) ||
      !canonical_unique_range(catalog.tablet_group_bindings,
                              &raft::TabletGroupBindingMetadata::tablet_id)) {
    return corruption("distributed metadata catalog is not a canonical committed snapshot");
  }
  for (const auto& definition : catalog.schema_definitions) {
    if (definition.schema == nullptr)
      return corruption("distributed metadata catalog contains a null schema definition");
  }
  if (!canonical_unique_range(catalog.schema_definitions, [](const auto& definition) {
        return definition.schema->schema_id();
      })) {
    return corruption("distributed metadata catalog schema order is not canonical");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status
validate_stable_observation(const raft::RaftGroupObservation& observation,
                            const raft::TabletPlacementMetadata& placement,
                            const raft::GroupId& group_id) {
  if (observation.group_id != group_id || observation.node_id == 0U ||
      observation.current_term == 0U || observation.last_log_index < observation.commit_index ||
      observation.commit_index < observation.applied_index ||
      (observation.role != raft::Role::kFollower && observation.role != raft::Role::kCandidate &&
       observation.role != raft::Role::kLeader)) {
    return corruption("distributed replica observation identity or indexes are invalid");
  }
  if (observation.joint_membership_active || observation.joint_membership_can_finalize ||
      observation.final_membership_pending || !observation.joint_old_voters.empty() ||
      !observation.joint_new_voters.empty() || observation.voters != placement.replicas ||
      observation.committed_voters != placement.replicas) {
    return unavailable("distributed replica membership differs from committed placement");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status validate_placement(const raft::TabletPlacementMetadata& placement,
                                                const schema::TableId& table_id,
                                                const schema::TabletId& tablet_id,
                                                const DistributedReadAdmission& admission,
                                                const DistributedReadConsistency consistency) {
  if (placement.table_id != table_id || placement.tablet_id != tablet_id ||
      placement.placement_epoch == 0U || placement.replicas.empty() ||
      placement.replicas.size() > raft::MetadataLimits{}.maximum_replicas_per_tablet ||
      placement.replicas.front() == 0U || !std::ranges::is_sorted(placement.replicas) ||
      std::ranges::adjacent_find(placement.replicas) != placement.replicas.end() ||
      !std::ranges::binary_search(placement.replicas, admission.serving_node)) {
    return unavailable("distributed fragment placement does not authorize the serving replica");
  }
  if (placement.leader_hint.has_value() &&
      !std::ranges::binary_search(placement.replicas, *placement.leader_hint)) {
    return invalid("distributed fragment placement leader is not a replica");
  }
  if (consistency == DistributedReadConsistency::kLeaderLinearizable &&
      placement.leader_hint.has_value() && *placement.leader_hint != admission.serving_node) {
    return unavailable("distributed fragment placement leader differs from admission");
  }
  return common::Status::ok();
}

} // namespace

common::Result<DistributedAggregateFragmentDispatch>
bind_distributed_aggregate_fragment(const DistributedAggregateFragmentBinding& binding) {
  const DistributedAggregatePlan& plan = binding.plan.get();
  const DistributedReadAdmission& admission = binding.admission.get();
  const manifest::TemporalDatabaseStorageSnapshot& snapshot = binding.snapshot.get();
  const schema::TableSchema& destination_schema = binding.destination_schema.get();
  const raft::TabletPlacementMetadata& placement = binding.placement.get();

  const common::Status admission_status = validate_distributed_read_admission(plan, admission);
  if (!admission_status.is_ok())
    return common::make_unexpected(admission_status);
  if (plan.query_id.is_nil() || binding.raft_group_id.is_nil() || !plan.scan_pushdown ||
      !plan.filter_pushdown || !plan.projection_pushdown || !plan.partial_aggregate_pushdown) {
    return common::make_unexpected(
        invalid("distributed aggregate plan cannot form a worker fragment"));
  }

  const auto planned =
      std::ranges::find(plan.fragments, admission.tablet_id, &DistributedTablet::tablet_id);
  if (planned == plan.fragments.end()) {
    return common::make_unexpected(invalid("distributed fragment tablet is absent from its plan"));
  }
  const common::Status placement_status =
      validate_placement(placement, destination_schema.table_id(), admission.tablet_id, admission,
                         plan.read_policy.consistency);
  if (!placement_status.is_ok())
    return common::make_unexpected(placement_status);

  const auto tablet = std::ranges::find(snapshot.tablets(), admission.tablet_id,
                                        &manifest::TemporalTabletDescriptor::tablet_id);
  if (tablet == snapshot.tablets().end())
    return common::make_unexpected(
        unavailable("distributed fragment tablet is absent from snapshot"));
  if (snapshot.database_id().uuid().is_nil() || snapshot.generation() == 0U ||
      tablet->table_id != destination_schema.table_id() || placement.table_id != tablet->table_id ||
      placement.tablet_id != tablet->tablet_id) {
    return common::make_unexpected(invalid("distributed fragment snapshot identity differs"));
  }
  if (tablet->commit_source != manifest::ManifestCommitSource::kRaft ||
      tablet->source_id != binding.raft_group_id ||
      tablet->durable_position != admission.applied_position) {
    return common::make_unexpected(
        unavailable("distributed fragment snapshot is not durable at the admitted Raft boundary"));
  }
  if (tablet->recovery_schema_id != destination_schema.schema_id() ||
      tablet->recovery_schema_version != destination_schema.version()) {
    return common::make_unexpected(unavailable(
        "distributed fragment destination schema differs from snapshot recovery schema"));
  }

  if (binding.destination_column_ordinals.empty() ||
      binding.destination_column_ordinals.size() > schema::kMaximumSchemaColumnCount ||
      binding.aggregate_input_index >= binding.destination_column_ordinals.size()) {
    return common::make_unexpected(
        invalid("distributed fragment projection is empty or out of bounds"));
  }
  std::bitset<schema::kMaximumSchemaColumnCount> seen;
  for (const std::uint32_t ordinal : binding.destination_column_ordinals) {
    if (ordinal >= destination_schema.columns().size() || seen[ordinal]) {
      return common::make_unexpected(
          invalid("distributed fragment projection contains an invalid or duplicate ordinal"));
    }
    seen.set(ordinal);
  }
  const std::uint32_t aggregate_ordinal =
      binding.destination_column_ordinals[binding.aggregate_input_index];
  if (destination_schema.columns()[aggregate_ordinal].type().kind() !=
      schema::LogicalTypeKind::kFloat64) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kNotSupported, "distributed fragment aggregate input is not Float64"});
  }
  if (binding.event_time_predicate.has_value() &&
      !binding.event_time_predicate->lower.has_value() &&
      !binding.event_time_predicate->upper.has_value()) {
    return common::make_unexpected(
        invalid("distributed fragment empty event-time predicate is not canonical"));
  }

  try {
    std::vector<std::uint32_t> projection(binding.destination_column_ordinals.begin(),
                                          binding.destination_column_ordinals.end());
    return DistributedAggregateFragmentDispatch{
        .raft_group_id = binding.raft_group_id,
        .fragment = {.query_id = plan.query_id,
                     .database_id = snapshot.database_id(),
                     .table_id = tablet->table_id,
                     .tablet_id = tablet->tablet_id,
                     .destination_schema_id = destination_schema.schema_id(),
                     .snapshot_generation = snapshot.generation(),
                     .serving_node = admission.serving_node,
                     .applied_position = admission.applied_position,
                     .observed_leader_commit_position = admission.observed_leader_commit_position,
                     .placement_epoch = placement.placement_epoch,
                     .read_policy = plan.read_policy,
                     .linearizable_barrier = admission.linearizable_barrier,
                     .destination_column_ordinals = std::move(projection),
                     .aggregate_input_index = binding.aggregate_input_index,
                     .event_time_predicate = binding.event_time_predicate}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kResourceExhausted, "distributed fragment binding allocation failed"});
  } catch (const std::length_error&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "distributed fragment binding exceeds limits"});
  }
}

common::Result<DistributedVectorFragmentDispatch>
bind_distributed_vector_fragment(const DistributedVectorFragmentBinding& binding) {
  const DistributedVectorQueryPlan& plan = binding.plan.get();
  const DistributedReadAdmission& admission = binding.admission.get();
  const manifest::TemporalDatabaseStorageSnapshot& snapshot = binding.snapshot.get();
  const schema::TableSchema& destination_schema = binding.destination_schema.get();
  const raft::TabletPlacementMetadata& placement = binding.placement.get();

  if (plan.query_id.is_nil() || plan.fragments.empty() || binding.raft_group_id.is_nil() ||
      ((plan.read_policy.consistency == DistributedReadConsistency::kLeaderLinearizable ||
        plan.read_policy.consistency == DistributedReadConsistency::kLocalEventual) &&
       plan.read_policy.maximum_staleness_positions.has_value())) {
    return common::make_unexpected(invalid("distributed vector plan authority is invalid"));
  }
  const common::Status admission_status =
      validate_distributed_read_admission(plan.read_policy, plan.fragments, admission);
  if (!admission_status.is_ok())
    return common::make_unexpected(admission_status);
  const auto planned =
      std::ranges::find(plan.fragments, admission.tablet_id, &DistributedTablet::tablet_id);
  if (planned == plan.fragments.end())
    return common::make_unexpected(
        invalid("distributed vector fragment tablet is absent from its plan"));
  const common::Status placement_status =
      validate_placement(placement, destination_schema.table_id(), admission.tablet_id, admission,
                         plan.read_policy.consistency);
  if (!placement_status.is_ok())
    return common::make_unexpected(placement_status);

  const auto tablet = std::ranges::find(snapshot.tablets(), admission.tablet_id,
                                        &manifest::TemporalTabletDescriptor::tablet_id);
  if (tablet == snapshot.tablets().end())
    return common::make_unexpected(
        unavailable("distributed vector fragment tablet is absent from snapshot"));
  if (snapshot.database_id().uuid().is_nil() || snapshot.generation() == 0U ||
      tablet->table_id != destination_schema.table_id() || placement.table_id != tablet->table_id ||
      placement.tablet_id != tablet->tablet_id) {
    return common::make_unexpected(
        invalid("distributed vector fragment snapshot identity differs"));
  }
  if (tablet->commit_source != manifest::ManifestCommitSource::kRaft ||
      tablet->source_id != binding.raft_group_id ||
      tablet->durable_position != admission.applied_position) {
    return common::make_unexpected(unavailable(
        "distributed vector fragment snapshot is not durable at the admitted Raft boundary"));
  }
  if (tablet->recovery_schema_id != destination_schema.schema_id() ||
      tablet->recovery_schema_version != destination_schema.version()) {
    return common::make_unexpected(unavailable(
        "distributed vector fragment destination schema differs from snapshot recovery schema"));
  }
  if (binding.destination_column_ordinals.empty() ||
      binding.destination_column_ordinals.size() > schema::kMaximumSchemaColumnCount) {
    return common::make_unexpected(
        invalid("distributed vector fragment projection is empty or out of bounds"));
  }
  std::bitset<schema::kMaximumSchemaColumnCount> seen;
  for (const std::uint32_t ordinal : binding.destination_column_ordinals) {
    if (ordinal >= destination_schema.columns().size() || seen[ordinal]) {
      return common::make_unexpected(invalid(
          "distributed vector fragment projection contains an invalid or duplicate ordinal"));
    }
    seen.set(ordinal);
  }
  const common::Status plan_status = validate_distributed_vector_plan_intent(
      plan.intent, static_cast<std::uint32_t>(binding.destination_column_ordinals.size()));
  if (!plan_status.is_ok())
    return common::make_unexpected(plan_status);
  for (const DistributedVectorAggregateIntent& aggregate : plan.intent.aggregates) {
    std::optional<VectorAggregateInput> input;
    if (aggregate.input_index.has_value()) {
      const std::uint32_t schema_ordinal =
          binding.destination_column_ordinals[*aggregate.input_index];
      const schema::ColumnDefinition& column = destination_schema.columns()[schema_ordinal];
      input = VectorAggregateInput{.column_ordinal = *aggregate.input_index,
                                   .type = column.type(),
                                   .nullable = column.nullable()};
    }
    const auto output = vector_aggregate_output_shape(
        {.operation = aggregate.operation, .input = std::move(input)});
    if (!output.has_value())
      return common::make_unexpected(output.error());
  }
  if (binding.event_time_predicate.has_value() &&
      !binding.event_time_predicate->lower.has_value() &&
      !binding.event_time_predicate->upper.has_value()) {
    return common::make_unexpected(
        invalid("distributed vector fragment event-time predicate is empty"));
  }

  try {
    return DistributedVectorFragmentDispatch{
        .query_id = plan.query_id,
        .database_id = snapshot.database_id(),
        .table_id = tablet->table_id,
        .tablet_id = tablet->tablet_id,
        .destination_schema_id = destination_schema.schema_id(),
        .raft_group_id = binding.raft_group_id,
        .snapshot_generation = snapshot.generation(),
        .serving_node = admission.serving_node,
        .applied_position = admission.applied_position,
        .observed_leader_commit_position = admission.observed_leader_commit_position,
        .placement_epoch = placement.placement_epoch,
        .read_policy = plan.read_policy,
        .linearizable_barrier = admission.linearizable_barrier,
        .destination_column_ordinals = {binding.destination_column_ordinals.begin(),
                                        binding.destination_column_ordinals.end()},
        .event_time_predicate = binding.event_time_predicate,
        .plan = plan.intent};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "distributed vector fragment binding allocation failed"});
  } catch (const std::length_error&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "distributed vector fragment binding exceeds container limits"});
  }
}

common::Result<BoundDistributedGroupedFloat64Fragment>
bind_distributed_grouped_float64_fragment(const DistributedGroupedFloat64FragmentBinding& binding) {
  auto aggregate = bind_distributed_aggregate_fragment(binding.aggregate);
  if (!aggregate.has_value())
    return common::make_unexpected(aggregate.error());
  if (binding.group_key_input_index >= aggregate->fragment.destination_column_ordinals.size()) {
    return common::make_unexpected(
        invalid("distributed grouped fragment key input is out of bounds"));
  }
  const schema::TableSchema& destination_schema = binding.aggregate.destination_schema.get();
  const std::uint32_t key_ordinal =
      aggregate->fragment.destination_column_ordinals[binding.group_key_input_index];
  if (destination_schema.columns()[key_ordinal].type().kind() !=
      schema::LogicalTypeKind::kFloat64) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotSupported,
                       "distributed grouped fragment key input is not Float64"});
  }
  return BoundDistributedGroupedFloat64Fragment{
      .raft_group_id = aggregate->raft_group_id,
      .fragment = {.aggregate = std::move(aggregate->fragment),
                   .group_key_input_index = binding.group_key_input_index}};
}

common::Result<DistributedGroupedFloat64FragmentDispatch>
bind_distributed_grouped_float64_fragment_dispatch(
    const DistributedGroupedFloat64FragmentBinding& binding) {
  auto bound = bind_distributed_grouped_float64_fragment(binding);
  if (!bound.has_value())
    return common::make_unexpected(bound.error());
  return DistributedGroupedFloat64FragmentDispatch{.raft_group_id = bound->raft_group_id,
                                                   .fragment = std::move(bound->fragment)};
}

CompatibleDistributedAggregateSnapshot::CompatibleDistributedAggregateSnapshot(
    manifest::TemporalDatabaseStorageSnapshot snapshot,
    std::vector<DistributedAggregateFragmentDispatch> dispatches) noexcept
    : snapshot_(std::move(snapshot)), dispatches_(std::move(dispatches)) {}

const manifest::TemporalDatabaseStorageSnapshot&
CompatibleDistributedAggregateSnapshot::snapshot() const noexcept {
  return snapshot_;
}

std::span<const DistributedAggregateFragmentDispatch>
CompatibleDistributedAggregateSnapshot::dispatches() const noexcept {
  return dispatches_;
}

common::Result<CompatibleDistributedAggregateSnapshot>
bind_compatible_distributed_aggregate_snapshot(
    const DistributedAggregatePlan& plan, manifest::TemporalDatabaseStorageSnapshot snapshot,
    const std::span<const DistributedAggregateSnapshotFragmentBinding> bindings,
    const DistributedAggregateSnapshotBindingLimits limits) {
  if (limits.maximum_fragments == 0U ||
      limits.maximum_fragments > DistributedPlanLimits{}.maximum_fragments ||
      limits.maximum_total_projection_ordinals == 0U ||
      limits.maximum_total_projection_ordinals > kMaximumDistributedSnapshotProjectionOrdinals) {
    return common::make_unexpected(
        invalid("compatible distributed snapshot binding limits are invalid"));
  }
  if (plan.query_id.is_nil() || plan.fragments.empty() || !plan.scan_pushdown ||
      !plan.filter_pushdown || !plan.projection_pushdown || !plan.partial_aggregate_pushdown ||
      snapshot.database_id().uuid().is_nil() || snapshot.generation() == 0U) {
    return common::make_unexpected(invalid("compatible distributed snapshot authority is invalid"));
  }
  if (plan.fragments.size() != bindings.size())
    return common::make_unexpected(
        invalid("compatible distributed snapshot binding count differs from plan"));
  if (bindings.size() > limits.maximum_fragments)
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "compatible distributed snapshot fragment limit is exhausted"});

  try {
    std::vector<DistributedAggregateFragmentDispatch> dispatches;
    dispatches.reserve(bindings.size());
    std::set<schema::TabletId> tablet_ids;
    std::size_t total_projection_ordinals = 0U;
    for (std::size_t index = 0U; index < bindings.size(); ++index) {
      const auto& binding = bindings[index];
      const DistributedReadAdmission& admission = binding.admission.get();
      if (plan.fragments[index].tablet_id.uuid().is_nil() ||
          !tablet_ids.insert(plan.fragments[index].tablet_id).second ||
          admission.tablet_id != plan.fragments[index].tablet_id) {
        return common::make_unexpected(
            invalid("compatible distributed snapshot tablet identity or order is invalid"));
      }
      if (binding.destination_column_ordinals.size() >
          limits.maximum_total_projection_ordinals - total_projection_ordinals) {
        return common::make_unexpected(
            common::Status{common::StatusCode::kResourceExhausted,
                           "compatible distributed snapshot projection limit is exhausted"});
      }
      total_projection_ordinals += binding.destination_column_ordinals.size();
      auto dispatch = bind_distributed_aggregate_fragment(
          {.plan = std::cref(plan),
           .admission = binding.admission,
           .snapshot = std::cref(snapshot),
           .destination_schema = binding.destination_schema,
           .raft_group_id = binding.raft_group_id,
           .placement = binding.placement,
           .destination_column_ordinals = binding.destination_column_ordinals,
           .aggregate_input_index = binding.aggregate_input_index,
           .event_time_predicate = binding.event_time_predicate});
      if (!dispatch.has_value())
        return common::make_unexpected(dispatch.error());
      if (dispatch->fragment.database_id != snapshot.database_id() ||
          dispatch->fragment.snapshot_generation != snapshot.generation()) {
        return common::make_unexpected(
            invalid("compatible distributed snapshot dispatch escaped its owning epoch"));
      }
      dispatches.push_back(std::move(*dispatch));
    }
    return CompatibleDistributedAggregateSnapshot{std::move(snapshot), std::move(dispatches)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "compatible distributed snapshot binding allocation failed"});
  } catch (const std::length_error&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "compatible distributed snapshot binding exceeds container limits"});
  }
}

CompatibleDistributedVectorSnapshot::CompatibleDistributedVectorSnapshot(
    manifest::TemporalDatabaseStorageSnapshot snapshot,
    std::vector<DistributedVectorFragmentDispatch> dispatches) noexcept
    : snapshot_(std::move(snapshot)), dispatches_(std::move(dispatches)) {}

const manifest::TemporalDatabaseStorageSnapshot&
CompatibleDistributedVectorSnapshot::snapshot() const noexcept {
  return snapshot_;
}

std::span<const DistributedVectorFragmentDispatch>
CompatibleDistributedVectorSnapshot::dispatches() const noexcept {
  return dispatches_;
}

common::Result<CompatibleDistributedVectorSnapshot> bind_compatible_distributed_vector_snapshot(
    const DistributedVectorQueryPlan& plan, manifest::TemporalDatabaseStorageSnapshot snapshot,
    const std::span<const DistributedVectorSnapshotFragmentBinding> bindings,
    const DistributedVectorSnapshotBindingLimits limits) {
  if (limits.maximum_fragments == 0U ||
      limits.maximum_fragments > DistributedPlanLimits{}.maximum_fragments ||
      limits.maximum_total_projection_ordinals == 0U ||
      limits.maximum_total_projection_ordinals > kMaximumDistributedSnapshotProjectionOrdinals) {
    return common::make_unexpected(
        invalid("compatible distributed vector snapshot limits are invalid"));
  }
  if (plan.query_id.is_nil() || plan.fragments.empty() || snapshot.database_id().uuid().is_nil() ||
      snapshot.generation() == 0U || plan.fragments.size() != bindings.size()) {
    return common::make_unexpected(
        invalid("compatible distributed vector snapshot authority is invalid"));
  }
  if (bindings.size() > limits.maximum_fragments) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "compatible distributed vector snapshot fragment limit is exhausted"});
  }
  try {
    std::vector<DistributedVectorFragmentDispatch> dispatches;
    dispatches.reserve(bindings.size());
    std::set<schema::TabletId> tablet_ids;
    std::size_t total_projection_ordinals{};
    for (std::size_t index = 0U; index < bindings.size(); ++index) {
      const DistributedVectorSnapshotFragmentBinding& binding = bindings[index];
      const DistributedReadAdmission& admission = binding.admission.get();
      if (plan.fragments[index].tablet_id.uuid().is_nil() ||
          !tablet_ids.insert(plan.fragments[index].tablet_id).second ||
          admission.tablet_id != plan.fragments[index].tablet_id) {
        return common::make_unexpected(
            invalid("compatible distributed vector snapshot tablet identity or order is invalid"));
      }
      if (binding.destination_column_ordinals.size() >
          limits.maximum_total_projection_ordinals - total_projection_ordinals) {
        return common::make_unexpected(
            common::Status{common::StatusCode::kResourceExhausted,
                           "compatible distributed vector snapshot projection limit is exhausted"});
      }
      total_projection_ordinals += binding.destination_column_ordinals.size();
      auto dispatch = bind_distributed_vector_fragment(
          {.plan = std::cref(plan),
           .admission = binding.admission,
           .snapshot = std::cref(snapshot),
           .destination_schema = binding.destination_schema,
           .raft_group_id = binding.raft_group_id,
           .placement = binding.placement,
           .destination_column_ordinals = binding.destination_column_ordinals,
           .event_time_predicate = binding.event_time_predicate});
      if (!dispatch.has_value())
        return common::make_unexpected(dispatch.error());
      if (dispatch->database_id != snapshot.database_id() ||
          dispatch->snapshot_generation != snapshot.generation()) {
        return common::make_unexpected(
            invalid("compatible distributed vector dispatch escaped its owning epoch"));
      }
      dispatches.push_back(std::move(*dispatch));
    }
    return CompatibleDistributedVectorSnapshot{std::move(snapshot), std::move(dispatches)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "compatible distributed vector snapshot binding allocation failed"});
  } catch (const std::length_error&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "compatible distributed vector snapshot binding exceeds container limits"});
  }
}

CompatibleDistributedGroupedFloat64Snapshot::CompatibleDistributedGroupedFloat64Snapshot(
    CompatibleDistributedAggregateSnapshot aggregate_snapshot,
    std::vector<DistributedGroupedFloat64FragmentDispatch> dispatches) noexcept
    : aggregate_snapshot_(std::move(aggregate_snapshot)), dispatches_(std::move(dispatches)) {}

const manifest::TemporalDatabaseStorageSnapshot&
CompatibleDistributedGroupedFloat64Snapshot::snapshot() const noexcept {
  return aggregate_snapshot_.snapshot();
}

std::span<const DistributedGroupedFloat64FragmentDispatch>
CompatibleDistributedGroupedFloat64Snapshot::dispatches() const noexcept {
  return dispatches_;
}

common::Result<CompatibleDistributedGroupedFloat64Snapshot>
bind_compatible_distributed_grouped_float64_snapshot(
    const DistributedAggregatePlan& plan, manifest::TemporalDatabaseStorageSnapshot snapshot,
    const std::span<const DistributedAggregateSnapshotFragmentBinding> bindings,
    const std::uint32_t group_key_input_index,
    const DistributedAggregateSnapshotBindingLimits limits) {
  auto aggregate =
      bind_compatible_distributed_aggregate_snapshot(plan, std::move(snapshot), bindings, limits);
  if (!aggregate.has_value())
    return common::make_unexpected(aggregate.error());
  if (aggregate->dispatches().size() != bindings.size()) {
    return common::make_unexpected(
        invalid("compatible grouped snapshot aggregate binding count is inconsistent"));
  }
  try {
    std::vector<DistributedGroupedFloat64FragmentDispatch> dispatches;
    dispatches.reserve(bindings.size());
    for (std::size_t index = 0U; index < bindings.size(); ++index) {
      const DistributedAggregateFragmentDispatch& nested = aggregate->dispatches()[index];
      if (group_key_input_index >= nested.fragment.destination_column_ordinals.size()) {
        return common::make_unexpected(
            invalid("compatible grouped snapshot key input is out of bounds"));
      }
      const std::uint32_t key_ordinal =
          nested.fragment.destination_column_ordinals[group_key_input_index];
      const schema::TableSchema& destination_schema = bindings[index].destination_schema.get();
      if (key_ordinal >= destination_schema.columns().size()) {
        return common::make_unexpected(
            invalid("compatible grouped snapshot key ordinal is out of bounds"));
      }
      if (destination_schema.columns()[key_ordinal].type().kind() !=
          schema::LogicalTypeKind::kFloat64) {
        return common::make_unexpected(
            common::Status{common::StatusCode::kNotSupported,
                           "compatible grouped snapshot key input is not Float64"});
      }
      dispatches.push_back({.raft_group_id = nested.raft_group_id,
                            .fragment = {.aggregate = nested.fragment,
                                         .group_key_input_index = group_key_input_index}});
    }
    return CompatibleDistributedGroupedFloat64Snapshot{std::move(*aggregate),
                                                       std::move(dispatches)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "compatible grouped snapshot binding allocation failed"});
  } catch (const std::length_error&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "compatible grouped snapshot binding exceeds container limits"});
  }
}

common::Result<CompatibleDistributedGroupedFloat64Snapshot>
bind_compatible_distributed_grouped_float64_snapshot(
    CompatibleDistributedAggregateSnapshot aggregate_snapshot,
    const schema::TableSchema& destination_schema, const std::uint32_t group_key_input_index) {
  try {
    std::vector<DistributedGroupedFloat64FragmentDispatch> dispatches;
    dispatches.reserve(aggregate_snapshot.dispatches().size());
    for (const DistributedAggregateFragmentDispatch& nested : aggregate_snapshot.dispatches()) {
      if (nested.fragment.table_id != destination_schema.table_id() ||
          nested.fragment.destination_schema_id != destination_schema.schema_id()) {
        return common::make_unexpected(
            invalid("compatible grouped specialization schema differs from bound dispatch"));
      }
      if (group_key_input_index >= nested.fragment.destination_column_ordinals.size()) {
        return common::make_unexpected(
            invalid("compatible grouped specialization key input is out of bounds"));
      }
      const std::uint32_t key_ordinal =
          nested.fragment.destination_column_ordinals[group_key_input_index];
      if (key_ordinal >= destination_schema.columns().size()) {
        return common::make_unexpected(
            invalid("compatible grouped specialization key ordinal is out of bounds"));
      }
      if (destination_schema.columns()[key_ordinal].type().kind() !=
          schema::LogicalTypeKind::kFloat64) {
        return common::make_unexpected(
            common::Status{common::StatusCode::kNotSupported,
                           "compatible grouped specialization key input is not Float64"});
      }
      dispatches.push_back({.raft_group_id = nested.raft_group_id,
                            .fragment = {.aggregate = nested.fragment,
                                         .group_key_input_index = group_key_input_index}});
    }
    return CompatibleDistributedGroupedFloat64Snapshot{std::move(aggregate_snapshot),
                                                       std::move(dispatches)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "compatible grouped specialization allocation failed"});
  } catch (const std::length_error&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "compatible grouped specialization exceeds container limits"});
  }
}

common::Result<CompatibleDistributedAggregateSnapshot>
bind_metadata_backed_distributed_aggregate_snapshot(
    const DistributedAggregatePlan& plan, manifest::TemporalDatabaseStorageSnapshot snapshot,
    const MetadataBackedDistributedAggregateSnapshotBinding& binding,
    const DistributedAggregateSnapshotBindingLimits limits) {
  const raft::MetadataCatalogSnapshot& catalog = binding.catalog.get();
  const common::Status catalog_status = validate_metadata_catalog_order(catalog);
  if (!catalog_status.is_ok())
    return common::make_unexpected(catalog_status);
  if (binding.table_id.uuid().is_nil() || plan.fragments.size() != binding.replica_proofs.size()) {
    return common::make_unexpected(
        invalid("metadata-backed distributed binding identity or proof count is invalid"));
  }

  const auto active = std::ranges::lower_bound(catalog.active_schemas, binding.table_id, {},
                                               &raft::ActiveSchemaMetadata::table_id);
  if (active == catalog.active_schemas.end() || active->table_id != binding.table_id)
    return common::make_unexpected(unavailable("distributed table has no active schema"));
  const auto definition =
      std::ranges::lower_bound(catalog.schema_definitions, active->schema_id, {},
                               [](const auto& value) { return value.schema->schema_id(); });
  if (definition == catalog.schema_definitions.end() ||
      definition->schema->schema_id() != active->schema_id ||
      definition->schema->table_id() != binding.table_id) {
    return common::make_unexpected(
        corruption("distributed active schema definition is absent or belongs to another table"));
  }

  try {
    std::vector<DistributedReadAdmission> admissions;
    std::vector<DistributedAggregateSnapshotFragmentBinding> resolved;
    admissions.reserve(plan.fragments.size());
    resolved.reserve(plan.fragments.size());
    for (std::size_t index = 0U; index < plan.fragments.size(); ++index) {
      const DistributedTablet& fragment = plan.fragments[index];
      const DistributedAggregateReplicaProof& proof = binding.replica_proofs[index];
      const raft::RaftGroupObservation& observation = proof.observation.get();
      const auto placement =
          std::ranges::lower_bound(catalog.tablet_placements, fragment.tablet_id, {},
                                   &raft::TabletPlacementMetadata::tablet_id);
      const auto group = std::ranges::lower_bound(catalog.tablet_group_bindings, fragment.tablet_id,
                                                  {}, &raft::TabletGroupBindingMetadata::tablet_id);
      if (placement == catalog.tablet_placements.end() ||
          placement->tablet_id != fragment.tablet_id ||
          group == catalog.tablet_group_bindings.end() || group->tablet_id != fragment.tablet_id) {
        return common::make_unexpected(
            unavailable("distributed tablet metadata authority is incomplete"));
      }
      if (placement->table_id != binding.table_id || group->group_id.is_nil()) {
        return common::make_unexpected(
            corruption("distributed tablet metadata identity is inconsistent"));
      }
      const common::Status observation_status =
          validate_stable_observation(observation, *placement, group->group_id);
      if (!observation_status.is_ok())
        return common::make_unexpected(observation_status);

      std::uint64_t observed_leader_commit_position = 0U;
      switch (plan.read_policy.consistency) {
      case DistributedReadConsistency::kLeaderLinearizable:
        if (proof.observed_leader_commit_position.has_value() ||
            !proof.linearizable_barrier.has_value() || observation.role != raft::Role::kLeader ||
            observation.leader_id != observation.node_id ||
            fragment.leader_node != observation.node_id ||
            proof.linearizable_barrier->term != observation.current_term ||
            proof.linearizable_barrier->read_index > observation.applied_index) {
          return common::make_unexpected(
              unavailable("distributed leader-linearizable proof is not current and applied"));
        }
        observed_leader_commit_position = observation.commit_index;
        break;
      case DistributedReadConsistency::kFollowerBoundedStale:
        if (proof.linearizable_barrier.has_value() ||
            !proof.observed_leader_commit_position.has_value() ||
            *proof.observed_leader_commit_position < observation.commit_index ||
            observation.role == raft::Role::kCandidate || !observation.leader_id.has_value() ||
            !std::ranges::binary_search(placement->replicas, *observation.leader_id) ||
            (observation.role == raft::Role::kLeader &&
             *observation.leader_id != observation.node_id) ||
            (observation.role == raft::Role::kFollower &&
             *observation.leader_id == observation.node_id)) {
          return common::make_unexpected(
              unavailable("distributed bounded-stale proof has no current leader observation"));
        }
        observed_leader_commit_position = *proof.observed_leader_commit_position;
        break;
      case DistributedReadConsistency::kLocalEventual:
        if (proof.linearizable_barrier.has_value() ||
            proof.observed_leader_commit_position.has_value()) {
          return common::make_unexpected(
              invalid("distributed local-eventual proof carries stronger authority"));
        }
        break;
      default:
        return common::make_unexpected(
            invalid("metadata-backed distributed read consistency is invalid"));
      }

      admissions.push_back({.tablet_id = fragment.tablet_id,
                            .serving_node = observation.node_id,
                            .applied_position = observation.applied_index,
                            .observed_leader_commit_position = observed_leader_commit_position,
                            .linearizable_barrier = proof.linearizable_barrier});
      resolved.push_back({.admission = std::cref(admissions.back()),
                          .destination_schema = std::cref(*definition->schema),
                          .raft_group_id = group->group_id,
                          .placement = std::cref(*placement),
                          .destination_column_ordinals = binding.destination_column_ordinals,
                          .aggregate_input_index = binding.aggregate_input_index,
                          .event_time_predicate = binding.event_time_predicate});
    }
    return bind_compatible_distributed_aggregate_snapshot(plan, std::move(snapshot), resolved,
                                                          limits);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "metadata-backed distributed snapshot binding allocation failed"});
  } catch (const std::length_error&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "metadata-backed distributed snapshot binding exceeds container limits"});
  }
}

common::Result<CompatibleDistributedAggregateSnapshot>
bind_group_backed_distributed_aggregate_snapshot(
    const DistributedAggregatePlan& plan, manifest::TemporalDatabaseStorageSnapshot snapshot,
    const GroupBackedDistributedAggregateSnapshotBinding& binding,
    const DistributedAggregateSnapshotBindingLimits limits) {
  const raft::MetadataCatalogSnapshot& catalog = binding.catalog.get();
  const common::Status catalog_status = validate_metadata_catalog_order(catalog);
  if (!catalog_status.is_ok())
    return common::make_unexpected(catalog_status);
  if (binding.group_authorities.empty() ||
      !std::ranges::is_sorted(
          binding.group_authorities, {},
          [](const auto& authority) { return authority.observation.group_id; }) ||
      std::ranges::adjacent_find(binding.group_authorities, {},
                                 [](const auto& authority) {
                                   return authority.observation.group_id;
                                 }) != binding.group_authorities.end() ||
      std::ranges::any_of(binding.group_authorities, [](const auto& authority) {
        return authority.observation.group_id.is_nil() ||
               authority.barrier.group_id != authority.observation.group_id ||
               authority.barrier.barrier.term != authority.observation.current_term;
      })) {
    return common::make_unexpected(
        invalid("group-backed distributed proof authority is not canonical"));
  }
  try {
    std::vector<DistributedAggregateReplicaProof> ordered;
    ordered.reserve(plan.fragments.size());
    for (const DistributedTablet& fragment : plan.fragments) {
      const auto group = std::ranges::lower_bound(catalog.tablet_group_bindings, fragment.tablet_id,
                                                  {}, &raft::TabletGroupBindingMetadata::tablet_id);
      if (group == catalog.tablet_group_bindings.end() || group->tablet_id != fragment.tablet_id)
        return common::make_unexpected(
            unavailable("distributed tablet has no committed Raft group binding"));
      const auto authority =
          std::ranges::lower_bound(binding.group_authorities, group->group_id, {},
                                   [](const auto& value) { return value.observation.group_id; });
      if (authority == binding.group_authorities.end() ||
          authority->observation.group_id != group->group_id)
        return common::make_unexpected(
            unavailable("distributed tablet group has no correlated read proof"));
      ordered.push_back({.observation = std::cref(authority->observation),
                         .linearizable_barrier = authority->barrier.barrier});
    }
    return bind_metadata_backed_distributed_aggregate_snapshot(
        plan, std::move(snapshot),
        {.catalog = binding.catalog,
         .table_id = binding.table_id,
         .replica_proofs = ordered,
         .destination_column_ordinals = binding.destination_column_ordinals,
         .aggregate_input_index = binding.aggregate_input_index,
         .event_time_predicate = binding.event_time_predicate},
        limits);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "group-backed distributed snapshot binding allocation failed"});
  } catch (const std::length_error&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "group-backed distributed snapshot binding exceeds container limits"});
  }
}

bool is_valid_distributed_aggregate_follower_read_authority(
    const DistributedAggregateFollowerReadAuthority& authority) {
  const raft::RaftGroupObservation& leader = authority.leader_observation;
  const raft::RaftGroupObservation& follower = authority.follower_observation;
  return !leader.group_id.is_nil() && follower.group_id == leader.group_id &&
         leader.node_id != 0U && follower.node_id != 0U && leader.node_id != follower.node_id &&
         leader.role == raft::Role::kLeader && follower.role == raft::Role::kFollower &&
         leader.current_term != 0U && follower.current_term == leader.current_term &&
         leader.leader_id == leader.node_id && follower.leader_id == leader.node_id &&
         leader.last_log_index >= leader.commit_index &&
         leader.commit_index >= leader.applied_index &&
         follower.last_log_index >= follower.commit_index &&
         follower.commit_index >= follower.applied_index &&
         leader.commit_index >= follower.commit_index && !leader.joint_membership_active &&
         !leader.joint_membership_can_finalize && !leader.final_membership_pending &&
         !follower.joint_membership_active && !follower.joint_membership_can_finalize &&
         !follower.final_membership_pending && leader.joint_old_voters.empty() &&
         leader.joint_new_voters.empty() && follower.joint_old_voters.empty() &&
         follower.joint_new_voters.empty() && leader.voters == leader.committed_voters &&
         leader.voters == follower.voters && leader.committed_voters == follower.committed_voters;
}

common::Result<CompatibleDistributedAggregateSnapshot>
bind_follower_group_backed_distributed_aggregate_snapshot(
    const DistributedAggregatePlan& plan, manifest::TemporalDatabaseStorageSnapshot snapshot,
    const FollowerGroupBackedDistributedAggregateSnapshotBinding& binding,
    const DistributedAggregateSnapshotBindingLimits limits) {
  const raft::MetadataCatalogSnapshot& catalog = binding.catalog.get();
  const common::Status catalog_status = validate_metadata_catalog_order(catalog);
  if (!catalog_status.is_ok())
    return common::make_unexpected(catalog_status);
  if (plan.read_policy.consistency != DistributedReadConsistency::kFollowerBoundedStale ||
      !plan.read_policy.maximum_staleness_positions.has_value() ||
      binding.group_authorities.empty() ||
      !std::ranges::is_sorted(
          binding.group_authorities, {},
          [](const auto& authority) { return authority.follower_observation.group_id; }) ||
      std::ranges::adjacent_find(binding.group_authorities, {},
                                 [](const auto& authority) {
                                   return authority.follower_observation.group_id;
                                 }) != binding.group_authorities.end() ||
      std::ranges::any_of(binding.group_authorities, [](const auto& authority) {
        return !is_valid_distributed_aggregate_follower_read_authority(authority);
      })) {
    return common::make_unexpected(
        invalid("follower-backed distributed proof authority is not canonical"));
  }
  try {
    std::vector<DistributedAggregateReplicaProof> ordered;
    ordered.reserve(plan.fragments.size());
    for (const DistributedTablet& fragment : plan.fragments) {
      const auto group = std::ranges::lower_bound(catalog.tablet_group_bindings, fragment.tablet_id,
                                                  {}, &raft::TabletGroupBindingMetadata::tablet_id);
      if (group == catalog.tablet_group_bindings.end() || group->tablet_id != fragment.tablet_id)
        return common::make_unexpected(
            unavailable("distributed tablet has no committed Raft group binding"));
      const auto authority = std::ranges::lower_bound(
          binding.group_authorities, group->group_id, {},
          [](const auto& value) { return value.follower_observation.group_id; });
      if (authority == binding.group_authorities.end() ||
          authority->follower_observation.group_id != group->group_id) {
        return common::make_unexpected(
            unavailable("distributed tablet group has no correlated follower proof"));
      }
      ordered.push_back(
          {.observation = std::cref(authority->follower_observation),
           .observed_leader_commit_position = authority->leader_observation.commit_index});
    }
    return bind_metadata_backed_distributed_aggregate_snapshot(
        plan, std::move(snapshot),
        {.catalog = binding.catalog,
         .table_id = binding.table_id,
         .replica_proofs = ordered,
         .destination_column_ordinals = binding.destination_column_ordinals,
         .aggregate_input_index = binding.aggregate_input_index,
         .event_time_predicate = binding.event_time_predicate},
        limits);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "follower-backed distributed snapshot binding allocation failed"});
  } catch (const std::length_error&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "follower-backed distributed snapshot binding exceeds container limits"});
  }
}

} // namespace chronos::query
