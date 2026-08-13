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

} // namespace chronos::query
