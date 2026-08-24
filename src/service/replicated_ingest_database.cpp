#include "chronos/service/replicated_ingest_database.hpp"

#include "chronos/ingest/retry_directory.hpp"
#include "chronos/ingest/tablet_state.hpp"
#include "chronos/query/tablet_state_pipeline.hpp"
#include "chronos/raft/durable_runtime.hpp"
#include "chronos/raft/metadata_runtime.hpp"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <new>
#include <optional>
#include <ranges>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace chronos::service {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return {common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status corruption(std::string message) {
  return {common::StatusCode::kCorruption, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return {common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] common::Status unavailable(std::string message) {
  return {common::StatusCode::kUnavailable, std::move(message)};
}

void observe_startup(const ReplicatedIngestDatabaseConfig& config,
                     const ReplicatedIngestDatabaseStartupStage stage) noexcept {
  if (config.startup_observer != nullptr)
    config.startup_observer->on_startup_stage(stage);
}

class DatabaseRuntimeShutdownObserver final : public ReplicatedIngestRuntimeShutdownObserver {
public:
  explicit DatabaseRuntimeShutdownObserver(
      ReplicatedIngestDatabaseShutdownObserver& configured) noexcept
      : observer_(configured) {}

  void on_shutdown_stage(const ReplicatedIngestRuntimeShutdownStage stage) noexcept override {
    switch (stage) {
    case ReplicatedIngestRuntimeShutdownStage::kCoordinatorReleased:
      observer_.on_shutdown_stage(ReplicatedIngestDatabaseShutdownStage::kCoordinatorReleased);
      return;
    case ReplicatedIngestRuntimeShutdownStage::kAcceptedWorkDrained:
      observer_.on_shutdown_stage(ReplicatedIngestDatabaseShutdownStage::kAcceptedWorkDrained);
      return;
    case ReplicatedIngestRuntimeShutdownStage::kApplicationsStopped:
      observer_.on_shutdown_stage(ReplicatedIngestDatabaseShutdownStage::kApplicationsStopped);
      return;
    case ReplicatedIngestRuntimeShutdownStage::kLogClosed:
      observer_.on_shutdown_stage(ReplicatedIngestDatabaseShutdownStage::kLogClosed);
      return;
    case ReplicatedIngestRuntimeShutdownStage::kWorkerStopped:
      observer_.on_shutdown_stage(ReplicatedIngestDatabaseShutdownStage::kRuntimeStopped);
      return;
    }
  }

private:
  ReplicatedIngestDatabaseShutdownObserver& observer_;
};

[[nodiscard]] const raft::GroupReadBarrier*
find_barrier(const std::span<const raft::GroupReadBarrier> barriers,
             const raft::GroupId& group_id) noexcept {
  const auto found = std::ranges::find(barriers, group_id, &raft::GroupReadBarrier::group_id);
  return found == barriers.end() ? nullptr : std::addressof(*found);
}

[[nodiscard]] const raft::RaftGroupConfiguration*
find_group(const std::vector<raft::RaftGroupConfiguration>& groups, const raft::GroupId& group_id) {
  const auto found = std::ranges::find(groups, group_id, &raft::RaftGroupConfiguration::group_id);
  return found == groups.end() ? nullptr : std::addressof(*found);
}

[[nodiscard]] common::Result<std::optional<ReplicatedSingleGroupQueryRoute>>
single_group_route(const raft::MetadataCatalogSnapshot& catalog, const schema::TableId& table_id) {
  std::optional<ReplicatedSingleGroupQueryRoute> route;
  bool incompatible{};
  for (const raft::TabletPlacementMetadata& placement : catalog.tablet_placements) {
    if (placement.table_id != table_id)
      continue;
    const auto binding = std::ranges::find(catalog.tablet_group_bindings, placement.tablet_id,
                                           &raft::TabletGroupBindingMetadata::tablet_id);
    if (binding == catalog.tablet_group_bindings.end())
      return common::make_unexpected(corruption("replicated query tablet has no group binding"));
    if (!route.has_value()) {
      route = ReplicatedSingleGroupQueryRoute{.table_id = table_id,
                                              .group_id = binding->group_id,
                                              .placement_epoch = placement.placement_epoch,
                                              .replicas = placement.replicas};
    } else if (route->group_id != binding->group_id ||
               route->placement_epoch != placement.placement_epoch ||
               route->replicas != placement.replicas) {
      incompatible = true;
    }
  }
  if (incompatible)
    route.reset();
  return route;
}

[[nodiscard]] common::Status
validate_observation_shape(const raft::RaftGroupObservation& observation,
                           const raft::GroupId& expected_group, const raft::NodeId expected_node) {
  if (observation.group_id != expected_group || observation.node_id != expected_node ||
      observation.node_id == 0U || observation.current_term == 0U ||
      observation.last_log_index < observation.commit_index ||
      observation.commit_index < observation.applied_index || observation.joint_membership_active ||
      observation.joint_membership_can_finalize || observation.final_membership_pending ||
      observation.voters.empty() || observation.voters != observation.committed_voters ||
      !observation.joint_old_voters.empty() || !observation.joint_new_voters.empty()) {
    return unavailable("replicated query group authority is invalid or reconfiguring");
  }
  if (observation.role == raft::Role::kLeader) {
    if (observation.leader_id != observation.node_id ||
        !std::ranges::binary_search(observation.voters, observation.node_id))
      return unavailable("replicated query local leader authority is invalid");
  } else if (observation.role == raft::Role::kFollower) {
    if (!observation.leader_id.has_value() || *observation.leader_id == observation.node_id ||
        !std::ranges::binary_search(observation.voters, *observation.leader_id))
      return unavailable("replicated query remote leader authority is invalid");
  } else {
    return unavailable("replicated query group has no authoritative leader");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status validate_group_placement(const raft::MetadataCatalogSnapshot& catalog,
                                                      const raft::RaftGroupObservation& observation,
                                                      const raft::GroupId& metadata_group) {
  if (observation.group_id == metadata_group)
    return common::Status::ok();
  bool found{};
  for (const raft::TabletGroupBindingMetadata& binding : catalog.tablet_group_bindings) {
    if (binding.group_id != observation.group_id)
      continue;
    const auto placement = std::ranges::find(catalog.tablet_placements, binding.tablet_id,
                                             &raft::TabletPlacementMetadata::tablet_id);
    if (placement == catalog.tablet_placements.end())
      return corruption("replicated query group binding has no placement");
    if (placement->replicas != observation.voters)
      return unavailable("replicated query group membership differs from committed placement");
    found = true;
  }
  return found ? common::Status::ok()
               : corruption("replicated query resident group has no committed tablet binding");
}

[[nodiscard]] common::Status validate_mutable_group_authorities(
    const std::span<const query::DistributedVectorGroupReadAuthority> authorities) {
  if (authorities.empty() ||
      authorities.size() > query::DistributedPlanLimits{}.maximum_fragments ||
      !std::ranges::is_sorted(
          authorities, {}, [](const auto& authority) { return authority.observation.group_id; }) ||
      std::ranges::adjacent_find(authorities, {}, [](const auto& authority) {
        return authority.observation.group_id;
      }) != authorities.end()) {
    return invalid("mutable query group authority order is invalid");
  }
  for (const query::DistributedVectorGroupReadAuthority& authority : authorities) {
    const raft::RaftGroupObservation& observation = authority.observation;
    if (authority.barrier.group_id != observation.group_id || observation.group_id.is_nil() ||
        authority.barrier.barrier.term == 0U || authority.barrier.barrier.context == 0U ||
        authority.barrier.barrier.read_index == 0U || observation.node_id == 0U ||
        observation.role != raft::Role::kLeader || observation.leader_id != observation.node_id ||
        observation.current_term != authority.barrier.barrier.term ||
        observation.last_log_index < observation.commit_index ||
        observation.commit_index < observation.applied_index ||
        observation.commit_index < authority.barrier.barrier.read_index ||
        observation.joint_membership_active || observation.joint_membership_can_finalize ||
        observation.final_membership_pending || observation.voters.empty() ||
        observation.voters != observation.committed_voters ||
        !observation.joint_old_voters.empty() || !observation.joint_new_voters.empty() ||
        !std::ranges::is_sorted(observation.voters) || observation.voters.front() == 0U ||
        std::ranges::adjacent_find(observation.voters) != observation.voters.end() ||
        !std::ranges::binary_search(observation.voters, observation.node_id)) {
      return unavailable("mutable query group authority is invalid or reconfiguring");
    }
  }
  return common::Status::ok();
}

[[nodiscard]] const query::DistributedVectorGroupReadAuthority*
find_authority(const std::span<const query::DistributedVectorGroupReadAuthority> authorities,
               const raft::GroupId& group_id) noexcept {
  const auto found = std::ranges::lower_bound(
      authorities, group_id, {}, [](const query::DistributedVectorGroupReadAuthority& authority) {
        return authority.observation.group_id;
      });
  return found != authorities.end() && found->observation.group_id == group_id
             ? std::addressof(*found)
             : nullptr;
}

[[nodiscard]] common::Result<raft::RaftGroupObservation>
observe_group(raft::AsyncDurableMultiRaftRuntime& runtime, const raft::GroupId& group_id) {
  auto completion = runtime.try_observe_group(group_id);
  if (!completion.has_value())
    return common::make_unexpected(completion.error());
  auto results = completion->wait();
  if (!results.has_value())
    return common::make_unexpected(results.error());
  if (results->size() != 1U || !results->front().status.is_ok() ||
      results->front().transition.has_value() || !results->front().observation.has_value()) {
    return common::make_unexpected(corruption("replicated query group observation is malformed"));
  }
  return std::move(*results->front().observation);
}

[[nodiscard]] const raft::CatalogTableDefinition*
active_definition(const raft::MetadataCatalogSnapshot& catalog, const schema::TableId& table_id) {
  const auto active =
      std::ranges::find(catalog.active_schemas, table_id, &raft::ActiveSchemaMetadata::table_id);
  if (active == catalog.active_schemas.end())
    return nullptr;
  const auto definition =
      std::ranges::find_if(catalog.schema_definitions, [&](const auto& candidate) {
        return candidate.schema != nullptr && candidate.schema->schema_id() == active->schema_id &&
               candidate.schema->table_id() == table_id;
      });
  return definition == catalog.schema_definitions.end() ? nullptr : std::addressof(*definition);
}

[[nodiscard]] common::Result<raft::MetadataCatalogSnapshot>
recover_catalog(const ReplicatedIngestDatabaseConfig& config,
                const runtime::DatabaseBootstrap& bootstrap) {
  raft::RaftPersistentLogConfig log{.directory_path = bootstrap.raft_directory_path(),
                                    .target_segment_size =
                                        bootstrap.descriptor().raft_segment_target_bytes};
  auto runtime = raft::DurableMultiRaftRuntime::open_existing(
      bootstrap.descriptor().local_node_id, log, config.raft_recovery, config.groups,
      config.runtime_limits.durable);
  if (!runtime.has_value())
    return common::make_unexpected(runtime.error());
  common::Result<raft::DurableMetadataStateMachine> metadata = config.metadata_snapshots.has_value()
      ? [&]() -> common::Result<raft::DurableMetadataStateMachine> {
    auto storage = raft::MetadataSnapshotStorage::open_existing(*config.metadata_snapshots);
    if (!storage.has_value())
      return common::make_unexpected(storage.error());
    return raft::DurableMetadataStateMachine::recover(
        bootstrap.descriptor().metadata_group_id, *runtime, std::move(*storage),
        config.metadata_limits, config.metadata_codec_limits, config.schema_codec_limits);
  }()
      : raft::DurableMetadataStateMachine::recover(
            bootstrap.descriptor().metadata_group_id, *runtime, config.metadata_limits,
            config.metadata_codec_limits, config.schema_codec_limits);
  if (!metadata.has_value()) {
    static_cast<void>(runtime->close());
    return common::make_unexpected(metadata.error());
  }
  auto catalog = metadata->state().catalog_snapshot();
  const common::Status closed = runtime->close();
  if (!catalog.has_value())
    return common::make_unexpected(catalog.error());
  if (!closed.is_ok())
    return common::make_unexpected(closed);
  return catalog;
}

[[nodiscard]] common::Result<std::vector<std::shared_ptr<const schema::TableSchema>>>
retained_schemas(const raft::MetadataCatalogSnapshot& catalog, const schema::TableId& table_id) {
  try {
    std::vector<std::shared_ptr<const schema::TableSchema>> schemas;
    for (const raft::CatalogTableDefinition& definition : catalog.schema_definitions) {
      if (definition.schema != nullptr && definition.schema->table_id() == table_id)
        schemas.push_back(definition.schema);
    }
    std::ranges::sort(schemas, {}, [](const auto& schema) { return schema->version().value(); });
    if (schemas.empty())
      return common::make_unexpected(corruption("replicated tablet has no retained schema"));
    for (std::size_t index = 1U; index < schemas.size(); ++index) {
      if (schemas[index]->version().value() != schemas[index - 1U]->version().value() + 1U)
        return common::make_unexpected(
            corruption("replicated tablet schema lineage is not consecutive"));
    }
    return schemas;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("replicated schema projection allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("replicated schema projection exceeds limits"));
  }
}

[[nodiscard]] common::Result<schema::SchemaLineage>
retained_lineage(const raft::MetadataCatalogSnapshot& catalog, const schema::TableId& table_id) {
  auto schemas = retained_schemas(catalog, table_id);
  if (!schemas.has_value())
    return common::make_unexpected(schemas.error());
  auto lineage = schema::SchemaLineage::create(**schemas->begin());
  if (!lineage.has_value())
    return common::make_unexpected(corruption("replicated query lineage root is invalid"));
  for (std::size_t index = 1U; index < schemas->size(); ++index) {
    const common::Status appended = lineage->append(*(*schemas)[index]);
    if (!appended.is_ok())
      return common::make_unexpected(corruption("replicated query lineage is invalid"));
  }
  return lineage;
}

[[nodiscard]] head::MutableHeadCapacity
head_capacity(const schema::TableSchema& schema,
              const runtime::DatabaseBootstrapDescriptor& bootstrap) {
  head::MutableHeadCapacity capacity;
  capacity.row_capacity = bootstrap.mutable_head_rows;
  capacity.variable_value_bytes.reserve(schema.columns().size());
  for (const schema::ColumnDefinition& column : schema.columns()) {
    capacity.variable_value_bytes.push_back(
        column.type().is_variable_width()
            ? static_cast<std::size_t>(bootstrap.variable_column_bytes)
            : 0U);
  }
  return capacity;
}

[[nodiscard]] common::Result<std::vector<ingest::AsyncRaftTabletApplicationConfig>>
build_tablets(const ReplicatedIngestDatabaseConfig& config,
              const runtime::DatabaseBootstrapDescriptor& bootstrap,
              const raft::MetadataCatalogSnapshot& catalog) {
  try {
    for (std::size_t index = 0U; index < config.tablet_snapshots.size(); ++index) {
      const auto& snapshot = config.tablet_snapshots[index];
      if (snapshot.group_id.is_nil() || snapshot.group_id == bootstrap.metadata_group_id ||
          find_group(config.groups, snapshot.group_id) == nullptr)
        return common::make_unexpected(
            invalid("tablet snapshot storage names an unconfigured group"));
      for (std::size_t previous = 0U; previous < index; ++previous) {
        if (config.tablet_snapshots[previous].group_id == snapshot.group_id)
          return common::make_unexpected(
              invalid("tablet snapshot storage repeats a configured group"));
      }
    }
    std::vector<ingest::AsyncRaftTabletApplicationConfig> tablets;
    tablets.reserve(config.groups.size() - 1U);
    for (const raft::TabletGroupBindingMetadata& binding : catalog.tablet_group_bindings) {
      const raft::RaftGroupConfiguration* group = find_group(config.groups, binding.group_id);
      const auto placement = std::ranges::find(catalog.tablet_placements, binding.tablet_id,
                                               &raft::TabletPlacementMetadata::tablet_id);
      if (placement == catalog.tablet_placements.end())
        return common::make_unexpected(corruption("committed tablet binding has no placement"));
      if (group == nullptr) {
        if (std::ranges::binary_search(placement->replicas, bootstrap.local_node_id))
          return common::make_unexpected(
              invalid("locally placed tablet group is missing from node configuration"));
        continue;
      }
      if (binding.group_id == bootstrap.metadata_group_id)
        return common::make_unexpected(
            invalid("committed tablet binding aliases the metadata group"));
      const raft::CatalogTableDefinition* definition =
          active_definition(catalog, placement->table_id);
      if (definition == nullptr)
        return common::make_unexpected(
            corruption("replicated tablet has no committed active schema definition"));
      const auto policy = std::ranges::find(catalog.table_policies, placement->table_id,
                                            &raft::TablePolicyMetadata::table_id);
      if (policy == catalog.table_policies.end() || policy->retry_retention_positions == 0U)
        return common::make_unexpected(
            invalid("replicated tablet has no complete committed retry policy"));
      auto schemas = retained_schemas(catalog, placement->table_id);
      if (!schemas.has_value() || schemas->back()->schema_id() != definition->schema->schema_id())
        return common::make_unexpected(
            schemas.has_value() ? corruption("active schema is not the retained lineage tail")
                                : schemas.error());
      const std::size_t retry_limit = static_cast<std::size_t>(
          std::min(policy->retry_retention_positions, bootstrap.maximum_retry_entries));
      auto tablet = ingest::TabletState::create(
          schemas->front(), binding.tablet_id,
          {.head_capacity = head_capacity(**schemas->begin(), bootstrap),
           .maximum_schema_versions = schemas->size(),
           .maximum_sealed_generations = bootstrap.maximum_sealed_generations,
           .maximum_retry_entries = retry_limit});
      auto retries = ingest::RetryDirectory::create({.maximum_entries = retry_limit});
      if (!tablet.has_value())
        return common::make_unexpected(tablet.error());
      if (!retries.has_value())
        return common::make_unexpected(retries.error());
      for (std::size_t index = 1U; index < schemas->size(); ++index) {
        common::Status registered = tablet->register_schema(
            (*schemas)[index], head_capacity(*(*schemas)[index], bootstrap));
        if (!registered.is_ok())
          return common::make_unexpected(std::move(registered));
      }
      std::optional<ingest::RaftTabletSnapshotStorage> snapshot_storage;
      const auto snapshot = std::ranges::find(config.tablet_snapshots, binding.group_id,
                                              &ingest::RaftTabletSnapshotStorageConfig::group_id);
      if (snapshot != config.tablet_snapshots.end()) {
        auto opened = ingest::RaftTabletSnapshotStorage::open_existing(*snapshot);
        if (!opened.has_value())
          return common::make_unexpected(opened.error());
        snapshot_storage.emplace(std::move(*opened));
      }
      tablets.push_back({.group_id = binding.group_id,
                         .snapshot_storage = std::move(snapshot_storage),
                         .retry_directory = std::move(*retries),
                         .tablet = std::move(*tablet),
                         .retained_schemas = std::move(*schemas),
                         .decode_limits = config.columnar_append_limits});
    }
    for (const raft::RaftGroupConfiguration& group : config.groups) {
      if (group.group_id == bootstrap.metadata_group_id)
        continue;
      if (!std::ranges::any_of(
              tablets, [&](const auto& tablet) { return tablet.group_id == group.group_id; }))
        return common::make_unexpected(
            invalid("configured tablet group has no committed application binding"));
    }
    return tablets;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("replicated tablet projection allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("replicated tablet projection exceeds limits"));
  }
}

} // namespace

class ReplicatedQuerySnapshot::Impl {
public:
  struct Table {
    schema::SchemaLineage lineage;
    std::vector<ingest::TabletSnapshot> tablets;
    std::optional<ReplicatedSingleGroupQueryRoute> single_group;
    bool complete_residency{};
  };

  Impl(const manifest::DatabaseId configured_database_id,
       std::shared_ptr<const query::QueryCatalogSnapshot> configured_catalog,
       std::shared_ptr<const raft::MetadataCatalogSnapshot> configured_metadata,
       std::vector<Table> configured_tables) noexcept
      : database_id(configured_database_id), catalog(std::move(configured_catalog)),
        metadata(std::move(configured_metadata)), tables(std::move(configured_tables)) {}

  manifest::DatabaseId database_id;
  std::shared_ptr<const query::QueryCatalogSnapshot> catalog;
  std::shared_ptr<const raft::MetadataCatalogSnapshot> metadata;
  std::vector<Table> tables;
};

ReplicatedQuerySnapshot::ReplicatedQuerySnapshot(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

ReplicatedQuerySnapshot::~ReplicatedQuerySnapshot() = default;
ReplicatedQuerySnapshot::ReplicatedQuerySnapshot(ReplicatedQuerySnapshot&&) noexcept = default;
ReplicatedQuerySnapshot&
ReplicatedQuerySnapshot::operator=(ReplicatedQuerySnapshot&&) noexcept = default;

const std::shared_ptr<const query::QueryCatalogSnapshot>&
ReplicatedQuerySnapshot::catalog() const noexcept {
  return impl_->catalog;
}

const ReplicatedSingleGroupQueryRoute*
ReplicatedQuerySnapshot::single_group_route(const schema::TableId& table_id) const noexcept {
  const auto table = std::ranges::find_if(impl_->tables, [&](const Impl::Table& candidate) {
    return candidate.lineage.table_id() == table_id;
  });
  return table != impl_->tables.end() && table->single_group.has_value()
             ? std::addressof(*table->single_group)
             : nullptr;
}

common::Result<std::unique_ptr<query::PhysicalOperator>>
ReplicatedQuerySnapshot::instantiate_table_pipeline(
    const query::QueryResourceContext& resources, const schema::TableId& table_id,
    const schema::SchemaId& destination_schema_id, const query::PhysicalPipelinePlan& pipeline,
    const query::TabletStatePipelineLimits limits) const {
  if (impl_ == nullptr)
    return common::make_unexpected(invalid("replicated query snapshot was moved from"));
  const auto table = std::ranges::find_if(impl_->tables, [&](const Impl::Table& candidate) {
    return candidate.lineage.table_id() == table_id;
  });
  if (table == impl_->tables.end())
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "replicated query table is not catalogued"});
  if (!table->complete_residency) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kUnavailable,
        "replicated query requires a distributed read because the table is not fully resident"});
  }
  if (table->tablets.empty())
    return common::make_unexpected(invalid("replicated query table has no tablet publication"));
  return query::instantiate_tablet_states_pipeline(resources, table->tablets, table->lineage,
                                                   destination_schema_id, pipeline, limits);
}

common::Result<std::vector<query::DistributedMutableVectorFragment>>
ReplicatedQuerySnapshot::bind_linearizable_mutable_vector_fragments(
    const ReplicatedMutableVectorQueryBinding& binding) const {
  if (impl_ == nullptr)
    return common::make_unexpected(invalid("replicated query snapshot was moved from"));
  const query::DistributedVectorQueryPlan& plan = binding.plan.get();
  if (plan.read_policy.consistency != query::DistributedReadConsistency::kLeaderLinearizable ||
      plan.query_id.is_nil() || plan.fragments.empty() ||
      plan.fragments.size() > query::DistributedPlanLimits{}.maximum_fragments) {
    return common::make_unexpected(
        invalid("mutable query snapshot binding requires a linearizable nonempty plan"));
  }
  const common::Status authorities = validate_mutable_group_authorities(binding.group_authorities);
  if (!authorities.is_ok())
    return common::make_unexpected(authorities);
  const auto table = std::ranges::find_if(impl_->tables, [&](const Impl::Table& candidate) {
    return candidate.lineage.table_id() == binding.table_id;
  });
  if (table == impl_->tables.end())
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "mutable query table is not catalogued"});
  if (table->lineage.current() == nullptr)
    return common::make_unexpected(corruption("mutable query table lineage has no current schema"));

  try {
    std::vector<query::DistributedMutableVectorFragment> fragments;
    fragments.reserve(plan.fragments.size());
    std::set<schema::TabletId> seen;
    for (const query::DistributedTablet& planned : plan.fragments) {
      if (!seen.insert(planned.tablet_id).second)
        return common::make_unexpected(invalid("mutable query plan repeats a tablet"));
      const auto snapshot = std::ranges::lower_bound(table->tablets, planned.tablet_id, {},
                                                     &ingest::TabletSnapshot::tablet_id);
      if (snapshot == table->tablets.end() || snapshot->tablet_id() != planned.tablet_id) {
        return common::make_unexpected(
            unavailable("mutable query selected tablet has no resident pinned publication"));
      }
      const auto placement =
          std::ranges::lower_bound(impl_->metadata->tablet_placements, planned.tablet_id, {},
                                   &raft::TabletPlacementMetadata::tablet_id);
      const auto group =
          std::ranges::lower_bound(impl_->metadata->tablet_group_bindings, planned.tablet_id, {},
                                   &raft::TabletGroupBindingMetadata::tablet_id);
      if (placement == impl_->metadata->tablet_placements.end() ||
          group == impl_->metadata->tablet_group_bindings.end() ||
          placement->tablet_id != planned.tablet_id || group->tablet_id != planned.tablet_id ||
          placement->table_id != binding.table_id) {
        return common::make_unexpected(
            corruption("mutable query tablet metadata is incomplete or inconsistent"));
      }
      const query::DistributedVectorGroupReadAuthority* const authority =
          find_authority(binding.group_authorities, group->group_id);
      if (authority == nullptr)
        return common::make_unexpected(unavailable("mutable query tablet authority is missing"));
      if (authority->observation.voters != placement->replicas)
        return common::make_unexpected(
            unavailable("mutable query group membership differs from committed placement"));
      const std::optional<head::HeadCommitPosition>& position = snapshot->applied_position();
      if (!position.has_value() || position->source != head::CommitSource::kRaft ||
          position->raft_group_id != group->group_id ||
          position->record_sequence < authority->barrier.barrier.read_index ||
          planned.leader_node != authority->observation.node_id ||
          planned.local_applied_position != position->record_sequence ||
          planned.known_leader_commit_position != authority->observation.commit_index) {
        return common::make_unexpected(
            unavailable("mutable query plan differs from pinned publication authority"));
      }
      const query::DistributedReadAdmission admission{
          .tablet_id = planned.tablet_id,
          .serving_node = authority->observation.node_id,
          .applied_position = position->record_sequence,
          .observed_leader_commit_position = authority->observation.commit_index,
          .linearizable_barrier = authority->barrier.barrier};
      auto fragment = query::bind_distributed_mutable_vector_fragment(
          {.plan = std::cref(plan),
           .admission = std::cref(admission),
           .database_id = impl_->database_id,
           .snapshot = std::cref(*snapshot),
           .lineage = std::cref(table->lineage),
           .raft_group_id = group->group_id,
           .placement = std::cref(*placement),
           .destination_column_ordinals = binding.destination_column_ordinals,
           .event_time_predicate = binding.event_time_predicate,
           .result_schema = binding.result_schema});
      if (!fragment.has_value())
        return common::make_unexpected(fragment.error());
      fragments.push_back(std::move(*fragment));
    }
    return fragments;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("mutable query snapshot binding allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("mutable query snapshot binding exceeds limits"));
  }
}

common::Result<ReplicatedRoutedMutableVectorQuery>
ReplicatedQuerySnapshot::bind_and_resolve_linearizable_mutable_vector_query(
    const ReplicatedMutableVectorQueryBinding& binding,
    const std::span<const cluster::DistributedQueryNodeTlsContext> tls_contexts,
    const cluster::DistributedQueryRouteResolutionLimits limits) const {
  if (impl_ == nullptr)
    return common::make_unexpected(invalid("replicated query snapshot was moved from"));
  auto fragments = bind_linearizable_mutable_vector_fragments(binding);
  if (!fragments.has_value())
    return common::make_unexpected(fragments.error());
  auto routes = cluster::resolve_distributed_query_node_routes(
      *impl_->metadata, std::span<const query::DistributedMutableVectorFragment>{*fragments},
      tls_contexts, limits);
  if (!routes.has_value())
    return common::make_unexpected(routes.error());
  return ReplicatedRoutedMutableVectorQuery{.fragments = std::move(*fragments),
                                            .routes = std::move(*routes)};
}

class ReplicatedIngestDatabase::Impl {
public:
  Impl(runtime::DatabaseBootstrap configured_bootstrap, ReplicatedIngestRuntime configured_runtime,
       std::vector<raft::GroupId> configured_resident_groups)
      : bootstrap_owner(std::move(configured_bootstrap)), runtime(std::move(configured_runtime)),
        resident_groups(std::move(configured_resident_groups)) {
    query_groups.reserve(resident_groups.size() + 1U);
    query_groups.push_back(bootstrap_owner.descriptor().metadata_group_id);
    query_groups.insert(query_groups.end(), resident_groups.begin(), resident_groups.end());
    std::ranges::sort(query_groups);
  }

  runtime::DatabaseBootstrap bootstrap_owner;
  ReplicatedIngestRuntime runtime;
  std::vector<raft::GroupId> resident_groups;
  std::vector<raft::GroupId> query_groups;
  bool shutdown_complete{};
  common::Status shutdown_status;
};

ReplicatedIngestDatabase::ReplicatedIngestDatabase(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

ReplicatedIngestDatabase::~ReplicatedIngestDatabase() {
  try {
    if (impl_ != nullptr)
      static_cast<void>(shutdown());
  } catch (...) { // NOLINT(bugprone-empty-catch)
    // Explicit shutdown reports failures; destruction is necessarily best-effort.
  }
}

ReplicatedIngestDatabase::ReplicatedIngestDatabase(ReplicatedIngestDatabase&&) noexcept = default;
ReplicatedIngestDatabase&
ReplicatedIngestDatabase::operator=(ReplicatedIngestDatabase&&) noexcept = default;

common::Result<ReplicatedIngestDatabase>
ReplicatedIngestDatabase::open_existing(ReplicatedIngestDatabaseConfig config) {
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(config.bootstrap);
  if (!bootstrap.has_value())
    return common::make_unexpected(bootstrap.error());
  observe_startup(config, ReplicatedIngestDatabaseStartupStage::kRootOwnerReady);
  const runtime::DatabaseBootstrapDescriptor descriptor = bootstrap->descriptor();
  if (config.groups.empty() || find_group(config.groups, descriptor.metadata_group_id) == nullptr)
    return common::make_unexpected(
        invalid("replicated group configuration omits the metadata group"));
  if (config.metadata_snapshots.has_value() &&
      config.metadata_snapshots->group_id != descriptor.metadata_group_id)
    return common::make_unexpected(invalid("metadata snapshot storage names a different group"));
  auto catalog = recover_catalog(config, *bootstrap);
  if (!catalog.has_value())
    return common::make_unexpected(catalog.error());
  observe_startup(config, ReplicatedIngestDatabaseStartupStage::kCatalogRecovered);
  auto tablets = build_tablets(config, descriptor, *catalog);
  if (!tablets.has_value())
    return common::make_unexpected(tablets.error());
  std::vector<raft::GroupId> resident_groups;
  try {
    resident_groups.reserve(config.groups.size() - 1U);
    for (const raft::RaftGroupConfiguration& group : config.groups) {
      if (group.group_id != descriptor.metadata_group_id)
        resident_groups.push_back(group.group_id);
    }
    std::ranges::sort(resident_groups);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("replicated resident-group allocation failed"));
  }
  observe_startup(config, ReplicatedIngestDatabaseStartupStage::kTabletOwnersPrepared);
  std::optional<raft::MetadataSnapshotStorage> metadata_snapshots;
  if (config.metadata_snapshots.has_value()) {
    auto opened = raft::MetadataSnapshotStorage::open_existing(*config.metadata_snapshots);
    if (!opened.has_value())
      return common::make_unexpected(opened.error());
    metadata_snapshots.emplace(std::move(*opened));
  }
  ReplicatedIngestRuntimeConfig runtime_config{
      .local_node_id = descriptor.local_node_id,
      .log = {.directory_path = bootstrap->raft_directory_path(),
              .target_segment_size = descriptor.raft_segment_target_bytes},
      .groups = std::move(config.groups),
      .tablets = std::move(*tablets),
      .metadata = {.group_id = descriptor.metadata_group_id,
                   .snapshot_storage = std::move(metadata_snapshots),
                   .state_limits = config.metadata_limits,
                   .codec_limits = config.metadata_codec_limits,
                   .schema_codec_limits = config.schema_codec_limits},
      .runtime_limits = config.runtime_limits,
      .application_limits = config.application_limits,
      .coordinator_limits = config.coordinator_limits};
  auto ingest_runtime =
      ReplicatedIngestRuntime::open_existing(std::move(runtime_config), config.raft_recovery);
  if (!ingest_runtime.has_value())
    return common::make_unexpected(ingest_runtime.error());
  observe_startup(config, ReplicatedIngestDatabaseStartupStage::kRuntimeReady);
  try {
    return ReplicatedIngestDatabase{std::make_unique<Impl>(
        std::move(*bootstrap), std::move(*ingest_runtime), std::move(resident_groups))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("replicated database owner allocation failed"));
  }
}

const runtime::DatabaseBootstrapDescriptor& ReplicatedIngestDatabase::bootstrap() const noexcept {
  return impl_->bootstrap_owner.descriptor();
}

ReplicatedIngestRuntime* ReplicatedIngestDatabase::ingest_runtime() noexcept {
  return is_running() ? std::addressof(impl_->runtime) : nullptr;
}

common::Result<ReplicatedQuerySnapshot> ReplicatedIngestDatabase::acquire_query_snapshot() const {
  return acquire_query_snapshot({});
}

common::Result<ReplicatedQuerySnapshot> ReplicatedIngestDatabase::acquire_query_snapshot(
    const std::span<const raft::GroupReadBarrier> barriers) const {
  if (!is_running())
    return common::make_unexpected(invalid("replicated query database is unavailable"));
  if (!barriers.empty()) {
    if (barriers.size() != impl_->query_groups.size())
      return common::make_unexpected(
          invalid("replicated query barrier vector has the wrong group count"));
    for (const raft::GroupId& group_id : impl_->query_groups) {
      const raft::GroupReadBarrier* barrier = find_barrier(barriers, group_id);
      if (barrier == nullptr || barrier->barrier.term == 0U || barrier->barrier.context == 0U ||
          barrier->barrier.read_index == 0U ||
          std::ranges::count(barriers, group_id, &raft::GroupReadBarrier::group_id) != 1U) {
        return common::make_unexpected(
            invalid("replicated query barrier vector identity is invalid"));
      }
    }
  }
  raft::AsyncRaftMetadataApplication* const metadata = impl_->runtime.metadata_application();
  ingest::AsyncRaftTabletApplication* const tablets = impl_->runtime.tablet_application();
  if (metadata == nullptr || tablets == nullptr)
    return common::make_unexpected(invalid("replicated query applications are unavailable"));
  auto database_id =
      manifest::DatabaseId::from_uuid(impl_->bootstrap_owner.descriptor().database_id);
  if (!database_id.has_value())
    return common::make_unexpected(corruption("replicated query database identity is invalid"));
  auto catalog = metadata->catalog_snapshot();
  if (!catalog.has_value())
    return common::make_unexpected(catalog.error());
  if (!barriers.empty()) {
    const raft::GroupReadBarrier* metadata_barrier =
        find_barrier(barriers, impl_->bootstrap_owner.descriptor().metadata_group_id);
    if (metadata_barrier == nullptr ||
        (*catalog)->applied_index < metadata_barrier->barrier.read_index)
      return common::make_unexpected(
          unavailable("replicated metadata publication trails its confirmed read barrier"));
  }
  try {
    std::vector<query::QueryCatalogTableInput> inputs;
    std::vector<ReplicatedQuerySnapshot::Impl::Table> tables;
    inputs.reserve((*catalog)->active_schemas.size());
    tables.reserve((*catalog)->active_schemas.size());
    for (const raft::ActiveSchemaMetadata& active : (*catalog)->active_schemas) {
      const raft::CatalogTableDefinition* definition =
          active_definition(**catalog, active.table_id);
      if (definition == nullptr || definition->schema == nullptr)
        return common::make_unexpected(corruption("replicated query active schema is incomplete"));
      auto lineage = retained_lineage(**catalog, active.table_id);
      if (!lineage.has_value() || lineage->current()->schema_id() != active.schema_id) {
        return common::make_unexpected(
            lineage.has_value() ? corruption("replicated query active schema is not lineage tail")
                                : lineage.error());
      }
      auto query_route = single_group_route(**catalog, active.table_id);
      if (!query_route.has_value())
        return common::make_unexpected(query_route.error());
      std::vector<ingest::TabletSnapshot> pinned;
      bool complete_residency = true;
      std::size_t placement_count{};
      for (const raft::TabletPlacementMetadata& placement : (*catalog)->tablet_placements) {
        if (placement.table_id != active.table_id)
          continue;
        ++placement_count;
        const auto binding =
            std::ranges::find((*catalog)->tablet_group_bindings, placement.tablet_id,
                              &raft::TabletGroupBindingMetadata::tablet_id);
        if (binding == (*catalog)->tablet_group_bindings.end())
          return common::make_unexpected(
              corruption("replicated query tablet has no group binding"));
        if (!std::ranges::binary_search(impl_->resident_groups, binding->group_id)) {
          complete_residency = false;
          continue;
        }
        auto snapshot = tablets->snapshot(binding->group_id);
        if (!snapshot.has_value())
          return common::make_unexpected(snapshot.error());
        if (!barriers.empty()) {
          const raft::GroupReadBarrier* tablet_barrier = find_barrier(barriers, binding->group_id);
          const std::optional<head::HeadCommitPosition>& position = snapshot->applied_position();
          if (tablet_barrier == nullptr || !position.has_value() ||
              position->source != head::CommitSource::kRaft ||
              position->raft_group_id != binding->group_id ||
              position->record_sequence < tablet_barrier->barrier.read_index) {
            return common::make_unexpected(
                unavailable("replicated tablet publication trails its confirmed read barrier"));
          }
        }
        if (snapshot->tablet_id() != placement.tablet_id ||
            snapshot->table_id() != active.table_id ||
            lineage->find(snapshot->schema_ptr()->schema_id()) == nullptr) {
          return common::make_unexpected(
              corruption("replicated query tablet publication disagrees with committed metadata"));
        }
        pinned.push_back(std::move(*snapshot));
      }
      if (placement_count == 0U)
        complete_residency = false;
      std::ranges::sort(pinned, {}, &ingest::TabletSnapshot::tablet_id);
      inputs.push_back(
          {.name = definition->name, .quoted = definition->quoted, .schema = definition->schema});
      tables.push_back({.lineage = std::move(*lineage),
                        .tablets = std::move(pinned),
                        .single_group = std::move(*query_route),
                        .complete_residency = complete_residency});
    }
    auto query_catalog = query::QueryCatalogSnapshot::create(
        std::max<std::uint64_t>(1U, (*catalog)->applied_index), inputs);
    if (!query_catalog.has_value())
      return common::make_unexpected(query_catalog.error().status());
    auto shared_catalog =
        std::make_shared<const query::QueryCatalogSnapshot>(std::move(*query_catalog));
    return ReplicatedQuerySnapshot{std::make_unique<ReplicatedQuerySnapshot::Impl>(
        *database_id, std::move(shared_catalog), std::move(*catalog), std::move(tables))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("replicated query snapshot allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("replicated query snapshot exceeds limits"));
  }
}

std::span<const raft::GroupId> ReplicatedIngestDatabase::query_barrier_groups() const noexcept {
  return is_running() ? std::span<const raft::GroupId>{impl_->query_groups}
                      : std::span<const raft::GroupId>{};
}

common::Result<std::optional<ReplicatedQueryLeaderRoute>>
ReplicatedIngestDatabase::resolve_query_leader(const ReplicatedSingleGroupQueryRoute& route) {
  if (!is_running())
    return common::make_unexpected(invalid("replicated query database is unavailable"));
  if (route.group_id.is_nil() || route.placement_epoch == 0U || route.replicas.empty() ||
      !std::ranges::is_sorted(route.replicas) ||
      std::ranges::adjacent_find(route.replicas) != route.replicas.end() ||
      std::ranges::any_of(route.replicas, [](const raft::NodeId node) { return node == 0U; })) {
    return common::make_unexpected(invalid("replicated single-group query route is invalid"));
  }
  raft::AsyncDurableMultiRaftRuntime* const runtime = impl_->runtime.runtime();
  raft::AsyncRaftMetadataApplication* const metadata = impl_->runtime.metadata_application();
  if (runtime == nullptr || metadata == nullptr)
    return common::make_unexpected(invalid("replicated query authority is unavailable"));
  try {
    std::vector<raft::RaftGroupObservation> observations;
    observations.reserve(impl_->query_groups.size());
    for (const raft::GroupId& group_id : impl_->query_groups) {
      auto observed = observe_group(*runtime, group_id);
      if (!observed.has_value())
        return common::make_unexpected(observed.error());
      observations.push_back(std::move(*observed));
    }

    auto catalog = metadata->catalog_snapshot();
    if (!catalog.has_value())
      return common::make_unexpected(catalog.error());
    auto current_route = single_group_route(**catalog, route.table_id);
    if (!current_route.has_value())
      return common::make_unexpected(current_route.error());
    if (!current_route->has_value() || **current_route != route)
      return common::make_unexpected(
          unavailable("replicated single-group query route changed during observation"));

    const auto& descriptor = impl_->bootstrap_owner.descriptor();
    std::optional<raft::NodeId> remote_leader;
    const raft::RaftGroupObservation* table_observation{};
    bool saw_local_leader{};
    for (std::size_t index = 0U; index < observations.size(); ++index) {
      const raft::RaftGroupObservation& observation = observations[index];
      const common::Status shaped = validate_observation_shape(
          observation, impl_->query_groups[index], descriptor.local_node_id);
      if (!shaped.is_ok())
        return common::make_unexpected(shaped);
      const common::Status placed =
          validate_group_placement(**catalog, observation, descriptor.metadata_group_id);
      if (!placed.is_ok())
        return common::make_unexpected(placed);
      if (observation.group_id == route.group_id)
        table_observation = std::addressof(observation);
      if (observation.role == raft::Role::kLeader) {
        saw_local_leader = true;
      } else if (!remote_leader.has_value()) {
        remote_leader = observation.leader_id;
      } else if (remote_leader != observation.leader_id) {
        return common::make_unexpected(
            unavailable("replicated query groups have different remote leaders"));
      }
    }
    if (table_observation == nullptr)
      return common::make_unexpected(
          corruption("replicated query route group is absent from the barrier vector"));
    if (table_observation->voters != route.replicas)
      return common::make_unexpected(
          unavailable("replicated query route membership changed during observation"));
    if (saw_local_leader && remote_leader.has_value())
      return common::make_unexpected(
          unavailable("replicated query groups have split local and remote leadership"));
    if (saw_local_leader)
      return std::optional<ReplicatedQueryLeaderRoute>{};
    if (!remote_leader.has_value() || table_observation->leader_id != remote_leader)
      return common::make_unexpected(
          unavailable("replicated query route has no common remote leader"));
    return std::optional<ReplicatedQueryLeaderRoute>{
        ReplicatedQueryLeaderRoute{.group_id = route.group_id,
                                   .leader_node_id = *remote_leader,
                                   .leader_term = table_observation->current_term,
                                   .placement_epoch = route.placement_epoch}};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("replicated query authority allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("replicated query authority exceeds limits"));
  }
}

bool ReplicatedIngestDatabase::is_running() const noexcept {
  return impl_ != nullptr && !impl_->shutdown_complete && impl_->runtime.is_running();
}

common::Status ReplicatedIngestDatabase::shutdown() {
  return shutdown_with(nullptr);
}

common::Status
ReplicatedIngestDatabase::shutdown(ReplicatedIngestDatabaseShutdownObserver& observer) {
  return shutdown_with(std::addressof(observer));
}

common::Status
ReplicatedIngestDatabase::shutdown_with(ReplicatedIngestDatabaseShutdownObserver* const observer) {
  if (impl_ == nullptr)
    return invalid("replicated database was moved from");
  if (impl_->shutdown_complete)
    return impl_->shutdown_status;
  if (observer != nullptr) {
    DatabaseRuntimeShutdownObserver runtime_observer{*observer};
    impl_->shutdown_status = impl_->runtime.shutdown(runtime_observer);
  } else {
    impl_->shutdown_status = impl_->runtime.shutdown();
  }
  const common::Status closed = impl_->bootstrap_owner.close();
  if (impl_->shutdown_status.is_ok())
    impl_->shutdown_status = closed;
  if (observer != nullptr)
    observer->on_shutdown_stage(ReplicatedIngestDatabaseShutdownStage::kRootReleased);
  impl_->shutdown_complete = true;
  return impl_->shutdown_status;
}

} // namespace chronos::service
