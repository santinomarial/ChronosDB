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
    bool complete_residency{};
  };

  Impl(std::shared_ptr<const query::QueryCatalogSnapshot> configured_catalog,
       std::vector<Table> configured_tables) noexcept
      : catalog(std::move(configured_catalog)), tables(std::move(configured_tables)) {}

  std::shared_ptr<const query::QueryCatalogSnapshot> catalog;
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
                        .complete_residency = complete_residency});
    }
    auto query_catalog = query::QueryCatalogSnapshot::create(
        std::max<std::uint64_t>(1U, (*catalog)->applied_index), inputs);
    if (!query_catalog.has_value())
      return common::make_unexpected(query_catalog.error().status());
    auto shared_catalog =
        std::make_shared<const query::QueryCatalogSnapshot>(std::move(*query_catalog));
    return ReplicatedQuerySnapshot{std::make_unique<ReplicatedQuerySnapshot::Impl>(
        std::move(shared_catalog), std::move(tables))};
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
