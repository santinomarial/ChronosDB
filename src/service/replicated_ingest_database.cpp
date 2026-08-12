#include "chronos/service/replicated_ingest_database.hpp"

#include "chronos/ingest/retry_directory.hpp"
#include "chronos/ingest/tablet_state.hpp"
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
      head::MutableHeadCapacity capacity;
      capacity.row_capacity = bootstrap.mutable_head_rows;
      capacity.variable_value_bytes.reserve(definition->schema->columns().size());
      for (const schema::ColumnDefinition& column : definition->schema->columns()) {
        capacity.variable_value_bytes.push_back(
            column.type().is_variable_width()
                ? static_cast<std::size_t>(bootstrap.variable_column_bytes)
                : 0U);
      }
      const std::size_t retry_limit = static_cast<std::size_t>(
          std::min(policy->retry_retention_positions, bootstrap.maximum_retry_entries));
      auto tablet = ingest::TabletState::create(
          definition->schema, binding.tablet_id,
          {.head_capacity = std::move(capacity),
           .maximum_schema_versions = schemas->size(),
           .maximum_sealed_generations = bootstrap.maximum_sealed_generations,
           .maximum_retry_entries = retry_limit});
      auto retries = ingest::RetryDirectory::create({.maximum_entries = retry_limit});
      if (!tablet.has_value())
        return common::make_unexpected(tablet.error());
      if (!retries.has_value())
        return common::make_unexpected(retries.error());
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

class ReplicatedIngestDatabase::Impl {
public:
  Impl(runtime::DatabaseBootstrap configured_bootstrap,
       ReplicatedIngestRuntime configured_runtime) noexcept
      : bootstrap_owner(std::move(configured_bootstrap)), runtime(std::move(configured_runtime)) {}

  runtime::DatabaseBootstrap bootstrap_owner;
  ReplicatedIngestRuntime runtime;
  bool shutdown_complete{};
  common::Status shutdown_status;
};

ReplicatedIngestDatabase::ReplicatedIngestDatabase(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

ReplicatedIngestDatabase::~ReplicatedIngestDatabase() {
  if (impl_ != nullptr)
    static_cast<void>(shutdown());
}

ReplicatedIngestDatabase::ReplicatedIngestDatabase(ReplicatedIngestDatabase&&) noexcept = default;
ReplicatedIngestDatabase&
ReplicatedIngestDatabase::operator=(ReplicatedIngestDatabase&&) noexcept = default;

common::Result<ReplicatedIngestDatabase>
ReplicatedIngestDatabase::open_existing(ReplicatedIngestDatabaseConfig config) {
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(config.bootstrap);
  if (!bootstrap.has_value())
    return common::make_unexpected(bootstrap.error());
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
  auto tablets = build_tablets(config, descriptor, *catalog);
  if (!tablets.has_value())
    return common::make_unexpected(tablets.error());
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
  try {
    return ReplicatedIngestDatabase{
        std::make_unique<Impl>(std::move(*bootstrap), std::move(*ingest_runtime))};
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

bool ReplicatedIngestDatabase::is_running() const noexcept {
  return impl_ != nullptr && !impl_->shutdown_complete && impl_->runtime.is_running();
}

common::Status ReplicatedIngestDatabase::shutdown() {
  if (impl_ == nullptr)
    return invalid("replicated database was moved from");
  if (impl_->shutdown_complete)
    return impl_->shutdown_status;
  impl_->shutdown_status = impl_->runtime.shutdown();
  const common::Status closed = impl_->bootstrap_owner.close();
  if (impl_->shutdown_status.is_ok())
    impl_->shutdown_status = closed;
  impl_->shutdown_complete = true;
  return impl_->shutdown_status;
}

} // namespace chronos::service
