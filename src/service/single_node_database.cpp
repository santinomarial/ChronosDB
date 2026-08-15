#include "chronos/service/single_node_database.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/common/uuid_generator.hpp"
#include "chronos/ingest/columnar_append_recovery.hpp"
#include "chronos/ingest/sealed_head_flush_queue.hpp"
#include "chronos/io/posix_io.hpp"
#include "chronos/manifest/checkpoint_builder.hpp"
#include "chronos/manifest/codec.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/sealed_head_flush_coordinator.hpp"
#include "chronos/manifest/startup_recovery.hpp"
#include "chronos/raft/durable_runtime.hpp"
#include "chronos/raft/metadata_codec.hpp"
#include "chronos/raft/metadata_runtime.hpp"
#include "chronos/raft/schema_definition_codec.hpp"
#include "chronos/wal/wal_replay_sink.hpp"
#include "chronos/wal/wal_writer.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <ranges>
#include <stdexcept>
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

[[nodiscard]] common::Status with_context(std::string context, const common::Status& status) {
  context.append(": ");
  context.append(status.message());
  return {status.code(), std::move(context)};
}

[[nodiscard]] common::Result<bool> is_new_log_directory(const std::string& path) {
  auto directory = io::PosixDirectory::open(path);
  if (!directory.has_value())
    return common::make_unexpected(directory.error());
  auto entries = directory->list_entries();
  if (!entries.has_value())
    return common::make_unexpected(entries.error());
  if (entries->empty())
    return true;
  return entries->size() == 1U && entries->front().name == "LOCK" &&
         entries->front().type == io::DirectoryEntryType::kRegularFile;
}

[[nodiscard]] common::Result<bool> has_final_manifest(const std::string& database_root) {
  auto root = io::PosixDirectory::open(database_root);
  if (!root.has_value())
    return common::make_unexpected(root.error());
  auto entries = root->list_entries();
  if (!entries.has_value())
    return common::make_unexpected(entries.error());
  const auto manifest_entry =
      std::ranges::find(*entries, manifest::kManifestDirectoryName, &io::DirectoryEntry::name);
  if (manifest_entry == entries->end())
    return false;
  if (manifest_entry->type != io::DirectoryEntryType::kDirectory)
    return common::make_unexpected(corruption("Manifest namespace is not a directory"));
  auto directory = root->open_directory(manifest::kManifestDirectoryName);
  if (!directory.has_value())
    return common::make_unexpected(directory.error());
  auto manifests = directory->list_entries();
  if (!manifests.has_value())
    return common::make_unexpected(manifests.error());
  return std::ranges::any_of(*manifests, [](const io::DirectoryEntry& entry) {
    return entry.type == io::DirectoryEntryType::kRegularFile &&
           manifest::parse_manifest_file_name(entry.name).has_value();
  });
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

struct RecoveredTable {
  std::string name;
  bool quoted{};
  schema::SchemaLineage lineage;
  raft::TablePolicyMetadata policy;
  std::vector<schema::TabletId> tablets;
};

[[nodiscard]] const raft::CatalogTableDefinition*
find_definition(const raft::MetadataCatalogSnapshot& catalog, const schema::SchemaId& schema_id) {
  const auto found = std::ranges::find_if(catalog.schema_definitions, [&](const auto& definition) {
    return definition.schema != nullptr && definition.schema->schema_id() == schema_id;
  });
  return found == catalog.schema_definitions.end() ? nullptr : &*found;
}

[[nodiscard]] common::Result<std::vector<RecoveredTable>>
build_complete_tables(const raft::MetadataCatalogSnapshot& catalog,
                      const runtime::DatabaseBootstrapDescriptor& bootstrap) {
  try {
    std::vector<RecoveredTable> tables;
    tables.reserve(catalog.active_schemas.size());
    for (const raft::ActiveSchemaMetadata& active : catalog.active_schemas) {
      const raft::CatalogTableDefinition* active_definition =
          find_definition(catalog, active.schema_id);
      if (active_definition == nullptr || active_definition->schema->table_id() != active.table_id)
        return common::make_unexpected(
            corruption("active schema is absent from catalog definitions"));
      const auto policy = std::ranges::find(catalog.table_policies, active.table_id,
                                            &raft::TablePolicyMetadata::table_id);
      std::vector<raft::TabletPlacementMetadata> placements;
      for (const auto& placement : catalog.tablet_placements) {
        if (placement.table_id == active.table_id)
          placements.push_back(placement);
      }
      // Schema, policy, and at least one placement form the current table-readiness boundary.
      // Prefixes left by interrupted table creation remain invisible and accept no WAL routing.
      if (policy == catalog.table_policies.end() || placements.empty())
        continue;
      for (const auto& placement : placements) {
        if (placement.replicas.size() != 1U ||
            placement.replicas.front() != bootstrap.local_node_id ||
            (placement.leader_hint.has_value() &&
             *placement.leader_hint != bootstrap.local_node_id)) {
          return common::make_unexpected(
              invalid("single-node database cannot own a nonlocal tablet placement"));
        }
      }

      std::vector<const raft::CatalogTableDefinition*> definitions;
      for (const auto& definition : catalog.schema_definitions) {
        if (definition.schema != nullptr && definition.schema->table_id() == active.table_id)
          definitions.push_back(&definition);
      }
      std::ranges::sort(definitions, {}, [](const auto* definition) {
        return definition->schema->version().value();
      });
      if (definitions.empty() || definitions.back()->schema->schema_id() != active.schema_id)
        return common::make_unexpected(corruption("active schema is not the lineage tail"));
      auto lineage = schema::SchemaLineage::create(*definitions.front()->schema);
      if (!lineage.has_value())
        return common::make_unexpected(corruption(lineage.error().message()));
      for (std::size_t index = 1U; index < definitions.size(); ++index) {
        const common::Status appended = lineage->append(*definitions[index]->schema);
        if (!appended.is_ok())
          return common::make_unexpected(corruption(appended.message()));
      }
      std::ranges::sort(placements, {}, &raft::TabletPlacementMetadata::tablet_id);
      std::vector<schema::TabletId> tablets;
      tablets.reserve(placements.size());
      for (const auto& placement : placements)
        tablets.push_back(placement.tablet_id);
      tables.push_back({.name = active_definition->name,
                        .quoted = active_definition->quoted,
                        .lineage = std::move(*lineage),
                        .policy = *policy,
                        .tablets = std::move(tablets)});
    }
    std::ranges::sort(tables, {},
                      [](const RecoveredTable& table) { return table.lineage.table_id(); });
    return tables;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("recovered table catalog allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("recovered table catalog exceeds container limits"));
  }
}

[[nodiscard]] ingest::TabletStateConfig
tablet_config(const RecoveredTable& table, const schema::TableSchema& schema,
              const runtime::DatabaseBootstrapDescriptor& bootstrap,
              std::shared_ptr<ingest::SealedHeadFlushQueue> flush_queue) {
  const std::uint64_t retry_limit =
      std::min(table.policy.retry_retention_positions, bootstrap.maximum_retry_entries);
  return {.head_capacity = head_capacity(schema, bootstrap),
          .maximum_schema_versions = table.lineage.size(),
          .maximum_sealed_generations = bootstrap.maximum_sealed_generations,
          .maximum_retry_entries = static_cast<std::size_t>(retry_limit),
          .flush_queue = std::move(flush_queue)};
}

class EmptyWalReplaySink final : public wal::WalReplaySink {
public:
  [[nodiscard]] common::Status preflight(const wal::WalReplayRecord&) override {
    return corruption("WAL contains an application record without a complete configured tablet");
  }
  [[nodiscard]] common::Status replay(const wal::WalReplayRecord&) override {
    return corruption("unconfigured WAL record reached replay");
  }
};

struct FreshTablet {
  schema::TabletId tablet_id;
  ingest::TabletState state;
};

struct TabletFlushOwner {
  schema::TabletId tablet_id;
  std::shared_ptr<ingest::SealedHeadFlushQueue> queue;
  manifest::SealedHeadFlushCoordinator coordinator;
};

struct PendingFlushQueue {
  schema::TabletId tablet_id;
  std::shared_ptr<ingest::SealedHeadFlushQueue> queue;
};

} // namespace

class SingleNodeDatabase::Impl {
public:
  Impl(runtime::DatabaseBootstrap configured_bootstrap,
       std::unique_ptr<raft::DurableMultiRaftRuntime> configured_raft,
       raft::DurableMetadataStateMachine configured_metadata,
       raft::MetadataCatalogSnapshot configured_catalog,
       std::vector<RecoveredTable> configured_tables,
       std::shared_ptr<const query::QueryCatalogSnapshot> configured_query_catalog,
       manifest::RecoveredManifestColumnarState configured_recovered,
       std::vector<TabletFlushOwner> configured_flush_owners,
       std::vector<FreshTablet> configured_fresh, wal::WalCommitCoordinator configured_wal,
       SingleNodeCommittedAppendObserver* configured_observer) noexcept
      : bootstrap_owner(std::move(configured_bootstrap)), raft_runtime(std::move(configured_raft)),
        metadata(std::move(configured_metadata)), catalog(std::move(configured_catalog)),
        tables(std::move(configured_tables)), query_catalog(std::move(configured_query_catalog)),
        recovered(std::move(configured_recovered)),
        flush_owners(std::move(configured_flush_owners)),
        fresh_tablets(std::move(configured_fresh)), wal_coordinator(std::move(configured_wal)),
        committed_append_observer(configured_observer) {}

  [[nodiscard]] common::Status refresh_catalog() {
    auto projected = metadata.state().catalog_snapshot();
    if (!projected.has_value())
      return projected.error();
    catalog = std::move(*projected);
    return common::Status::ok();
  }

  [[nodiscard]] common::Status propose_and_apply(const std::uint8_t type,
                                                 std::vector<std::byte> payload) {
    auto proposed = raft_runtime->execute_batch(
        {{metadata.group_id(), raft::ProposeExactRetainedOperation{type, std::move(payload)}}});
    if (!proposed.has_value())
      return with_context("propose table metadata", proposed.error());
    if (proposed->size() != 1U)
      return corruption("table metadata proposal returned an invalid result count");
    if (!proposed->front().status.is_ok())
      return with_context("propose table metadata", proposed->front().status);
    auto applied = metadata.apply_committed();
    if (!applied.has_value())
      return with_context("apply table metadata", applied.error());
    return refresh_catalog();
  }

  [[nodiscard]] manifest::RecoveredManifestColumnarState* recovered_state() noexcept {
    return recovered.has_value() ? std::addressof(*recovered) : nullptr;
  }

  runtime::DatabaseBootstrap bootstrap_owner;
  std::unique_ptr<raft::DurableMultiRaftRuntime> raft_runtime;
  raft::DurableMetadataStateMachine metadata;
  raft::MetadataCatalogSnapshot catalog;
  std::vector<RecoveredTable> tables;
  std::shared_ptr<const query::QueryCatalogSnapshot> query_catalog;
  std::optional<manifest::RecoveredManifestColumnarState> recovered;
  std::vector<TabletFlushOwner> flush_owners;
  std::vector<FreshTablet> fresh_tablets;
  wal::WalCommitCoordinator wal_coordinator;
  SingleNodeCommittedAppendObserver* committed_append_observer{};
  bool shutdown{};
};

SingleNodeDatabase::SingleNodeDatabase(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
SingleNodeDatabase::~SingleNodeDatabase() {
  shutdown_noexcept();
}
SingleNodeDatabase::SingleNodeDatabase(SingleNodeDatabase&&) noexcept = default;
SingleNodeDatabase& SingleNodeDatabase::operator=(SingleNodeDatabase&& other) noexcept {
  if (this != std::addressof(other)) {
    shutdown_noexcept();
    impl_ = std::move(other.impl_);
  }
  return *this;
}

void SingleNodeDatabase::shutdown_noexcept() noexcept {
  try {
    if (impl_ != nullptr)
      static_cast<void>(shutdown());
  } catch (...) { // NOLINT(bugprone-empty-catch)
    // Explicit shutdown reports failures; destruction and replacement are necessarily best-effort.
  }
}

common::Result<SingleNodeDatabase>
SingleNodeDatabase::open_or_create(const SingleNodeDatabaseConfig& config) {
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(config.bootstrap);
  if (!bootstrap.has_value())
    return common::make_unexpected(bootstrap.error());
  const runtime::DatabaseBootstrapDescriptor descriptor = bootstrap->descriptor();
  const raft::GroupId metadata_group = descriptor.metadata_group_id;
  const std::vector<raft::RaftGroupConfiguration> groups{
      {metadata_group, {descriptor.local_node_id}}};
  const raft::RaftPersistentLogConfig raft_config{
      .directory_path = bootstrap->raft_directory_path(),
      .target_segment_size = descriptor.raft_segment_target_bytes};
  auto new_raft = is_new_log_directory(raft_config.directory_path);
  if (!new_raft.has_value())
    return common::make_unexpected(
        with_context("classify metadata Raft directory", new_raft.error()));
  auto raft_runtime =
      *new_raft
          ? raft::DurableMultiRaftRuntime::create_new(descriptor.local_node_id, raft_config, groups)
          : raft::DurableMultiRaftRuntime::open_existing(descriptor.local_node_id, raft_config,
                                                         config.raft_recovery, groups);
  if (!raft_runtime.has_value())
    return common::make_unexpected(
        with_context("open metadata Raft runtime", raft_runtime.error()));
  auto election = raft_runtime->execute_batch({{metadata_group, raft::StartElectionOperation{}}});
  if (!election.has_value())
    return common::make_unexpected(
        with_context("elect single-node metadata leader", election.error()));
  if (election->size() != 1U)
    return common::make_unexpected(
        corruption("single-node metadata election returned an invalid result count"));
  if (!election->front().status.is_ok())
    return common::make_unexpected(
        with_context("elect single-node metadata leader", election->front().status));
  std::unique_ptr<raft::DurableMultiRaftRuntime> stable_raft;
  try {
    stable_raft = std::make_unique<raft::DurableMultiRaftRuntime>(std::move(*raft_runtime));
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("metadata Raft owner allocation failed"));
  }
  auto metadata = raft::DurableMetadataStateMachine::recover(metadata_group, *stable_raft);
  if (!metadata.has_value())
    return common::make_unexpected(with_context("recover metadata catalog", metadata.error()));
  auto catalog = metadata->state().catalog_snapshot();
  if (!catalog.has_value())
    return common::make_unexpected(catalog.error());
  auto tables = build_complete_tables(*catalog, descriptor);
  if (!tables.has_value())
    return common::make_unexpected(tables.error());

  try {
    std::vector<query::QueryCatalogTableInput> query_inputs;
    query_inputs.reserve(tables->size());
    for (const RecoveredTable& table : *tables) {
      query_inputs.push_back(
          {.name = table.name, .quoted = table.quoted, .schema = table.lineage.current()});
    }
    auto query_catalog_value = query::QueryCatalogSnapshot::create(
        std::max<std::uint64_t>(1U, catalog->applied_index), query_inputs);
    if (!query_catalog_value.has_value())
      return common::make_unexpected(query_catalog_value.error().status());
    auto query_catalog =
        std::make_shared<const query::QueryCatalogSnapshot>(std::move(*query_catalog_value));

    ingest::ColumnarAppendRecoveryConfig recovery_config;
    recovery_config.retry_directory = {
        .maximum_entries = static_cast<std::size_t>(descriptor.maximum_retry_entries)};
    std::vector<PendingFlushQueue> pending_flush_queues;
    for (const RecoveredTable& table : *tables) {
      for (const schema::TabletId tablet_id : table.tablets) {
        auto flush_queue = ingest::SealedHeadFlushQueue::create(
            {.capacity = descriptor.maximum_sealed_generations});
        if (!flush_queue.has_value())
          return common::make_unexpected(flush_queue.error());
        pending_flush_queues.push_back({.tablet_id = tablet_id, .queue = *flush_queue});
        auto initial = table.lineage.at(0U);
        ingest::ColumnarRecoveryTabletConfig tablet{
            .schema = initial,
            .tablet_id = tablet_id,
            .state = tablet_config(table, *initial, descriptor, *flush_queue),
            .successors = {},
            .durable_seed = std::nullopt};
        for (std::size_t version = 1U; version < table.lineage.size(); ++version) {
          const auto successor = table.lineage.at(version);
          tablet.successors.push_back(
              {.schema = successor, .head_capacity = head_capacity(*successor, descriptor)});
        }
        recovery_config.tablets.push_back(std::move(tablet));
      }
    }

    const wal::WalWriterConfig wal_config{.directory_path = bootstrap->wal_directory_path(),
                                          .target_segment_size =
                                              descriptor.wal_segment_target_bytes};
    auto database_id = manifest::DatabaseId::from_uuid(descriptor.database_id);
    if (!database_id.has_value())
      return common::make_unexpected(
          corruption("database bootstrap identity is invalid for Manifest storage"));

    auto established = has_final_manifest(bootstrap->database_root());
    if (!established.has_value())
      return common::make_unexpected(
          with_context("classify database Manifest namespace", established.error()));
    wal::WalId manifest_wal_id{};
    std::vector<schema::TabletId> durable_tablet_ids;
    if (*established) {
      auto storage =
          manifest::ManifestStorage::open_existing({.database_root = bootstrap->database_root()});
      if (!storage.has_value())
        return common::make_unexpected(storage.error());
      auto identity = storage->selected_identity();
      if (!identity.has_value())
        return common::make_unexpected(identity.error());
      if (identity->database_id != *database_id)
        return common::make_unexpected(
            corruption("selected Manifest database identity disagrees with Bootstrap"));
      manifest_wal_id = identity->wal_id;
      durable_tablet_ids = std::move(identity->tablet_ids);
    } else {
      auto new_wal = is_new_log_directory(wal_config.directory_path);
      if (!new_wal.has_value())
        return common::make_unexpected(with_context("classify WAL directory", new_wal.error()));
      common::Result<wal::WalWriter> writer =
          common::make_unexpected(invalid("WAL not opened for Manifest initialization"));
      if (*new_wal) {
        writer = wal::WalWriter::create_new(wal_config);
      } else if (recovery_config.tablets.empty()) {
        EmptyWalReplaySink sink;
        writer = wal::WalWriter::open_existing(wal_config, config.wal_recovery, sink);
      } else {
        auto bootstrap_recovery = recovery_config;
        auto state = ingest::recover_columnar_append_wal(wal_config, config.wal_recovery,
                                                         std::move(bootstrap_recovery));
        if (!state.has_value())
          return common::make_unexpected(
              with_context("recover pre-Manifest database WAL", state.error()));
        writer = state->release_writer();
      }
      if (!writer.has_value())
        return common::make_unexpected(
            with_context("open WAL for Manifest initialization", writer.error()));
      manifest_wal_id = writer->wal_id();
      {
        auto storage = manifest::ManifestStorage::initialize_empty(
            {.database_root = bootstrap->database_root(),
             .database_id = *database_id,
             .wal_id = manifest_wal_id});
        if (!storage.has_value())
          return common::make_unexpected(
              with_context("initialize database Manifest storage", storage.error()));
      }
      const common::Status closed = writer->close();
      if (!closed.is_ok())
        return common::make_unexpected(with_context("close initialized database WAL", closed));
    }

    std::vector<manifest::TabletSchemaBinding> schema_bindings;
    for (const RecoveredTable& table : *tables) {
      for (const schema::TabletId& tablet_id : table.tablets) {
        if (std::ranges::binary_search(durable_tablet_ids, tablet_id))
          schema_bindings.push_back({.tablet_id = tablet_id, .lineage = std::cref(table.lineage)});
      }
    }
    std::ranges::sort(schema_bindings, {}, &manifest::TabletSchemaBinding::tablet_id);

    auto recovered = manifest::recover_manifest_columnar_database(
        {.manifest_storage = {.database_root = bootstrap->database_root()},
         .manifest_load = {.expected_database_id = *database_id,
                           .expected_wal_id = manifest_wal_id,
                           .schema_bindings = schema_bindings,
                           .decode_limits = {},
                           .part_validation_limits = {}},
         .wal_writer = wal_config,
         .wal_recovery = config.wal_recovery,
         .reclaim_checkpointed_wal_segments = true,
         .columnar_recovery = std::move(recovery_config)});
    if (!recovered.has_value())
      return common::make_unexpected(
          with_context("recover Manifest-backed database", recovered.error()));
    std::vector<TabletFlushOwner> flush_owners;
    flush_owners.reserve(pending_flush_queues.size());
    for (PendingFlushQueue& pending : pending_flush_queues) {
      auto flush = manifest::SealedHeadFlushCoordinator::create(
          pending.queue, recovered->manifest_storage(), recovered->storage_publisher());
      if (!flush.has_value())
        return common::make_unexpected(flush.error());
      flush_owners.push_back({.tablet_id = pending.tablet_id,
                              .queue = std::move(pending.queue),
                              .coordinator = std::move(*flush)});
    }
    auto writer = recovered->release_writer();
    if (!writer.has_value())
      return common::make_unexpected(writer.error());
    auto coordinator = wal::WalCommitCoordinator::start(std::move(*writer), config.wal_commit);
    if (!coordinator.has_value())
      return common::make_unexpected(coordinator.error());
    return SingleNodeDatabase{std::make_unique<Impl>(
        std::move(*bootstrap), std::move(stable_raft), std::move(*metadata), std::move(*catalog),
        std::move(*tables), std::move(query_catalog), std::move(*recovered),
        std::move(flush_owners), std::vector<FreshTablet>{}, std::move(*coordinator),
        config.committed_append_observer)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("single-node database allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("single-node database exceeds container limits"));
  }
}

const runtime::DatabaseBootstrapDescriptor& SingleNodeDatabase::bootstrap() const noexcept {
  return impl_->bootstrap_owner.descriptor();
}
const raft::MetadataCatalogSnapshot& SingleNodeDatabase::metadata_catalog() const noexcept {
  return impl_->catalog;
}
const std::shared_ptr<const query::QueryCatalogSnapshot>&
SingleNodeDatabase::query_catalog() const noexcept {
  return impl_->query_catalog;
}
const schema::SchemaLineage*
SingleNodeDatabase::find_lineage(const schema::TableId& table_id) const noexcept {
  if (impl_ == nullptr)
    return nullptr;
  const auto found = std::ranges::find_if(impl_->tables, [&](const RecoveredTable& table) {
    return table.lineage.table_id() == table_id;
  });
  return found == impl_->tables.end() ? nullptr : &found->lineage;
}
ingest::TabletState* SingleNodeDatabase::find_tablet(const schema::TabletId& tablet_id) noexcept {
  if (impl_ == nullptr)
    return nullptr;
  if (auto* recovered = impl_->recovered_state(); recovered != nullptr) {
    if (auto* tablet = recovered->tablet(tablet_id); tablet != nullptr)
      return tablet;
  }
  const auto found = std::ranges::find(impl_->fresh_tablets, tablet_id, &FreshTablet::tablet_id);
  return found == impl_->fresh_tablets.end() ? nullptr : &found->state;
}
const ingest::TabletState*
SingleNodeDatabase::find_tablet(const schema::TabletId& tablet_id) const noexcept {
  if (impl_ == nullptr)
    return nullptr;
  if (auto* recovered = impl_->recovered_state(); recovered != nullptr) {
    if (const auto* tablet = recovered->tablet(tablet_id); tablet != nullptr)
      return tablet;
  }
  const auto found = std::ranges::find(impl_->fresh_tablets, tablet_id, &FreshTablet::tablet_id);
  return found == impl_->fresh_tablets.end() ? nullptr : &found->state;
}
common::Result<std::vector<ingest::TabletSnapshot>>
SingleNodeDatabase::table_snapshots(const schema::TableId& table_id) const {
  if (impl_ == nullptr || impl_->shutdown)
    return common::make_unexpected(invalid("database tablet state is unavailable"));
  const auto table = std::ranges::find_if(impl_->tables, [&](const RecoveredTable& candidate) {
    return candidate.lineage.table_id() == table_id;
  });
  if (table == impl_->tables.end())
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "table has no local runtime state"});
  try {
    std::vector<ingest::TabletSnapshot> snapshots;
    snapshots.reserve(table->tablets.size());
    for (const schema::TabletId& tablet_id : table->tablets) {
      const ingest::TabletState* const tablet = find_tablet(tablet_id);
      if (tablet == nullptr)
        return common::make_unexpected(corruption("local table placement has no tablet state"));
      auto snapshot = tablet->snapshot();
      if (!snapshot.has_value())
        return common::make_unexpected(snapshot.error());
      snapshots.push_back(std::move(*snapshot));
    }
    if (snapshots.empty())
      return common::make_unexpected(corruption("routable table has no tablet state"));
    return snapshots;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("tablet snapshot allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("tablet snapshot count exceeds container limits"));
  }
}
common::Result<ingest::ColumnarAppendExecutionResult>
SingleNodeDatabase::execute_append(const schema::TabletId tablet_id,
                                   const ingest::ColumnarAppendExecutionInput& input) {
  auto* recovered = impl_ == nullptr ? nullptr : impl_->recovered_state();
  if (impl_ == nullptr || impl_->shutdown || recovered == nullptr)
    return common::make_unexpected(invalid("single-node append storage is unavailable"));
  if (input.batch == nullptr)
    return common::make_unexpected(invalid("single-node append requires an owning batch"));
  ingest::TabletState* const tablet = find_tablet(tablet_id);
  if (tablet == nullptr)
    return common::make_unexpected(invalid("single-node append tablet is not local"));
  auto before = tablet->snapshot();
  if (!before.has_value())
    return common::make_unexpected(before.error());
  if (before->table_id() != input.batch->schema().table_id() || before->tablet_id() != tablet_id)
    return common::make_unexpected(invalid("single-node append batch is routed to another tablet"));

  std::shared_ptr<const columnar::OwnedColumnarBatch> retained_batch = input.batch;
  auto executed = ingest::execute_columnar_append(input, recovered->retry_directory(), *tablet,
                                                  impl_->wal_coordinator);
  if (!executed.has_value())
    return common::make_unexpected(executed.error());
  const auto& wal_commit = executed->wal_commit;
  if (executed->kind == ingest::ColumnarAppendExecutionKind::kApplied &&
      impl_->committed_append_observer != nullptr && wal_commit.has_value() &&
      executed->outcome != nullptr) {
    const auto& commit = *wal_commit;
    impl_->committed_append_observer->on_applied(
        {.tablet_id = tablet_id,
         .position = {.wal_id = commit.append.record_start.wal_id,
                      .record_sequence = commit.append.record_sequence},
         .batch = std::move(retained_batch),
         .outcome = executed->outcome});
  }
  return executed;
}
common::Result<manifest::DatabaseStorageSnapshot> SingleNodeDatabase::storage_snapshot() const {
  const auto* recovered = impl_ == nullptr ? nullptr : impl_->recovered_state();
  if (recovered == nullptr)
    return common::make_unexpected(invalid("database storage publication is unavailable"));
  return recovered->snapshot();
}

common::Result<SingleNodeSubscriptionSnapshotContext>
SingleNodeDatabase::subscription_snapshot_context(const schema::TableId& table_id) const {
  auto* recovered = impl_ == nullptr ? nullptr : impl_->recovered_state();
  if (impl_ == nullptr || impl_->shutdown || recovered == nullptr)
    return common::make_unexpected(invalid("database subscription storage is unavailable"));
  const auto table = std::ranges::find_if(impl_->tables, [&](const RecoveredTable& candidate) {
    return candidate.lineage.table_id() == table_id;
  });
  if (table == impl_->tables.end())
    return common::make_unexpected(common::Status{common::StatusCode::kNotFound,
                                                  "subscription table has no local runtime state"});
  return SingleNodeSubscriptionSnapshotContext{.storage =
                                                   &std::as_const(*recovered).manifest_storage(),
                                               .publisher = &recovered->storage_publisher(),
                                               .lineage = &table->lineage};
}

common::Result<std::unique_ptr<query::PhysicalOperator>>
SingleNodeDatabase::instantiate_table_pipeline(
    const query::QueryResourceContext& resources, const schema::TableId& table_id,
    const schema::SchemaId& destination_schema_id, const query::PhysicalPipelinePlan& pipeline,
    const query::SnapshotTabletPipelineLimits limits) const {
  const auto* recovered = impl_ == nullptr ? nullptr : impl_->recovered_state();
  if (impl_ == nullptr || impl_->shutdown || recovered == nullptr)
    return common::make_unexpected(invalid("database query storage is unavailable"));
  const auto table = std::ranges::find_if(impl_->tables, [&](const RecoveredTable& candidate) {
    return candidate.lineage.table_id() == table_id;
  });
  if (table == impl_->tables.end())
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "query table has no local runtime state"});
  auto snapshot = recovered->snapshot();
  if (!snapshot.has_value())
    return common::make_unexpected(snapshot.error());
  return query::instantiate_snapshot_tablets_pipeline(
      resources, std::as_const(*recovered).manifest_storage(), *snapshot, table->tablets,
      table->lineage, destination_schema_id, pipeline, limits);
}

common::Result<std::unique_ptr<query::PhysicalOperator>>
SingleNodeDatabase::instantiate_asof_pipeline(
    const query::QueryResourceContext& resources,
    const std::span<const SingleNodeAsofSourceBinding> sources,
    const query::PhysicalAsofPlan& plan) const {
  const auto* recovered = impl_ == nullptr ? nullptr : impl_->recovered_state();
  if (impl_ == nullptr || impl_->shutdown || recovered == nullptr)
    return common::make_unexpected(invalid("database query storage is unavailable"));
  try {
    std::vector<query::SnapshotTableSourceBinding> bindings;
    bindings.reserve(sources.size());
    for (const SingleNodeAsofSourceBinding& source : sources) {
      const auto table = std::ranges::find_if(impl_->tables, [&](const RecoveredTable& candidate) {
        return candidate.lineage.table_id() == source.table_id;
      });
      if (table == impl_->tables.end()) {
        return common::make_unexpected(common::Status{
            common::StatusCode::kNotFound, "ASOF query table has no local runtime state"});
      }
      bindings.push_back({.target_tablets = table->tablets,
                          .lineage = std::cref(table->lineage),
                          .destination_schema_id = source.destination_schema_id,
                          .limits = source.limits});
    }
    auto snapshot = recovered->snapshot();
    if (!snapshot.has_value())
      return common::make_unexpected(snapshot.error());
    return query::instantiate_snapshot_tables_asof_plan(
        resources, std::as_const(*recovered).manifest_storage(), *snapshot, bindings, plan);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("single-node ASOF query allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("single-node ASOF query exceeds container limits"));
  }
}

common::Result<std::size_t> SingleNodeDatabase::flush_ready_heads() {
  auto* recovered = impl_ == nullptr ? nullptr : impl_->recovered_state();
  if (impl_ == nullptr || impl_->shutdown || recovered == nullptr)
    return common::make_unexpected(invalid("database is not accepting storage maintenance"));
  try {
    common::SystemUuidGenerator identities;
    std::size_t completed = 0U;
    for (TabletFlushOwner& owner : impl_->flush_owners) {
      ingest::TabletState* const current_tablet = find_tablet(owner.tablet_id);
      if (current_tablet == nullptr)
        return common::make_unexpected(corruption("flush queue has no tablet owner"));
      auto current_snapshot = current_tablet->snapshot();
      if (!current_snapshot.has_value())
        return common::make_unexpected(current_snapshot.error());
      auto refreshed = recovered->storage_publisher().publish_tablet_snapshot(*current_snapshot);
      if (!refreshed.has_value())
        return common::make_unexpected(refreshed.error());
      while (owner.queue->metrics().ready != 0U) {
        ingest::TabletState* const tablet = find_tablet(owner.tablet_id);
        if (tablet == nullptr)
          return common::make_unexpected(corruption("flush queue has no tablet owner"));
        auto snapshot = tablet->snapshot();
        if (!snapshot.has_value())
          return common::make_unexpected(snapshot.error());
        if (snapshot->sealed_generations().empty())
          return common::make_unexpected(
              corruption("ready flush queue has no visible sealed generation"));
        const head::HeadSnapshot& sealed = snapshot->sealed_generations().front();

        std::vector<std::uint64_t> record_sequences;
        for (std::uint32_t row = 0U; row < sealed.row_count(); ++row) {
          auto metadata = sealed.row_metadata(row);
          if (!metadata.has_value())
            return common::make_unexpected(metadata.error());
          if (metadata->commit_position.source != head::CommitSource::kWal)
            return common::make_unexpected(
                invalid("Manifest v1 flush requires WAL-backed sealed rows"));
          const std::uint64_t sequence = metadata->commit_position.record_sequence;
          if (std::ranges::find(record_sequences, sequence) == record_sequences.end())
            record_sequences.push_back(sequence);
        }
        std::ranges::sort(record_sequences);
        auto retry_entries = snapshot->retry_entries();
        if (!retry_entries.has_value())
          return common::make_unexpected(retry_entries.error());
        std::vector<manifest::RetryDescriptor> retries;
        retries.reserve(record_sequences.size());
        for (const std::uint64_t sequence : record_sequences) {
          const auto entry = std::ranges::find_if(*retry_entries, [&](const auto& candidate) {
            return candidate.outcome != nullptr &&
                   candidate.outcome->commit_source == head::CommitSource::kWal &&
                   candidate.outcome->record_sequence == sequence;
          });
          if (entry == retry_entries->end())
            return common::make_unexpected(corruption("sealed WAL record has no retry outcome"));
          retries.push_back({.client_id = entry->identity.client_id,
                             .client_batch_id = entry->identity.client_batch_id,
                             .table_id = entry->outcome->mutation.table_id,
                             .tablet_id = entry->outcome->mutation.tablet_id,
                             .request_digest = entry->outcome->mutation.request_digest,
                             .wal_id = entry->outcome->wal_id,
                             .record_sequence = entry->outcome->record_sequence,
                             .applied_row_count = entry->outcome->applied_row_count});
        }

        auto storage = recovered->snapshot();
        if (!storage.has_value())
          return common::make_unexpected(storage.error());
        std::vector<schema::TabletId> binding_tablets;
        binding_tablets.reserve(storage->durable_tablets().size() + 1U);
        for (const manifest::TabletDescriptor& durable : storage->durable_tablets())
          binding_tablets.push_back(durable.tablet_id);
        if (std::ranges::find(binding_tablets, owner.tablet_id) == binding_tablets.end())
          binding_tablets.push_back(owner.tablet_id);
        std::ranges::sort(binding_tablets);
        std::vector<manifest::TabletSchemaBinding> schema_bindings;
        schema_bindings.reserve(binding_tablets.size());
        for (const schema::TabletId& tablet_id : binding_tablets) {
          const auto table =
              std::ranges::find_if(impl_->tables, [&](const RecoveredTable& candidate) {
                return std::ranges::find(candidate.tablets, tablet_id) != candidate.tablets.end();
              });
          if (table == impl_->tables.end())
            return common::make_unexpected(
                corruption("durable Manifest tablet has no retained schema lineage"));
          schema_bindings.push_back({.tablet_id = tablet_id, .lineage = std::cref(table->lineage)});
        }

        std::array<common::Uuid, 3U> generated;
        for (std::size_t index = 0U; index < generated.size(); ++index) {
          bool unique = false;
          for (std::size_t attempt = 0U; attempt < 8U && !unique; ++attempt) {
            auto identity = identities.generate();
            if (!identity.has_value())
              return common::make_unexpected(identity.error());
            unique = std::ranges::find(generated.begin(), generated.begin() + index, *identity) ==
                     generated.begin() + index;
            if (unique)
              generated[index] = *identity;
          }
          if (!unique)
            return common::make_unexpected(
                common::Status{common::StatusCode::kUnavailable,
                               "storage identity source repeatedly returned duplicates"});
        }
        auto part_id = cseg::PartId::from_uuid(generated[0U]);
        if (!part_id.has_value())
          return common::make_unexpected(part_id.error());
        auto flushed =
            owner.coordinator.try_flush_one(*tablet, {.part_id = *part_id,
                                                      .part_nonce = generated[1U],
                                                      .manifest_nonce = generated[2U],
                                                      .compression = cseg::PageCompression::kNone,
                                                      .new_retries = retries,
                                                      .schema_bindings = schema_bindings,
                                                      .manifest_decode_limits = {},
                                                      .part_validation_limits = {}});
        if (!flushed.has_value())
          return common::make_unexpected(flushed.error());
        if (!flushed->has_value())
          break;
        ++completed;
      }
    }
    return completed;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("sealed-head flush allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("sealed-head flush exceeds container limits"));
  }
}

common::Status SingleNodeDatabase::checkpoint_flushed_wal() {
  auto* recovered = impl_ == nullptr ? nullptr : impl_->recovered_state();
  if (recovered == nullptr)
    return invalid("database storage is unavailable for WAL checkpointing");
  try {
    auto snapshot = recovered->snapshot();
    if (!snapshot.has_value())
      return snapshot.error();
    if (snapshot->parts().empty())
      return common::Status::ok();
    const std::optional<std::uint64_t> generation =
        common::checked_add(snapshot->generation(), std::uint64_t{1U});
    if (!generation.has_value())
      return exhausted("Manifest generation overflowed during WAL checkpointing");

    auto predecessor = manifest::decode_manifest_v1_exact(snapshot->manifest_bytes());
    if (!predecessor.has_value())
      return corruption("published Manifest failed exact decode during WAL checkpointing");
    auto candidate =
        manifest::encode_manifest_v1({.generation = *generation,
                                      .database_id = snapshot->database_id(),
                                      .wal_id = snapshot->wal_id(),
                                      .reclaim_checkpoint = snapshot->reclaim_checkpoint(),
                                      .tablets = snapshot->durable_tablets(),
                                      .parts = snapshot->parts(),
                                      .retries = snapshot->retries()});
    if (!candidate.has_value())
      return candidate.error();
    auto decoded_candidate = manifest::decode_manifest_v1_exact(candidate->bytes());
    if (!decoded_candidate.has_value())
      return corruption("checkpoint candidate failed exact decode");

    std::vector<manifest::TabletSchemaBinding> schema_bindings;
    schema_bindings.reserve(snapshot->durable_tablets().size());
    for (const manifest::TabletDescriptor& durable : snapshot->durable_tablets()) {
      const auto table =
          std::ranges::find_if(impl_->tables, [&](const RecoveredTable& candidate_table) {
            return candidate_table.lineage.table_id() == durable.table_id &&
                   std::ranges::find(candidate_table.tablets, durable.tablet_id) !=
                       candidate_table.tablets.end();
          });
      if (table == impl_->tables.end())
        return corruption("durable Manifest tablet has no checkpoint schema lineage");
      schema_bindings.push_back(
          {.tablet_id = durable.tablet_id, .lineage = std::cref(table->lineage)});
    }

    std::vector<manifest::SnapshotPartImage> owned_images;
    std::vector<std::string> image_names;
    owned_images.reserve(snapshot->parts().size());
    image_names.reserve(snapshot->parts().size());
    for (const manifest::PartDescriptor& part : snapshot->parts()) {
      const std::array ids{part.part_id};
      auto loaded = recovered->manifest_storage().load_snapshot_part_images(*snapshot, ids,
                                                                            schema_bindings, {});
      if (!loaded.has_value())
        return loaded.error();
      if (loaded->size() != 1U)
        return corruption("checkpoint part-image load returned an invalid count");
      owned_images.push_back(std::move(loaded->front()));
      image_names.push_back(manifest::part_file_name(part.part_id));
    }
    std::vector<manifest::ReferencedPartImage> referenced_images;
    referenced_images.reserve(owned_images.size());
    for (std::size_t index = 0U; index < owned_images.size(); ++index)
      referenced_images.push_back(
          {.file_name = image_names[index], .bytes = owned_images[index].bytes()});

    auto checkpointed = manifest::build_manifest_v1_checkpointed_generation(
        {.wal_directory = impl_->bootstrap_owner.wal_directory_path(),
         .predecessor = std::cref(*predecessor),
         .candidate = std::cref(*decoded_candidate),
         .schema_bindings = schema_bindings,
         .referenced_parts = referenced_images,
         .command_decode_limits = {},
         .part_validation_limits = {}});
    if (!checkpointed.has_value())
      return checkpointed.error();
    if (checkpointed->reclaim_checkpoint == snapshot->reclaim_checkpoint())
      return common::Status::ok();
    common::SystemUuidGenerator identities;
    auto nonce = identities.generate();
    if (!nonce.has_value())
      return nonce.error();
    auto installed = recovered->manifest_storage().install_manifest(
        {.encoded_manifest = std::cref(checkpointed->encoded_manifest),
         .schema_bindings = schema_bindings,
         .nonce = *nonce,
         .decode_limits = {},
         .part_validation_limits = {},
         .compaction_replacement = nullptr,
         .compaction_equivalence_limits = {}});
    if (!installed.has_value())
      return installed.error();
    auto selected = recovered->manifest_storage().load_selected_manifest(
        {.expected_database_id = snapshot->database_id(),
         .expected_wal_id = snapshot->wal_id(),
         .schema_bindings = schema_bindings,
         .decode_limits = {},
         .part_validation_limits = {}});
    if (!selected.has_value())
      return selected.error();
    auto selected_owner =
        std::make_shared<const manifest::LoadedManifestGeneration>(std::move(*selected));
    auto published = recovered->storage_publisher().publish_manifest(
        {.selected_manifest = std::move(selected_owner), .replacements = {}});
    return published.has_value() ? common::Status::ok() : published.error();
  } catch (const std::bad_alloc&) {
    return exhausted("WAL checkpoint allocation failed");
  } catch (const std::length_error&) {
    return exhausted("WAL checkpoint exceeds container limits");
  }
}
common::Status SingleNodeDatabase::shutdown() {
  if (impl_ == nullptr || impl_->shutdown)
    return common::Status::ok();
  common::Status result = common::Status::ok();
  auto flushed = flush_ready_heads();
  if (!flushed.has_value())
    result = with_context("drain sealed heads during shutdown", flushed.error());
  impl_->shutdown = true;
  const common::Status wal = impl_->wal_coordinator.shutdown();
  if (result.is_ok())
    result = wal;
  if (wal.is_ok()) {
    const common::Status checkpoint = checkpoint_flushed_wal();
    if (result.is_ok())
      result = checkpoint;
  }
  impl_->flush_owners.clear();
  impl_->recovered.reset();
  const common::Status raft = impl_->raft_runtime->close();
  if (result.is_ok())
    result = raft;
  const common::Status bootstrap = impl_->bootstrap_owner.close();
  if (result.is_ok())
    result = bootstrap;
  return result;
}

common::Result<CreatedSingleNodeTable>
SingleNodeDatabase::create_table(const query::BoundSqlCreateTable& statement,
                                 NewTableIdentities identities,
                                 const std::uint64_t retry_retention_positions) {
  auto* recovered = impl_ == nullptr ? nullptr : impl_->recovered_state();
  if (impl_ == nullptr || impl_->shutdown || recovered == nullptr)
    return common::make_unexpected(invalid("database is not accepting table creation"));
  if (statement.catalog().get() != impl_->query_catalog.get())
    return common::make_unexpected(invalid("CREATE TABLE was bound against a stale catalog"));
  if (retry_retention_positions == 0U ||
      identities.column_ids.size() != statement.syntax().columns().size())
    return common::make_unexpected(
        invalid("CREATE TABLE identities or retry retention are invalid"));

  try {
    const query::SqlIdentifier& name = statement.syntax().table();
    const raft::CatalogTableDefinition* existing =
        impl_->metadata.state().find_active_table_definition(name.text(), name.quoted());
    const bool resumed = existing != nullptr;
    schema::TableId table_id = resumed ? existing->schema->table_id() : identities.table_id;
    schema::SchemaId schema_id = resumed ? existing->schema->schema_id() : identities.schema_id;
    std::vector<schema::ColumnId> column_ids;
    if (resumed) {
      column_ids.reserve(existing->schema->columns().size());
      for (const schema::ColumnDefinition& column : existing->schema->columns())
        column_ids.push_back(column.id());
    } else {
      column_ids = std::move(identities.column_ids);
    }
    auto materialized =
        query::materialize_sql_v1_table_schema(statement, table_id, schema_id, column_ids);
    if (!materialized.has_value())
      return common::make_unexpected(materialized.error().status());
    auto schema_ptr = std::make_shared<const schema::TableSchema>(std::move(*materialized));
    raft::CatalogTableDefinition definition{
        .name = name.text(), .quoted = name.quoted(), .schema = schema_ptr};
    if (resumed && !(*existing == definition)) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kAlreadyExists,
                         "incomplete table creation has the same name but a different definition"});
    }
    if (!resumed) {
      auto encoded = raft::encode_schema_definition_v1(definition);
      if (!encoded.has_value())
        return common::make_unexpected(encoded.error());
      const common::Status installed =
          impl_->propose_and_apply(raft::kRaftSchemaDefinitionEntryType, std::move(*encoded));
      if (!installed.is_ok())
        return common::make_unexpected(installed);
    }

    const query::BoundSqlTablePolicy& bound_policy = statement.policy();
    const raft::TablePolicyMetadata policy{
        .table_id = table_id,
        .partition_interval_ns = bound_policy.partition_interval_ns,
        .retention_ns = bound_policy.retention_ns,
        .system_history_ns = bound_policy.system_history_retention_ns,
        .allowed_lateness_ns = bound_policy.allowed_lateness_ns,
        .retry_retention_positions = retry_retention_positions};
    if (const auto* current = impl_->metadata.state().find_table_policy(table_id);
        current != nullptr && *current != policy) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kAlreadyExists,
                         "incomplete table creation has a different complete policy"});
    }
    auto encoded_policy = raft::encode_metadata_command_v1(policy);
    if (!encoded_policy.has_value())
      return common::make_unexpected(encoded_policy.error());
    common::Status installed =
        impl_->propose_and_apply(raft::kRaftMetadataCommandEntryType, std::move(*encoded_policy));
    if (!installed.is_ok())
      return common::make_unexpected(installed);

    schema::TabletId tablet_id = identities.tablet_id;
    std::vector<const raft::TabletPlacementMetadata*> existing_placements;
    for (const auto& placement : impl_->catalog.tablet_placements) {
      if (placement.table_id == table_id)
        existing_placements.push_back(&placement);
    }
    if (existing_placements.size() > 1U)
      return common::make_unexpected(invalid("initial table creation has multiple placements"));
    if (!existing_placements.empty())
      tablet_id = existing_placements.front()->tablet_id;
    const raft::TabletPlacementMetadata placement{
        .table_id = table_id,
        .tablet_id = tablet_id,
        .placement_epoch = 1U,
        .replicas = {impl_->bootstrap_owner.descriptor().local_node_id},
        .leader_hint = impl_->bootstrap_owner.descriptor().local_node_id};
    if (!existing_placements.empty() && *existing_placements.front() != placement) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kAlreadyExists,
                         "incomplete table creation has a different initial placement"});
    }
    auto encoded_placement = raft::encode_metadata_command_v1(placement);
    if (!encoded_placement.has_value())
      return common::make_unexpected(encoded_placement.error());
    installed = impl_->propose_and_apply(raft::kRaftMetadataCommandEntryType,
                                         std::move(*encoded_placement));
    if (!installed.is_ok())
      return common::make_unexpected(installed);

    auto rebuilt = build_complete_tables(impl_->catalog, impl_->bootstrap_owner.descriptor());
    if (!rebuilt.has_value())
      return common::make_unexpected(rebuilt.error());
    std::vector<query::QueryCatalogTableInput> inputs;
    inputs.reserve(rebuilt->size());
    for (const RecoveredTable& table : *rebuilt)
      inputs.push_back({table.name, table.quoted, table.lineage.current()});
    auto query_catalog = query::QueryCatalogSnapshot::create(impl_->catalog.applied_index, inputs);
    if (!query_catalog.has_value())
      return common::make_unexpected(query_catalog.error().status());

    if (find_tablet(tablet_id) == nullptr) {
      const auto table = std::ranges::find_if(*rebuilt, [&](const RecoveredTable& candidate) {
        return candidate.lineage.table_id() == table_id;
      });
      if (table == rebuilt->end())
        return common::make_unexpected(corruption("created table is absent after metadata apply"));
      auto flush_queue = ingest::SealedHeadFlushQueue::create(
          {.capacity = impl_->bootstrap_owner.descriptor().maximum_sealed_generations});
      if (!flush_queue.has_value())
        return common::make_unexpected(flush_queue.error());
      auto state = ingest::TabletState::create(table->lineage.at(0U), tablet_id,
                                               tablet_config(*table, *table->lineage.at(0U),
                                                             impl_->bootstrap_owner.descriptor(),
                                                             *flush_queue));
      if (!state.has_value())
        return common::make_unexpected(state.error());
      for (std::size_t version = 1U; version < table->lineage.size(); ++version) {
        const auto successor = table->lineage.at(version);
        const common::Status registered = state->register_schema(
            successor, head_capacity(*successor, impl_->bootstrap_owner.descriptor()));
        if (!registered.is_ok())
          return common::make_unexpected(registered);
      }
      auto tablet_snapshot = state->snapshot();
      if (!tablet_snapshot.has_value())
        return common::make_unexpected(tablet_snapshot.error());
      auto flush = manifest::SealedHeadFlushCoordinator::create(
          *flush_queue, recovered->manifest_storage(), recovered->storage_publisher());
      if (!flush.has_value())
        return common::make_unexpected(flush.error());
      impl_->fresh_tablets.reserve(impl_->fresh_tablets.size() + 1U);
      impl_->flush_owners.reserve(impl_->flush_owners.size() + 1U);
      auto published = recovered->storage_publisher().publish_tablet_snapshot(*tablet_snapshot);
      if (!published.has_value())
        return common::make_unexpected(published.error());
      impl_->fresh_tablets.push_back({tablet_id, std::move(*state)});
      impl_->flush_owners.push_back({.tablet_id = tablet_id,
                                     .queue = std::move(*flush_queue),
                                     .coordinator = std::move(*flush)});
    }
    impl_->tables = std::move(*rebuilt);
    impl_->query_catalog =
        std::make_shared<const query::QueryCatalogSnapshot>(std::move(*query_catalog));
    return CreatedSingleNodeTable{.table_id = table_id,
                                  .schema_id = schema_id,
                                  .tablet_id = tablet_id,
                                  .metadata_index = impl_->catalog.applied_index,
                                  .resumed_incomplete_creation = resumed};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("CREATE TABLE allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("CREATE TABLE exceeds container limits"));
  }
}

} // namespace chronos::service
