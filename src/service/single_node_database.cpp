#include "chronos/service/single_node_database.hpp"

#include "chronos/ingest/columnar_append_recovery.hpp"
#include "chronos/io/posix_io.hpp"
#include "chronos/raft/durable_runtime.hpp"
#include "chronos/raft/metadata_runtime.hpp"
#include "chronos/wal/wal_replay_sink.hpp"
#include "chronos/wal/wal_writer.hpp"

#include <algorithm>
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
              const runtime::DatabaseBootstrapDescriptor& bootstrap) {
  const std::uint64_t retry_limit =
      std::min(table.policy.retry_retention_positions, bootstrap.maximum_retry_entries);
  return {.head_capacity = head_capacity(schema, bootstrap),
          .maximum_schema_versions = table.lineage.size(),
          .maximum_sealed_generations = bootstrap.maximum_sealed_generations,
          .maximum_retry_entries = static_cast<std::size_t>(retry_limit),
          .flush_queue = nullptr};
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

} // namespace

class SingleNodeDatabase::Impl {
public:
  Impl(runtime::DatabaseBootstrap configured_bootstrap,
       raft::DurableMultiRaftRuntime configured_raft,
       raft::DurableMetadataStateMachine configured_metadata,
       raft::MetadataCatalogSnapshot configured_catalog,
       std::vector<RecoveredTable> configured_tables,
       std::shared_ptr<const query::QueryCatalogSnapshot> configured_query_catalog,
       std::optional<ingest::RecoveredColumnarAppendState> configured_recovered,
       std::optional<ingest::RetryDirectory> configured_retry,
       std::vector<FreshTablet> configured_fresh, wal::WalCommitCoordinator configured_wal) noexcept
      : bootstrap_owner(std::move(configured_bootstrap)), raft_runtime(std::move(configured_raft)),
        metadata(std::move(configured_metadata)), catalog(std::move(configured_catalog)),
        tables(std::move(configured_tables)), query_catalog(std::move(configured_query_catalog)),
        recovered(std::move(configured_recovered)), retry(std::move(configured_retry)),
        fresh_tablets(std::move(configured_fresh)), wal_coordinator(std::move(configured_wal)) {}

  runtime::DatabaseBootstrap bootstrap_owner;
  raft::DurableMultiRaftRuntime raft_runtime;
  raft::DurableMetadataStateMachine metadata;
  raft::MetadataCatalogSnapshot catalog;
  std::vector<RecoveredTable> tables;
  std::shared_ptr<const query::QueryCatalogSnapshot> query_catalog;
  std::optional<ingest::RecoveredColumnarAppendState> recovered;
  std::optional<ingest::RetryDirectory> retry;
  std::vector<FreshTablet> fresh_tablets;
  wal::WalCommitCoordinator wal_coordinator;
  bool shutdown{};
};

SingleNodeDatabase::SingleNodeDatabase(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
SingleNodeDatabase::~SingleNodeDatabase() {
  if (impl_ != nullptr)
    static_cast<void>(shutdown());
}
SingleNodeDatabase::SingleNodeDatabase(SingleNodeDatabase&&) noexcept = default;
SingleNodeDatabase& SingleNodeDatabase::operator=(SingleNodeDatabase&&) noexcept = default;

common::Result<SingleNodeDatabase>
SingleNodeDatabase::open_or_create(SingleNodeDatabaseConfig config) {
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
  auto metadata = raft::DurableMetadataStateMachine::recover(metadata_group, *raft_runtime);
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
    for (const RecoveredTable& table : *tables) {
      for (const schema::TabletId tablet_id : table.tablets) {
        auto initial = table.lineage.at(0U);
        ingest::ColumnarRecoveryTabletConfig tablet{.schema = initial,
                                                    .tablet_id = tablet_id,
                                                    .state =
                                                        tablet_config(table, *initial, descriptor),
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
    auto new_wal = is_new_log_directory(wal_config.directory_path);
    if (!new_wal.has_value())
      return common::make_unexpected(with_context("classify WAL directory", new_wal.error()));
    std::optional<ingest::RecoveredColumnarAppendState> recovered;
    std::optional<ingest::RetryDirectory> retry;
    std::vector<FreshTablet> fresh;
    common::Result<wal::WalWriter> writer = common::make_unexpected(invalid("WAL not opened"));
    if (*new_wal) {
      writer = wal::WalWriter::create_new(wal_config);
      if (!writer.has_value())
        return common::make_unexpected(with_context("create database WAL", writer.error()));
      auto directory = ingest::RetryDirectory::create(recovery_config.retry_directory);
      if (!directory.has_value())
        return common::make_unexpected(directory.error());
      retry.emplace(std::move(*directory));
      fresh.reserve(recovery_config.tablets.size());
      for (const auto& configured : recovery_config.tablets) {
        auto state =
            ingest::TabletState::create(configured.schema, configured.tablet_id, configured.state);
        if (!state.has_value())
          return common::make_unexpected(state.error());
        for (const auto& successor : configured.successors) {
          const common::Status registered =
              state->register_schema(successor.schema, successor.head_capacity);
          if (!registered.is_ok())
            return common::make_unexpected(registered);
        }
        fresh.push_back({configured.tablet_id, std::move(*state)});
      }
    } else if (recovery_config.tablets.empty()) {
      EmptyWalReplaySink sink;
      writer = wal::WalWriter::open_existing(wal_config, config.wal_recovery, sink);
      if (!writer.has_value())
        return common::make_unexpected(with_context("open empty database WAL", writer.error()));
      auto directory = ingest::RetryDirectory::create(recovery_config.retry_directory);
      if (!directory.has_value())
        return common::make_unexpected(directory.error());
      retry.emplace(std::move(*directory));
    } else {
      auto state = ingest::recover_columnar_append_wal(wal_config, config.wal_recovery,
                                                       std::move(recovery_config));
      if (!state.has_value())
        return common::make_unexpected(with_context("recover database WAL", state.error()));
      auto released = state->release_writer();
      if (!released.has_value())
        return common::make_unexpected(released.error());
      writer = std::move(*released);
      recovered.emplace(std::move(*state));
    }
    auto coordinator = wal::WalCommitCoordinator::start(std::move(*writer), config.wal_commit);
    if (!coordinator.has_value())
      return common::make_unexpected(coordinator.error());
    return SingleNodeDatabase{std::make_unique<Impl>(
        std::move(*bootstrap), std::move(*raft_runtime), std::move(*metadata), std::move(*catalog),
        std::move(*tables), std::move(query_catalog), std::move(recovered), std::move(retry),
        std::move(fresh), std::move(*coordinator))};
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
  const auto found = std::ranges::find_if(impl_->tables, [&](const RecoveredTable& table) {
    return table.lineage.table_id() == table_id;
  });
  return found == impl_->tables.end() ? nullptr : &found->lineage;
}
ingest::TabletState* SingleNodeDatabase::find_tablet(const schema::TabletId& tablet_id) noexcept {
  if (impl_->recovered.has_value()) {
    if (auto* tablet = impl_->recovered->tablet(tablet_id); tablet != nullptr)
      return tablet;
  }
  const auto found = std::ranges::find(impl_->fresh_tablets, tablet_id, &FreshTablet::tablet_id);
  return found == impl_->fresh_tablets.end() ? nullptr : &found->state;
}
const ingest::TabletState*
SingleNodeDatabase::find_tablet(const schema::TabletId& tablet_id) const noexcept {
  if (impl_->recovered.has_value()) {
    if (const auto* tablet = std::as_const(*impl_->recovered).tablet(tablet_id); tablet != nullptr)
      return tablet;
  }
  const auto found = std::ranges::find(impl_->fresh_tablets, tablet_id, &FreshTablet::tablet_id);
  return found == impl_->fresh_tablets.end() ? nullptr : &found->state;
}
ingest::RetryDirectory& SingleNodeDatabase::retry_directory() noexcept {
  return impl_->recovered.has_value() ? impl_->recovered->retry_directory() : *impl_->retry;
}
wal::WalCommitCoordinator& SingleNodeDatabase::wal_coordinator() noexcept {
  return impl_->wal_coordinator;
}
common::Status SingleNodeDatabase::shutdown() {
  if (impl_ == nullptr || impl_->shutdown)
    return common::Status::ok();
  impl_->shutdown = true;
  common::Status result = impl_->wal_coordinator.shutdown();
  const common::Status raft = impl_->raft_runtime.close();
  if (result.is_ok())
    result = raft;
  const common::Status bootstrap = impl_->bootstrap_owner.close();
  if (result.is_ok())
    result = bootstrap;
  return result;
}

} // namespace chronos::service
