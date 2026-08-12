#include "chronos/manifest/startup_recovery.hpp"

#include <algorithm>
#include <exception>
#include <functional>
#include <new>
#include <ranges>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace chronos::manifest {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status corruption(std::string message) {
  return common::Status{common::StatusCode::kCorruption, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] common::Status internal(std::string message) {
  return common::Status{common::StatusCode::kInternal, std::move(message)};
}

[[nodiscard]] ingest::ColumnarRecoveryTabletConfig*
find_tablet(ingest::ColumnarAppendRecoveryConfig& config,
            const schema::TabletId& tablet_id) noexcept {
  const auto found = std::ranges::find(config.tablets, tablet_id,
                                       &ingest::ColumnarRecoveryTabletConfig::tablet_id);
  return found == config.tablets.end() ? nullptr : &*found;
}

[[nodiscard]] bool has_schema(const ingest::ColumnarRecoveryTabletConfig& configured,
                              const schema::SchemaId& schema_id,
                              const schema::SchemaVersion schema_version) noexcept {
  if (configured.schema != nullptr && configured.schema->schema_id() == schema_id &&
      configured.schema->version() == schema_version) {
    return true;
  }
  return std::ranges::any_of(configured.successors, [&](const auto& successor) {
    return successor.schema != nullptr && successor.schema->schema_id() == schema_id &&
           successor.schema->version() == schema_version;
  });
}

[[nodiscard]] common::Status
derive_durable_prefix(const LoadedManifestGeneration& manifest,
                      ingest::ColumnarAppendRecoveryConfig& recovery_config) {
  if (recovery_config.checkpoint.has_value()) {
    return invalid("Manifest startup derives the WAL checkpoint; caller checkpoint is forbidden");
  }
  for (const ingest::ColumnarRecoveryTabletConfig& tablet : recovery_config.tablets) {
    if (tablet.durable_seed.has_value()) {
      return invalid("Manifest startup derives tablet durable seeds; caller seed is forbidden");
    }
  }

  for (std::size_t index = 0U; index < recovery_config.tablets.size(); ++index) {
    const ingest::ColumnarRecoveryTabletConfig& candidate = recovery_config.tablets[index];
    if (candidate.schema == nullptr) {
      return invalid("Manifest startup tablet requires a retained schema lineage");
    }
    for (std::size_t other = index + 1U; other < recovery_config.tablets.size(); ++other) {
      if (candidate.tablet_id == recovery_config.tablets[other].tablet_id) {
        return invalid("Manifest startup recovery configuration repeats a tablet identity");
      }
    }
  }

  const WalCheckpoint& checkpoint = manifest.reclaim_checkpoint();
  recovery_config.checkpoint =
      wal::WalReplayCheckpoint{.wal_id = manifest.wal_id(),
                               .record_sequence = checkpoint.record_sequence,
                               .segment_number = checkpoint.segment_number,
                               .byte_offset = checkpoint.byte_offset};
  for (const TabletDescriptor& durable : manifest.tablets()) {
    ingest::ColumnarRecoveryTabletConfig* const configured =
        find_tablet(recovery_config, durable.tablet_id);
    if (configured == nullptr) {
      return common::Status{common::StatusCode::kNotFound,
                            "selected Manifest references an unconfigured recovery tablet"};
    }
    if (configured->schema == nullptr || configured->schema->table_id() != durable.table_id ||
        !has_schema(*configured, durable.recovery_schema_id, durable.recovery_schema_version)) {
      return corruption("selected Manifest tablet does not bind its recovery lineage");
    }

    std::vector<ingest::ColumnarRecoveryRetrySeed> retries;
    for (const RetryDescriptor& retry : manifest.retries()) {
      if (retry.tablet_id != durable.tablet_id) {
        continue;
      }
      retries.push_back(ingest::ColumnarRecoveryRetrySeed{
          .identity = ingest::RetryIdentity{.client_id = retry.client_id,
                                            .client_batch_id = retry.client_batch_id},
          .outcome = ingest::ColumnarAppendRetryOutcome{
              .mutation =
                  ingest::ColumnarAppendMutationIdentity{.table_id = retry.table_id,
                                                         .tablet_id = retry.tablet_id,
                                                         .request_digest = retry.request_digest},
              .wal_id = retry.wal_id,
              .record_sequence = retry.record_sequence,
              .applied_row_count = retry.applied_row_count}});
    }
    configured->durable_seed = ingest::ColumnarRecoveryTabletSeed{
        .recovery_schema_id = durable.recovery_schema_id,
        .recovery_schema_version = durable.recovery_schema_version,
        .durable_record_sequence = durable.durable_record_sequence,
        .retries = std::move(retries)};
  }
  return common::Status::ok();
}

} // namespace

class RecoveredManifestColumnarState::Impl {
public:
  Impl(ManifestStorage manifest_storage, ingest::RecoveredColumnarAppendState columnar_state,
       DatabaseStoragePublisher storage_publisher, ManifestColumnarStartupReport report) noexcept
      : manifest_storage_(std::move(manifest_storage)), columnar_state_(std::move(columnar_state)),
        storage_publisher_(std::move(storage_publisher)), report_(report) {}

  // Destruction is reverse declaration order: publication, WAL-bearing columnar state, Manifest.
  ManifestStorage manifest_storage_;
  ingest::RecoveredColumnarAppendState columnar_state_;
  DatabaseStoragePublisher storage_publisher_;
  ManifestColumnarStartupReport report_;
};

RecoveredManifestColumnarState::RecoveredManifestColumnarState(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}

RecoveredManifestColumnarState::~RecoveredManifestColumnarState() = default;
RecoveredManifestColumnarState::RecoveredManifestColumnarState(
    RecoveredManifestColumnarState&&) noexcept = default;
RecoveredManifestColumnarState&
RecoveredManifestColumnarState::operator=(RecoveredManifestColumnarState&&) noexcept = default;

const ManifestColumnarStartupReport& RecoveredManifestColumnarState::report() const noexcept {
  return implementation_->report_;
}

common::Result<DatabaseStorageSnapshot> RecoveredManifestColumnarState::snapshot() const {
  return implementation_->storage_publisher_.snapshot();
}

ingest::RetryDirectory& RecoveredManifestColumnarState::retry_directory() noexcept {
  return implementation_->columnar_state_.retry_directory();
}

ingest::TabletState*
RecoveredManifestColumnarState::tablet(const schema::TabletId& tablet_id) noexcept {
  return implementation_->columnar_state_.tablet(tablet_id);
}

ManifestStorage& RecoveredManifestColumnarState::manifest_storage() noexcept {
  return implementation_->manifest_storage_;
}
const ManifestStorage& RecoveredManifestColumnarState::manifest_storage() const noexcept {
  return implementation_->manifest_storage_;
}

DatabaseStoragePublisher& RecoveredManifestColumnarState::storage_publisher() noexcept {
  return implementation_->storage_publisher_;
}

common::Result<wal::WalWriter> RecoveredManifestColumnarState::release_writer() {
  return implementation_->columnar_state_.release_writer();
}

common::Result<RecoveredManifestColumnarState>
recover_manifest_columnar_database(ManifestColumnarStartupConfig config) {
  try {
    common::Result<ManifestStorage> storage =
        ManifestStorage::open_existing(config.manifest_storage);
    if (!storage.has_value()) {
      return common::make_unexpected(storage.error());
    }
    common::Result<LoadedManifestGeneration> loaded =
        storage->load_selected_manifest(config.manifest_load);
    if (!loaded.has_value()) {
      return common::make_unexpected(loaded.error());
    }
    auto selected = std::make_shared<const LoadedManifestGeneration>(std::move(*loaded));

    common::Status derived = derive_durable_prefix(*selected, config.columnar_recovery);
    if (!derived.is_ok()) {
      return common::make_unexpected(std::move(derived));
    }
    std::vector<schema::TabletId> configured_tablets;
    configured_tablets.reserve(config.columnar_recovery.tablets.size());
    for (const ingest::ColumnarRecoveryTabletConfig& configured :
         config.columnar_recovery.tablets) {
      configured_tablets.push_back(configured.tablet_id);
    }
    common::Result<ingest::RecoveredColumnarAppendState> columnar =
        ingest::recover_columnar_append_wal(config.wal_writer, config.wal_recovery,
                                            std::move(config.columnar_recovery));
    if (!columnar.has_value()) {
      return common::make_unexpected(columnar.error());
    }

    common::Result<TemporaryCleanupReport> cleanup = storage->cleanup_temporaries();
    if (!cleanup.has_value()) {
      return common::make_unexpected(cleanup.error());
    }

    std::optional<wal::WalSegmentReclamationReport> wal_reclamation;
    if (config.reclaim_checkpointed_wal_segments) {
      const WalCheckpoint& durable = selected->reclaim_checkpoint();
      common::Result<wal::WalSegmentReclamationReport> reclaimed =
          columnar->reclaim_checkpointed_segments({.wal_id = selected->wal_id(),
                                                   .record_sequence = durable.record_sequence,
                                                   .segment_number = durable.segment_number,
                                                   .byte_offset = durable.byte_offset});
      if (!reclaimed.has_value()) {
        return common::make_unexpected(reclaimed.error());
      }
      wal_reclamation = *reclaimed;
    }

    std::vector<ingest::TabletSnapshot> snapshots;
    snapshots.reserve(columnar->tablet_count());
    for (const schema::TabletId& tablet_id : configured_tablets) {
      ingest::TabletState* const tablet = columnar->tablet(tablet_id);
      if (tablet == nullptr) {
        return common::make_unexpected(internal("recovered tablet disappeared before publication"));
      }
      common::Result<ingest::TabletSnapshot> snapshot = tablet->snapshot();
      if (!snapshot.has_value()) {
        return common::make_unexpected(snapshot.error());
      }
      snapshots.push_back(std::move(*snapshot));
    }
    std::vector<DatabaseStorageTabletInput> inputs;
    inputs.reserve(snapshots.size());
    for (const ingest::TabletSnapshot& snapshot : snapshots) {
      inputs.push_back(DatabaseStorageTabletInput{.snapshot = std::cref(snapshot)});
    }
    common::Result<DatabaseStoragePublisher> publisher =
        DatabaseStoragePublisher::create(selected, inputs);
    if (!publisher.has_value()) {
      return common::make_unexpected(publisher.error());
    }

    ManifestColumnarStartupReport report{.selected_generation = selected->generation(),
                                         .reclaim_checkpoint = selected->reclaim_checkpoint(),
                                         .tablet_count = selected->tablets().size(),
                                         .part_count = selected->parts().size(),
                                         .retry_count = selected->retries().size(),
                                         .orphan_part_count = selected->orphan_parts().size(),
                                         .temporary_cleanup = *cleanup,
                                         .wal_reclamation = wal_reclamation};
    auto implementation = std::make_unique<RecoveredManifestColumnarState::Impl>(
        std::move(*storage), std::move(*columnar), std::move(*publisher), report);
    return RecoveredManifestColumnarState{std::move(implementation)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Manifest columnar startup allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("Manifest columnar startup configuration exceeds container limits"));
  } catch (const std::exception& error) {
    return common::make_unexpected(
        internal(std::string{"Manifest columnar startup threw: "} + error.what()));
  } catch (...) {
    return common::make_unexpected(
        internal("Manifest columnar startup threw an unknown exception"));
  }
}

} // namespace chronos::manifest
