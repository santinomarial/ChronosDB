#include "chronos/query/temporal_recovery.hpp"

#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/query/committed_temporal_command.hpp"
#include "chronos/wal/wal_replay_sink.hpp"

#include <algorithm>
#include <exception>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status corruption(std::string message) {
  return common::Status{common::StatusCode::kCorruption, std::move(message)};
}

[[nodiscard]] common::Status not_found(std::string message) {
  return common::Status{common::StatusCode::kNotFound, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] common::Status internal(std::string message) {
  return common::Status{common::StatusCode::kInternal, std::move(message)};
}

[[nodiscard]] common::Status unsupported(std::string message) {
  return common::Status{common::StatusCode::kNotSupported, std::move(message)};
}

template <typename Value>
[[nodiscard]] Value* optional_pointer(std::optional<Value>& value) noexcept {
  return value.has_value() ? std::addressof(*value) : nullptr;
}

template <typename Value>
[[nodiscard]] const Value* optional_pointer(const std::optional<Value>& value) noexcept {
  return value.has_value() ? std::addressof(*value) : nullptr;
}

[[nodiscard]] common::Status replay_error(const common::Status& status) {
  switch (status.code()) {
  case common::StatusCode::kInvalidArgument:
  case common::StatusCode::kAlreadyExists:
    return corruption("recovered temporal command violates committed history: " + status.message());
  default:
    return status;
  }
}

} // namespace

class RecoveredTemporalState::Impl {
public:
  struct TableEntry {
    std::shared_ptr<const schema::TableSchema> schema;
    std::unique_ptr<TemporalSnapshotProvider> provider;
    std::optional<schema::TabletId> tablet_id;
    std::uint64_t durable_position{};
    std::uint64_t verified_covered_commands{};
    std::uint64_t applied_commands{};
  };

  class ReplaySink final : public wal::WalReplaySink {
  public:
    explicit ReplaySink(Impl& owner) noexcept : owner_(owner) {}

    [[nodiscard]] common::Status preflight(const wal::WalReplayRecord& record) override {
      try {
        return owner_.preflight(record);
      } catch (const std::bad_alloc&) {
        return exhausted("temporal recovery preflight allocation failed");
      } catch (const std::length_error&) {
        return exhausted("temporal recovery preflight exceeded container limits");
      } catch (const std::exception& error) {
        return internal(std::string{"temporal recovery preflight threw: "} + error.what());
      } catch (...) {
        return internal("temporal recovery preflight threw an unknown exception");
      }
    }

    [[nodiscard]] common::Status replay(const wal::WalReplayRecord& record) override {
      try {
        return owner_.replay(record);
      } catch (const std::bad_alloc&) {
        return exhausted("temporal recovery replay allocation failed");
      } catch (const std::length_error&) {
        return exhausted("temporal recovery replay exceeded container limits");
      } catch (const std::exception& error) {
        return internal(std::string{"temporal recovery replay threw: "} + error.what());
      } catch (...) {
        return internal("temporal recovery replay threw an unknown exception");
      }
    }

  private:
    Impl& owner_;
  };

  Impl(std::vector<TableEntry> tables, const TemporalCommandLimits decode_limits) noexcept
      : tables_(std::move(tables)), decode_limits_(decode_limits) {}

  [[nodiscard]] TableEntry* find(const schema::TableId table_id) noexcept {
    const auto found = std::ranges::find_if(tables_, [table_id](const TableEntry& entry) {
      return entry.schema->table_id() == table_id;
    });
    return found == tables_.end() ? nullptr : &*found;
  }

  [[nodiscard]] const TableEntry* find(const schema::TableId table_id) const noexcept {
    const auto found = std::ranges::find_if(tables_, [table_id](const TableEntry& entry) {
      return entry.schema->table_id() == table_id;
    });
    return found == tables_.end() ? nullptr : &*found;
  }

  [[nodiscard]] common::Result<DecodedTemporalCommandView>
  decode(const wal::WalReplayRecord& record) const {
    auto command = decode_temporal_command_v1(record.payload, decode_limits_);
    if (!command.has_value()) {
      return common::make_unexpected(command.error());
    }
    return std::move(*command);
  }

  [[nodiscard]] common::Result<TableEntry*> resolve(const DecodedTemporalCommandView& command) {
    TableEntry* const target = find(command.batch().table_id());
    if (target == nullptr) {
      return common::make_unexpected(
          not_found("temporal command references an unconfigured recovery table"));
    }
    const common::Status schema_status =
        columnar::validate_columnar_batch_schema(command.batch(), *target->schema);
    if (!schema_status.is_ok()) {
      return common::make_unexpected(
          corruption("temporal command does not match its retained recovery schema: " +
                     schema_status.message()));
    }
    return target;
  }

  [[nodiscard]] common::Status preflight(const wal::WalReplayRecord& record) {
    auto command = decode(record);
    if (!command.has_value()) {
      return command.error();
    }
    auto target = resolve(*command);
    return target.has_value() ? common::Status::ok() : target.error();
  }

  [[nodiscard]] common::Status replay(const wal::WalReplayRecord& record) {
    auto command = decode(record);
    if (!command.has_value()) {
      return command.error();
    }
    auto target = resolve(*command);
    if (!target.has_value()) {
      return target.error();
    }
    if (record.header.record_sequence <= (*target)->durable_position) {
      auto verified = verify_retained_temporal_command(
          *command, *(*target)->schema, record.header.record_sequence, record.record_start.wal_id,
          *(*target)->provider);
      if (!verified.has_value()) {
        return replay_error(verified.error());
      }
      ++(*target)->verified_covered_commands;
      ++verified_covered_commands_;
      return common::Status::ok();
    }
    auto applied = apply_committed_temporal_command(
        *command, *(*target)->schema, record.header.record_sequence, record.record_start.wal_id,
        *(*target)->provider);
    if (!applied.has_value()) {
      return replay_error(applied.error());
    }
    ++(*target)->applied_commands;
    ++applied_commands_;
    return common::Status::ok();
  }

  std::vector<TableEntry> tables_;
  TemporalCommandLimits decode_limits_;
  std::optional<wal::WalWriter> writer_;
  std::uint64_t verified_covered_commands_{};
  std::uint64_t applied_commands_{};
};

class RecoveredManifestTemporalState::Impl {
public:
  Impl(manifest::ManifestStorage storage, RecoveredTemporalState temporal,
       std::shared_ptr<const manifest::LoadedTemporalManifestGeneration> selected,
       TemporalManifestWalStartupReport report) noexcept
      : storage_(std::move(storage)), temporal_(std::move(temporal)),
        selected_(std::move(selected)), report_(std::move(report)) {}

  // Reverse destruction releases the selected/provider state and WAL lock before Manifest storage.
  manifest::ManifestStorage storage_;
  RecoveredTemporalState temporal_;
  std::shared_ptr<const manifest::LoadedTemporalManifestGeneration> selected_;
  TemporalManifestWalStartupReport report_;
};

RecoveredTemporalState::RecoveredTemporalState(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
RecoveredTemporalState::~RecoveredTemporalState() = default;
RecoveredTemporalState::RecoveredTemporalState(RecoveredTemporalState&&) noexcept = default;
RecoveredTemporalState&
RecoveredTemporalState::operator=(RecoveredTemporalState&&) noexcept = default;

TemporalSnapshotProvider*
RecoveredTemporalState::provider(const schema::TableId table_id) noexcept {
  Impl::TableEntry* const entry = implementation_->find(table_id);
  return entry == nullptr ? nullptr : entry->provider.get();
}

const TemporalSnapshotProvider*
RecoveredTemporalState::provider(const schema::TableId table_id) const noexcept {
  const Impl::TableEntry* const entry = implementation_->find(table_id);
  return entry == nullptr ? nullptr : entry->provider.get();
}

std::size_t RecoveredTemporalState::table_count() const noexcept {
  return implementation_->tables_.size();
}

common::Result<wal::WalWriter> RecoveredTemporalState::release_writer() {
  wal::WalWriter* writer = optional_pointer(implementation_->writer_);
  if (writer == nullptr) {
    return common::make_unexpected(invalid("recovered temporal WAL writer was already released"));
  }
  wal::WalWriter output = std::move(*writer);
  implementation_->writer_.reset();
  return output;
}

common::Result<RecoveredTemporalState>
recover_temporal_wal(const wal::WalWriterConfig& writer_config,
                     const wal::WalRecoveryOptions& recovery_options,
                     TemporalRecoveryConfig recovery_config) {
  if (recovery_config.tables.empty()) {
    return common::make_unexpected(invalid("temporal recovery requires at least one table"));
  }
  try {
    std::vector<RecoveredTemporalState::Impl::TableEntry> tables;
    tables.reserve(recovery_config.tables.size());
    for (TemporalRecoveryTableConfig& configured : recovery_config.tables) {
      if (configured.schema == nullptr) {
        return common::make_unexpected(invalid("temporal recovery table requires a schema"));
      }
      if (std::ranges::any_of(tables, [&configured](const auto& existing) {
            return existing.schema->table_id() == configured.schema->table_id() ||
                   existing.schema->schema_id() == configured.schema->schema_id();
          })) {
        return common::make_unexpected(
            invalid("temporal recovery configuration repeats a table or schema identity"));
      }
      auto provider = TemporalSnapshotProvider::create(configured.schema, configured.store_limits);
      if (!provider.has_value()) {
        return common::make_unexpected(provider.error());
      }
      tables.push_back(RecoveredTemporalState::Impl::TableEntry{
          .schema = std::move(configured.schema), .provider = std::move(*provider)});
    }
    auto implementation = std::make_unique<RecoveredTemporalState::Impl>(
        std::move(tables), recovery_config.decode_limits);
    RecoveredTemporalState::Impl::ReplaySink sink{*implementation};
    auto writer = wal::WalWriter::open_existing(writer_config, recovery_options, sink);
    if (!writer.has_value()) {
      return common::make_unexpected(writer.error());
    }
    implementation->writer_.emplace(std::move(*writer));
    return RecoveredTemporalState{std::move(implementation)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("temporal recovery state allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("temporal recovery configuration exceeded container limits"));
  }
}

RecoveredManifestTemporalState::RecoveredManifestTemporalState(
    std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
RecoveredManifestTemporalState::~RecoveredManifestTemporalState() = default;
RecoveredManifestTemporalState::RecoveredManifestTemporalState(
    RecoveredManifestTemporalState&&) noexcept = default;
RecoveredManifestTemporalState&
RecoveredManifestTemporalState::operator=(RecoveredManifestTemporalState&&) noexcept = default;

const TemporalManifestWalStartupReport& RecoveredManifestTemporalState::report() const noexcept {
  return implementation_->report_;
}

TemporalSnapshotProvider* RecoveredManifestTemporalState::provider() noexcept {
  if (implementation_->temporal_.table_count() != 1U)
    return nullptr;
  return implementation_->temporal_.provider(implementation_->report_.tablets.front().table_id);
}

const TemporalSnapshotProvider* RecoveredManifestTemporalState::provider() const noexcept {
  if (implementation_->temporal_.table_count() != 1U)
    return nullptr;
  return implementation_->temporal_.provider(implementation_->report_.tablets.front().table_id);
}

TemporalSnapshotProvider*
RecoveredManifestTemporalState::provider(const schema::TableId table_id) noexcept {
  return implementation_->temporal_.provider(table_id);
}

const TemporalSnapshotProvider*
RecoveredManifestTemporalState::provider(const schema::TableId table_id) const noexcept {
  return implementation_->temporal_.provider(table_id);
}

std::size_t RecoveredManifestTemporalState::table_count() const noexcept {
  return implementation_->temporal_.table_count();
}

manifest::ManifestStorage& RecoveredManifestTemporalState::manifest_storage() noexcept {
  return implementation_->storage_;
}

const manifest::LoadedTemporalManifestGeneration&
RecoveredManifestTemporalState::selected_manifest() const noexcept {
  return *implementation_->selected_;
}

common::Result<wal::WalWriter> RecoveredManifestTemporalState::release_writer() {
  return implementation_->temporal_.release_writer();
}

common::Result<RecoveredManifestTemporalState>
recover_manifest_temporal_wal(TemporalManifestWalStartupConfig config) {
  try {
    common::Result<manifest::ManifestStorage> storage =
        manifest::ManifestStorage::open_existing(config.manifest_storage);
    if (!storage.has_value()) {
      return common::make_unexpected(storage.error());
    }
    common::Result<manifest::LoadedTemporalManifestGeneration> loaded =
        storage->load_selected_temporal_manifest(config.manifest_load);
    if (!loaded.has_value()) {
      return common::make_unexpected(loaded.error());
    }
    auto selected =
        std::make_shared<const manifest::LoadedTemporalManifestGeneration>(std::move(*loaded));
    if (selected->tablets().empty()) {
      return common::make_unexpected(
          unsupported("Manifest temporal WAL startup requires at least one WAL tablet"));
    }
    const manifest::TemporalWalReclaimCheckpoint* manifest_checkpoint =
        optional_pointer(selected->wal_reclaim_checkpoint());
    if (manifest_checkpoint == nullptr) {
      return common::make_unexpected(unsupported(
          "Manifest temporal WAL startup requires one durable global replay checkpoint"));
    }
    const wal::WalReplayCheckpoint checkpoint{
        .wal_id = manifest_checkpoint->wal_id,
        .record_sequence = manifest_checkpoint->coordinate.record_sequence,
        .segment_number = manifest_checkpoint->coordinate.segment_number,
        .byte_offset = manifest_checkpoint->coordinate.byte_offset};

    std::vector<RecoveredTemporalState::Impl::TableEntry> tables;
    tables.reserve(selected->tablets().size());
    std::uint64_t maximum_durable_position = 0U;
    std::uint64_t total_part_count = 0U;
    std::uint64_t total_version_count = 0U;
    for (const manifest::TemporalTabletDescriptor& tablet : selected->tablets()) {
      if (tablet.commit_source != cseg::temporal_format::CommitSource::kWal) {
        return common::make_unexpected(
            unsupported("Manifest temporal WAL startup cannot replay a Raft tablet"));
      }
      if (manifest_checkpoint->coordinate.record_sequence > tablet.durable_position) {
        return common::make_unexpected(
            corruption("Manifest temporal WAL checkpoint is ahead of a tablet durable boundary"));
      }
      if (std::ranges::any_of(tables, [&tablet](const auto& existing) {
            return existing.schema->table_id() == tablet.table_id;
          })) {
        return common::make_unexpected(unsupported(
            "Temporal Mutation Command v1 cannot route multiple tablets for one table"));
      }

      const auto binding = std::ranges::find(config.manifest_load.schema_bindings, tablet.tablet_id,
                                             &manifest::TabletSchemaBinding::tablet_id);
      if (binding == config.manifest_load.schema_bindings.end()) {
        return common::make_unexpected(
            not_found("Manifest temporal tablet has no configured recovery lineage"));
      }
      std::shared_ptr<const schema::TableSchema> recovery_schema =
          binding->lineage.get().find(tablet.recovery_schema_id);
      if (recovery_schema == nullptr || recovery_schema->table_id() != tablet.table_id ||
          recovery_schema->version() != tablet.recovery_schema_version) {
        return common::make_unexpected(
            corruption("Manifest temporal recovery schema disappeared after validation"));
      }

      std::unique_ptr<TemporalSnapshotProvider> provider;
      if (tablet.part_count == 0U) {
        common::Result<std::unique_ptr<TemporalSnapshotProvider>> fresh =
            TemporalSnapshotProvider::create(recovery_schema, config.store_limits);
        if (!fresh.has_value()) {
          return common::make_unexpected(fresh.error());
        }
        provider = std::move(*fresh);
      } else {
        if (!config.retained_system_time_ns.has_value()) {
          return common::make_unexpected(invalid(
              "Manifest temporal WAL startup requires an explicit retained history boundary"));
        }
        if (tablet.first_part_index > selected->parts().size() ||
            tablet.part_count > selected->parts().size() - tablet.first_part_index) {
          return common::make_unexpected(
              corruption("Manifest temporal tablet part range became inaccessible"));
        }
        const std::span<const manifest::TemporalPartDescriptor> descriptors =
            selected->parts().subspan(static_cast<std::size_t>(tablet.first_part_index),
                                      static_cast<std::size_t>(tablet.part_count));
        std::vector<cseg::PartId> part_ids;
        part_ids.reserve(descriptors.size());
        for (const manifest::TemporalPartDescriptor& descriptor : descriptors) {
          part_ids.push_back(descriptor.part_id);
        }
        common::Result<std::vector<manifest::LoadedTemporalPartImage>> images =
            storage->load_temporal_part_images(selected, part_ids,
                                               config.manifest_load.schema_bindings,
                                               config.manifest_load.part_validation_limits);
        if (!images.has_value()) {
          return common::make_unexpected(images.error());
        }
        std::vector<TemporalManifestCsegPartView> views;
        views.reserve(images->size());
        for (const manifest::LoadedTemporalPartImage& image : *images) {
          views.push_back(TemporalManifestCsegPartView{.descriptor = &image.descriptor(),
                                                       .bytes = image.bytes()});
        }
        common::Result<std::unique_ptr<TemporalSnapshotProvider>> restored =
            restore_manifest_v2_temporal_tablet_history(
                recovery_schema, binding->lineage.get(), tablet, views,
                {cseg::temporal_format::CommitSource::kWal, tablet.source_id},
                *config.retained_system_time_ns, config.store_limits, config.cseg_limits);
        if (!restored.has_value()) {
          return common::make_unexpected(restored.error());
        }
        provider = std::move(*restored);
      }

      if (tablet.part_count > std::numeric_limits<std::uint64_t>::max() - total_part_count ||
          tablet.durable_version_count >
              std::numeric_limits<std::uint64_t>::max() - total_version_count) {
        return common::make_unexpected(exhausted("Manifest temporal startup totals overflow"));
      }
      total_part_count += tablet.part_count;
      total_version_count += tablet.durable_version_count;
      maximum_durable_position = std::max(maximum_durable_position, tablet.durable_position);
      tables.push_back(
          RecoveredTemporalState::Impl::TableEntry{.schema = std::move(recovery_schema),
                                                   .provider = std::move(provider),
                                                   .tablet_id = tablet.tablet_id,
                                                   .durable_position = tablet.durable_position});
    }
    auto temporal_implementation =
        std::make_unique<RecoveredTemporalState::Impl>(std::move(tables), config.command_limits);
    RecoveredTemporalState::Impl::ReplaySink sink{*temporal_implementation};
    common::Result<wal::WalWriter> writer = wal::WalWriter::open_existing_from_checkpoint(
        config.wal_writer, config.wal_recovery, checkpoint, sink);
    if (!writer.has_value()) {
      return common::make_unexpected(writer.error());
    }
    common::Result<std::uint64_t> next_record_sequence = writer->next_record_sequence();
    if (!next_record_sequence.has_value()) {
      return common::make_unexpected(next_record_sequence.error());
    }
    if (*next_record_sequence <= maximum_durable_position) {
      return common::make_unexpected(
          corruption("temporal WAL ends before a Manifest tablet durable boundary"));
    }
    temporal_implementation->writer_.emplace(std::move(*writer));
    RecoveredTemporalState temporal{std::move(temporal_implementation)};

    std::vector<TemporalManifestWalStartupReport::Tablet> tablet_reports;
    tablet_reports.reserve(temporal.implementation_->tables_.size());
    for (const RecoveredTemporalState::Impl::TableEntry& entry :
         temporal.implementation_->tables_) {
      if (!entry.tablet_id.has_value()) {
        return common::make_unexpected(
            corruption("recovered temporal tablet report lost its routing identity"));
      }
      const auto descriptor = std::ranges::find(selected->tablets(), *entry.tablet_id,
                                                &manifest::TemporalTabletDescriptor::tablet_id);
      if (descriptor == selected->tablets().end()) {
        return common::make_unexpected(
            corruption("recovered temporal tablet report lost its descriptor"));
      }
      tablet_reports.push_back(TemporalManifestWalStartupReport::Tablet{
          .table_id = entry.schema->table_id(),
          .tablet_id = *entry.tablet_id,
          .durable_position = entry.durable_position,
          .verified_covered_command_count = entry.verified_covered_commands,
          .applied_suffix_command_count = entry.applied_commands,
          .part_count = descriptor->part_count,
          .durable_version_count = descriptor->durable_version_count});
    }
    common::Result<manifest::TemporaryCleanupReport> cleanup = storage->cleanup_temporaries();
    if (!cleanup.has_value()) {
      return common::make_unexpected(cleanup.error());
    }
    std::optional<wal::WalSegmentReclamationReport> wal_reclamation;
    if (config.reclaim_checkpointed_wal_segments) {
      wal::WalWriter* recovered_writer = optional_pointer(temporal.implementation_->writer_);
      if (recovered_writer == nullptr) {
        return common::make_unexpected(
            internal("Manifest temporal WAL startup lost its reopened writer"));
      }
      common::Result<wal::WalSegmentReclamationReport> reclaimed =
          recovered_writer->reclaim_checkpointed_segments(checkpoint);
      if (!reclaimed.has_value()) {
        return common::make_unexpected(reclaimed.error());
      }
      wal_reclamation = *reclaimed;
    }

    TemporalManifestWalStartupReport report{
        .selected_generation = selected->generation(),
        .checkpoint = checkpoint,
        .tablets = std::move(tablet_reports),
        .verified_covered_command_count = temporal.implementation_->verified_covered_commands_,
        .applied_suffix_command_count = temporal.implementation_->applied_commands_,
        .part_count = total_part_count,
        .durable_version_count = total_version_count,
        .retained_system_time_ns = config.retained_system_time_ns,
        .orphan_part_count = selected->orphan_parts().size(),
        .temporary_cleanup = *cleanup,
        .wal_reclamation = wal_reclamation};
    auto implementation = std::make_unique<RecoveredManifestTemporalState::Impl>(
        std::move(*storage), std::move(temporal), std::move(selected), std::move(report));
    return RecoveredManifestTemporalState{std::move(implementation)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Manifest temporal WAL startup allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("Manifest temporal WAL startup exceeded container limits"));
  } catch (const std::exception& error) {
    return common::make_unexpected(
        internal(std::string{"Manifest temporal WAL startup threw: "} + error.what()));
  } catch (...) {
    return common::make_unexpected(
        internal("Manifest temporal WAL startup threw an unknown exception"));
  }
}

} // namespace chronos::query
