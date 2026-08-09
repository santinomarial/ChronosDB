#ifndef CHRONOS_QUERY_TEMPORAL_RECOVERY_HPP_
#define CHRONOS_QUERY_TEMPORAL_RECOVERY_HPP_

#include "chronos/common/result.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/query/temporal_command.hpp"
#include "chronos/query/temporal_cseg_snapshot.hpp"
#include "chronos/query/temporal_snapshot.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/table_schema.hpp"
#include "chronos/wal/wal_recovery.hpp"
#include "chronos/wal/wal_writer.hpp"
#include "chronos/wal/wal_writer_config.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::query {

struct TemporalManifestWalStartupConfig;
class RecoveredManifestTemporalState;

struct TemporalRecoveryTableConfig {
  std::shared_ptr<const schema::TableSchema> schema;
  TemporalStoreLimits store_limits;
};

struct TemporalRecoveryConfig {
  std::vector<TemporalRecoveryTableConfig> tables;
  TemporalCommandLimits decode_limits;
};

// Owns fresh temporal providers reconstructed after whole-WAL structural/schema preflight and the
// locked reopened WAL. Recovery supports exact retained schemas supplied by the catalog owner.
// release_writer() transfers the one writer to the live commit coordinator; it may be called once.
// Any preflight or replay failure destroys the complete fresh state, so no partial temporal history
// becomes query-visible.
class RecoveredTemporalState {
public:
  RecoveredTemporalState() = delete;
  ~RecoveredTemporalState();
  RecoveredTemporalState(const RecoveredTemporalState&) = delete;
  RecoveredTemporalState& operator=(const RecoveredTemporalState&) = delete;
  RecoveredTemporalState(RecoveredTemporalState&&) noexcept;
  RecoveredTemporalState& operator=(RecoveredTemporalState&&) noexcept;

  [[nodiscard]] TemporalSnapshotProvider* provider(schema::TableId table_id) noexcept;
  [[nodiscard]] const TemporalSnapshotProvider* provider(schema::TableId table_id) const noexcept;
  [[nodiscard]] std::size_t table_count() const noexcept;
  [[nodiscard]] common::Result<wal::WalWriter> release_writer();

private:
  class Impl;
  explicit RecoveredTemporalState(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;

  friend common::Result<RecoveredTemporalState> recover_temporal_wal(const wal::WalWriterConfig&,
                                                                     const wal::WalRecoveryOptions&,
                                                                     TemporalRecoveryConfig);
  friend common::Result<RecoveredManifestTemporalState>
      recover_manifest_temporal_wal(TemporalManifestWalStartupConfig);
};

// Opens and verifies one existing WAL containing Temporal Mutation Command v1 records, preflights
// every command and retained-schema binding, then replays in physical record order into fresh
// providers. Cross-record mutation transitions are validated during replay into disposable state.
[[nodiscard]] common::Result<RecoveredTemporalState>
recover_temporal_wal(const wal::WalWriterConfig& writer_config,
                     const wal::WalRecoveryOptions& recovery_options,
                     TemporalRecoveryConfig recovery_config);

struct TemporalManifestWalStartupConfig {
  manifest::ManifestStorageConfig manifest_storage;
  manifest::TemporalManifestLoadRequest manifest_load;
  wal::WalWriterConfig wal_writer;
  wal::WalRecoveryOptions wal_recovery;
  std::optional<std::int64_t> retained_system_time_ns;
  TemporalStoreLimits store_limits;
  TemporalManifestCsegResolutionLimits cseg_limits;
  TemporalCommandLimits command_limits;
  bool reclaim_checkpointed_wal_segments{false};
};

struct TemporalManifestWalStartupReport {
  std::uint64_t selected_generation{};
  wal::WalReplayCheckpoint checkpoint;
  schema::TabletId tablet_id;
  std::uint64_t tablet_durable_position{};
  std::uint64_t verified_covered_command_count{};
  std::uint64_t applied_suffix_command_count{};
  std::uint64_t part_count{};
  std::uint64_t durable_version_count{};
  std::optional<std::int64_t> retained_system_time_ns;
  std::size_t orphan_part_count{};
  manifest::TemporaryCleanupReport temporary_cleanup;
  std::optional<wal::WalSegmentReclamationReport> wal_reclamation;
};

// Owns the selected Manifest v2 generation, reconstructed temporal provider, reopened WAL writer,
// and both filesystem locks. This composition supports exactly one WAL tablet. A global reclaim
// checkpoint may equal or trail its durable boundary: intervening commands are verified against
// retained history and only commands after the tablet boundary are applied. Multi-tablet routing
// remains a separate application-snapshot contract.
class RecoveredManifestTemporalState {
public:
  RecoveredManifestTemporalState() = delete;
  ~RecoveredManifestTemporalState();
  RecoveredManifestTemporalState(const RecoveredManifestTemporalState&) = delete;
  RecoveredManifestTemporalState& operator=(const RecoveredManifestTemporalState&) = delete;
  RecoveredManifestTemporalState(RecoveredManifestTemporalState&&) noexcept;
  RecoveredManifestTemporalState& operator=(RecoveredManifestTemporalState&&) noexcept;

  [[nodiscard]] const TemporalManifestWalStartupReport& report() const noexcept;
  [[nodiscard]] TemporalSnapshotProvider* provider() noexcept;
  [[nodiscard]] const TemporalSnapshotProvider* provider() const noexcept;
  [[nodiscard]] manifest::ManifestStorage& manifest_storage() noexcept;
  [[nodiscard]] const manifest::LoadedTemporalManifestGeneration&
  selected_manifest() const noexcept;
  [[nodiscard]] common::Result<wal::WalWriter> release_writer();

private:
  class Impl;
  explicit RecoveredManifestTemporalState(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;

  friend common::Result<RecoveredManifestTemporalState>
      recover_manifest_temporal_wal(TemporalManifestWalStartupConfig);
};

// Acquires Manifest then WAL ownership, restores the selected tablet's complete CSEG history,
// preflights the WAL after the global checkpoint, verifies commands through the tablet durable
// boundary, applies only the later suffix, cleans recognized temporaries, and returns nothing
// usable unless the complete unpublished composition succeeds.
[[nodiscard]] common::Result<RecoveredManifestTemporalState>
recover_manifest_temporal_wal(TemporalManifestWalStartupConfig config);

} // namespace chronos::query

#endif // CHRONOS_QUERY_TEMPORAL_RECOVERY_HPP_
