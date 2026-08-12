#ifndef CHRONOS_MANIFEST_STARTUP_RECOVERY_HPP_
#define CHRONOS_MANIFEST_STARTUP_RECOVERY_HPP_

#include "chronos/common/result.hpp"
#include "chronos/ingest/columnar_append_recovery.hpp"
#include "chronos/manifest/publication.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/wal/wal_recovery.hpp"
#include "chronos/wal/wal_writer.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>

namespace chronos::manifest {

struct ManifestColumnarStartupConfig {
  ManifestStorageConfig manifest_storage;
  ManifestLoadRequest manifest_load;
  wal::WalWriterConfig wal_writer;
  wal::WalRecoveryOptions wal_recovery;
  // Disabled by default. When enabled, startup revalidates and synchronously removes only closed
  // WAL segments covered by the selected durable Manifest checkpoint before publishing state.
  bool reclaim_checkpointed_wal_segments{false};
  // The caller supplies the complete retained catalog/tablet configuration but no checkpoint or
  // durable seeds. Recovery derives those fields only from the selected validated Manifest.
  ingest::ColumnarAppendRecoveryConfig columnar_recovery;
};

struct ManifestColumnarStartupReport {
  std::uint64_t selected_generation{};
  WalCheckpoint reclaim_checkpoint;
  std::size_t tablet_count{};
  std::size_t part_count{};
  std::size_t retry_count{};
  std::size_t orphan_part_count{};
  TemporaryCleanupReport temporary_cleanup;
  // Disengaged when cleanup was disabled; engaged even when no segment remained to remove.
  std::optional<wal::WalSegmentReclamationReport> wal_reclamation;

  friend bool operator==(const ManifestColumnarStartupReport&,
                         const ManifestColumnarStartupReport&) = default;
};

// Owns one fully recovered Manifest/CSEG/head publication, fresh columnar retry/tablet state, and
// both storage locks. Manifest ownership precedes WAL ownership and outlives it during ordinary
// destruction. The WAL writer may be released once for live coordination; that coordinator must
// be stopped before this owner is destroyed so lock release remains WAL-before-Manifest.
class RecoveredManifestColumnarState {
public:
  RecoveredManifestColumnarState() = delete;
  ~RecoveredManifestColumnarState();

  RecoveredManifestColumnarState(const RecoveredManifestColumnarState&) = delete;
  RecoveredManifestColumnarState& operator=(const RecoveredManifestColumnarState&) = delete;
  RecoveredManifestColumnarState(RecoveredManifestColumnarState&&) noexcept;
  RecoveredManifestColumnarState& operator=(RecoveredManifestColumnarState&&) noexcept;

  [[nodiscard]] const ManifestColumnarStartupReport& report() const noexcept;
  [[nodiscard]] common::Result<DatabaseStorageSnapshot> snapshot() const;
  [[nodiscard]] ingest::RetryDirectory& retry_directory() noexcept;
  [[nodiscard]] ingest::TabletState* tablet(const schema::TabletId& tablet_id) noexcept;
  [[nodiscard]] ManifestStorage& manifest_storage() noexcept;
  [[nodiscard]] const ManifestStorage& manifest_storage() const noexcept;
  [[nodiscard]] DatabaseStoragePublisher& storage_publisher() noexcept;
  [[nodiscard]] common::Result<wal::WalWriter> release_writer();

private:
  class Impl;
  explicit RecoveredManifestColumnarState(std::unique_ptr<Impl> implementation) noexcept;

  std::unique_ptr<Impl> implementation_;

  friend common::Result<RecoveredManifestColumnarState>
      recover_manifest_columnar_database(ManifestColumnarStartupConfig);
};

// Acquires Manifest then WAL ownership, validates the selected Manifest and every referenced part,
// derives exact durable columnar seeds, preflights/replays the required WAL suffix, cleans only
// recognized Manifest/part temporaries, and constructs one aggregate publication. Nothing usable
// escapes unless every step succeeds.
[[nodiscard]] common::Result<RecoveredManifestColumnarState>
recover_manifest_columnar_database(ManifestColumnarStartupConfig config);

} // namespace chronos::manifest

#endif // CHRONOS_MANIFEST_STARTUP_RECOVERY_HPP_
