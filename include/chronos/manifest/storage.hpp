#ifndef CHRONOS_MANIFEST_STORAGE_HPP_
#define CHRONOS_MANIFEST_STORAGE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "chronos/manifest/codec.hpp"
#include "chronos/manifest/compaction_equivalence.hpp"
#include "chronos/manifest/part_validation.hpp"
#include "chronos/manifest/retirement.hpp"
#include "chronos/manifest/types.hpp"
#include "chronos/manifest/validation.hpp"
#include "chronos/schema/table_schema.hpp"
#include "chronos/wal/types.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace chronos::io::detail {
class PosixSyscalls;
}

namespace chronos::manifest {

class LoadedManifestGeneration;

namespace detail {
class ManifestStorageTestAccess;
}

inline constexpr std::string_view kPartsDirectoryName = "parts";
inline constexpr std::string_view kManifestDirectoryName = "manifest";
inline constexpr std::string_view kManifestLockFileName = "LOCK";

struct ManifestStorageConfig {
  std::string database_root;
  std::uint16_t file_permissions{0600U};
};

struct PartInstallRequest {
  std::reference_wrapper<const cseg::EncodedCsegPart> encoded_part;
  PartDescriptor descriptor;
  wal::WalId wal_id;
  std::reference_wrapper<const schema::TableSchema> schema;
  common::Uuid nonce;
  ReferencedPartValidationLimits validation_limits;
};

struct InstalledPart {
  std::string file_name;
  PartDescriptor descriptor;
};

struct PartInstallationMetrics {
  std::uint64_t attempts{};
  std::uint64_t failures{};
  std::uint64_t installed_parts{};
  std::uint64_t installed_bytes{};
  std::uint64_t file_syncs{};
  std::uint64_t directory_syncs{};

  friend bool operator==(const PartInstallationMetrics&, const PartInstallationMetrics&) = default;
};

struct ManifestInstallRequest {
  std::reference_wrapper<const EncodedManifest> encoded_manifest;
  std::span<const TabletSchemaBinding> schema_bindings;
  common::Uuid nonce;
  ManifestDecodeLimits decode_limits;
  ReferencedPartValidationLimits part_validation_limits;
  // Null retains the Phase 6 add-only authority. A non-null replacement selects the Phase 7
  // compaction transition and causes storage to reread and independently prove exact on-disk
  // input/output equivalence before any Manifest candidate is created.
  const ManifestCompactionReplacement* compaction_replacement{};
  CompactionEquivalenceLimits compaction_equivalence_limits;
};

struct InstalledManifest {
  std::string file_name;
  std::uint64_t generation{};
  WalCheckpoint reclaim_checkpoint;
  std::uint64_t tablet_count{};
  std::uint64_t part_count{};
  std::uint64_t retry_count{};
};

struct ManifestInstallationMetrics {
  std::uint64_t attempts{};
  std::uint64_t failures{};
  std::uint64_t installed_generations{};
  std::uint64_t installed_bytes{};
  std::uint64_t referenced_parts_validated{};
  std::uint64_t file_syncs{};
  std::uint64_t directory_syncs{};

  friend bool operator==(const ManifestInstallationMetrics&,
                         const ManifestInstallationMetrics&) = default;
};

struct ManifestNamespaceSnapshot {
  std::vector<std::uint64_t> generations;
  std::vector<cseg::PartId> final_parts;
  std::vector<std::string> temporary_parts;
  std::vector<std::string> temporary_manifests;
};

struct TemporaryCleanupReport {
  std::uint64_t removed_parts{};
  std::uint64_t removed_manifests{};
  std::uint64_t directory_syncs{};

  friend bool operator==(const TemporaryCleanupReport&, const TemporaryCleanupReport&) = default;
};

enum class PartReclamationOutcome : std::uint8_t {
  kPending,
  kReclaimed,
};

struct PartReclamationRequest {
  std::reference_wrapper<const LoadedManifestGeneration> selected_manifest;
  std::reference_wrapper<const RetiredPartSet> retirement;
  ManifestDecodeLimits decode_limits;
};

struct PartReclamationReport {
  PartReclamationOutcome outcome{PartReclamationOutcome::kPending};
  std::uint64_t predecessor_generation{};
  std::uint64_t candidate_parts{};
  std::uint64_t removed_parts{};
  std::uint64_t removed_bytes{};
  std::uint64_t already_absent_parts{};
  std::uint64_t directory_syncs{};

  friend bool operator==(const PartReclamationReport&, const PartReclamationReport&) = default;
};

struct PartReclamationMetrics {
  std::uint64_t attempts{};
  std::uint64_t pending{};
  std::uint64_t failures{};
  std::uint64_t reclaimed_parts{};
  std::uint64_t reclaimed_bytes{};
  std::uint64_t already_absent_parts{};
  std::uint64_t directory_syncs{};

  friend bool operator==(const PartReclamationMetrics&, const PartReclamationMetrics&) = default;
};

struct ManifestLoadRequest {
  DatabaseId expected_database_id;
  wal::WalId expected_wal_id;
  std::span<const TabletSchemaBinding> schema_bindings;
  ManifestDecodeLimits decode_limits;
  ReferencedPartValidationLimits part_validation_limits;
};

class LoadedPartImage {
public:
  LoadedPartImage() = delete;
  LoadedPartImage(const LoadedPartImage&) = delete;
  LoadedPartImage& operator=(const LoadedPartImage&) = delete;
  LoadedPartImage(LoadedPartImage&&) noexcept = default;
  LoadedPartImage& operator=(LoadedPartImage&&) noexcept = default;

  [[nodiscard]] const PartDescriptor& descriptor() const noexcept;
  [[nodiscard]] common::ByteView bytes() const noexcept;

private:
  LoadedPartImage(PartDescriptor descriptor, std::vector<std::byte> bytes) noexcept;

  PartDescriptor descriptor_;
  std::vector<std::byte> bytes_;

  friend class ManifestStorage;
};

// One fully validated in-memory CSEG image selected by an exact aggregate database snapshot. The
// move-only owner pins that publication epoch, so compaction reclamation cannot unlink this part
// while its bytes or descriptor may still be used by a query.
class SnapshotPartImage {
public:
  SnapshotPartImage() = delete;
  SnapshotPartImage(const SnapshotPartImage&) = delete;
  SnapshotPartImage& operator=(const SnapshotPartImage&) = delete;
  SnapshotPartImage(SnapshotPartImage&&) noexcept = default;
  SnapshotPartImage& operator=(SnapshotPartImage&&) noexcept = default;

  [[nodiscard]] const DatabaseId& database_id() const noexcept;
  [[nodiscard]] const wal::WalId& wal_id() const noexcept;
  [[nodiscard]] std::uint64_t snapshot_generation() const noexcept;
  [[nodiscard]] const PartDescriptor& descriptor() const noexcept;
  [[nodiscard]] common::ByteView bytes() const noexcept;
  // Conservative complete publication pin plus owned image/object/allocation bytes. Separate
  // images from one snapshot intentionally report the shared epoch independently.
  [[nodiscard]] std::size_t retained_buffer_bytes() const noexcept;

private:
  SnapshotPartImage(DatabaseId database_id, wal::WalId wal_id, std::uint64_t snapshot_generation,
                    PartDescriptor descriptor, std::vector<std::byte> bytes,
                    DatabaseStorageRetentionToken retention,
                    std::size_t snapshot_retained_buffer_bytes) noexcept;

  DatabaseId database_id_;
  wal::WalId wal_id_;
  std::uint64_t snapshot_generation_{};
  PartDescriptor descriptor_;
  std::vector<std::byte> bytes_;
  DatabaseStorageRetentionToken retention_;
  std::size_t snapshot_retained_buffer_bytes_{};

  friend class ManifestStorage;
};

// Owns the exact selected Manifest bytes and parsed descriptor state. Returned spans and byte views
// remain valid until this move-only owner is destroyed or moved from. Orphan and temporary names
// are observations from the same locked namespace scan; no cleanup or publication is performed.
class LoadedManifestGeneration {
public:
  LoadedManifestGeneration() = delete;
  ~LoadedManifestGeneration();

  LoadedManifestGeneration(const LoadedManifestGeneration&) = delete;
  LoadedManifestGeneration& operator=(const LoadedManifestGeneration&) = delete;
  LoadedManifestGeneration(LoadedManifestGeneration&&) noexcept;
  LoadedManifestGeneration& operator=(LoadedManifestGeneration&&) noexcept;

  [[nodiscard]] std::uint64_t generation() const noexcept;
  [[nodiscard]] std::uint64_t previous_generation() const noexcept;
  [[nodiscard]] const DatabaseId& database_id() const noexcept;
  [[nodiscard]] const wal::WalId& wal_id() const noexcept;
  [[nodiscard]] const WalCheckpoint& reclaim_checkpoint() const noexcept;
  [[nodiscard]] std::span<const TabletDescriptor> tablets() const noexcept;
  [[nodiscard]] std::span<const PartDescriptor> parts() const noexcept;
  [[nodiscard]] std::span<const RetryDescriptor> retries() const noexcept;
  [[nodiscard]] common::ByteView encoded_bytes() const noexcept;
  [[nodiscard]] std::span<const cseg::PartId> orphan_parts() const noexcept;
  [[nodiscard]] std::span<const std::string> temporary_parts() const noexcept;
  [[nodiscard]] std::span<const std::string> temporary_manifests() const noexcept;
  // Conservative bytes retained by this loaded owner, including encoded/decoded descriptor,
  // namespace-observation capacities, object storage, and allocator allowances.
  [[nodiscard]] std::size_t retained_buffer_bytes() const noexcept;

private:
  class Impl;
  explicit LoadedManifestGeneration(std::unique_ptr<Impl> implementation) noexcept;

  std::unique_ptr<Impl> implementation_;

  friend class ManifestStorage;
};

// A move-only, single-threaded owner of the existing database root, parts directory, manifest
// directory, and manifest writer lock. open_existing() never creates missing directories or LOCK.
// The deployment must prevent out-of-band mutation while the lock is held.
class ManifestStorage {
public:
  ManifestStorage() = delete;
  ~ManifestStorage();

  ManifestStorage(const ManifestStorage&) = delete;
  ManifestStorage& operator=(const ManifestStorage&) = delete;
  ManifestStorage(ManifestStorage&&) noexcept;
  ManifestStorage& operator=(ManifestStorage&&) noexcept;

  [[nodiscard]] static common::Result<ManifestStorage>
  open_existing(const ManifestStorageConfig& config);

  // Implements the complete per-part installation ordering. The input and exact readback are both
  // validated before file sync. A final name is never replaced. Failure after rename but before
  // directory sync poisons this owner; restart/recovery must resolve the durable namespace.
  [[nodiscard]] common::Result<InstalledPart> install_part(const PartInstallRequest& request);

  // Installs exactly the next generation after the current highest final name. The selected
  // predecessor, candidate transition, catalog binding, and every referenced final CSEG image are
  // revalidated before mutation. Exact readback precedes file sync and a no-replace rename;
  // failure after rename but before directory sync poisons this owner.
  [[nodiscard]] common::Result<InstalledManifest>
  install_manifest(const ManifestInstallRequest& request);

  // Classifies both locked directories without following symlinks. Final manifest generations
  // must be nonempty and consecutive from one; every other entry must be an exact regular final or
  // recognized temporary name (plus manifest/LOCK). Final parts may be unreferenced orphans.
  [[nodiscard]] common::Result<ManifestNamespaceSnapshot> scan_namespace() const;

  // Re-scans first, removes only recognized temporaries, and synchronizes each changed directory.
  // It never promotes a candidate or removes a final part/generation.
  [[nodiscard]] common::Result<TemporaryCleanupReport> cleanup_temporaries();

  // Reclaims only compaction inputs whose exact predecessor publication no longer has a reader.
  // The current selected owner is reread and exact-compared before any unlink. A failure after
  // the first unlink poisons this storage owner until restart.
  [[nodiscard]] common::Result<PartReclamationReport>
  reclaim_retired_parts(const PartReclamationRequest& request);

  // Selects only the highest consecutive final generation, exact-decodes it without fallback,
  // binds configured identities/catalog state, validates every referenced final CSEG, and returns
  // an owned unpublished result. Recognized temporaries and unreferenced finals are only reported.
  [[nodiscard]] common::Result<LoadedManifestGeneration>
  load_selected_manifest(const ManifestLoadRequest& request) const;

  // Rereads exact strictly sorted part identities from the currently selected generation and
  // returns owning validated images. The supplied generation must still be the namespace maximum.
  [[nodiscard]] common::Result<std::vector<LoadedPartImage>>
  load_selected_part_images(const LoadedManifestGeneration& selected,
                            std::span<const cseg::PartId> part_ids,
                            std::span<const TabletSchemaBinding> schema_bindings,
                            ReferencedPartValidationLimits limits) const;

  // Loads exact parts from one held aggregate snapshot. Unlike the compaction-oriented selected
  // generation loader, this accepts a predecessor epoch after a newer Manifest is published. Each
  // returned image owns a retention token before file access and remains independently safe.
  [[nodiscard]] common::Result<std::vector<SnapshotPartImage>>
  load_snapshot_part_images(const DatabaseStorageSnapshot& snapshot,
                            std::span<const cseg::PartId> part_ids,
                            std::span<const TabletSchemaBinding> schema_bindings,
                            ReferencedPartValidationLimits limits) const;

  [[nodiscard]] bool is_usable() const noexcept;
  [[nodiscard]] common::Status poison_status() const;
  [[nodiscard]] PartInstallationMetrics metrics() const noexcept;
  [[nodiscard]] ManifestInstallationMetrics manifest_metrics() const noexcept;
  [[nodiscard]] PartReclamationMetrics reclamation_metrics() const noexcept;

private:
  class Impl;
  explicit ManifestStorage(std::unique_ptr<Impl> implementation) noexcept;
  [[nodiscard]] static common::Result<ManifestStorage>
  open_existing_with(const ManifestStorageConfig& config, io::detail::PosixSyscalls& syscalls);

  std::unique_ptr<Impl> implementation_;

  friend class detail::ManifestStorageTestAccess;
};

} // namespace chronos::manifest

#endif // CHRONOS_MANIFEST_STORAGE_HPP_
