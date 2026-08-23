#ifndef CHRONOS_RAFT_METADATA_SNAPSHOT_STORAGE_HPP_
#define CHRONOS_RAFT_METADATA_SNAPSHOT_STORAGE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/raft/metadata_snapshot.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace chronos::io::detail {
class PosixSyscalls;
}

namespace chronos::raft {

namespace detail {
class MetadataSnapshotStorageTestAccess;
}

struct MetadataSnapshotStorageConfig {
  std::string directory_path;
  GroupId group_id;
  MetadataSnapshotCodecLimits codec_limits;
  std::uint16_t file_permissions{0600U};
};

struct InstalledMetadataSnapshot {
  LogIndex last_included_index{};
  std::string file_name;
  bool already_present{false};
};

struct LoadedMetadataSnapshot {
  std::string file_name;
  MetadataApplicationSnapshot snapshot;
  std::vector<std::byte> bytes;
};

struct MetadataSnapshotReclamationReport {
  std::optional<LogIndex> authoritative_index;
  std::size_t reclaimed_files{};
};

// Process-local single-owner counters. Successful cleanup counters advance only after the related
// directory synchronization succeeds; failures can therefore leave uncounted namespace changes.
struct MetadataSnapshotCleanupMetrics {
  std::uint64_t temporary_files_removed{};
  std::uint64_t temporary_directory_syncs{};
  std::uint64_t reclamation_attempts{};
  std::uint64_t reclamation_failures{};
  std::uint64_t reclaimed_files{};
  std::uint64_t reclamation_directory_syncs{};

  friend bool operator==(const MetadataSnapshotCleanupMetrics&,
                         const MetadataSnapshotCleanupMetrics&) = default;
};

[[nodiscard]] common::Result<std::string> metadata_snapshot_file_name(LogIndex last_included_index);

// Single-thread-affine owner of one lock-protected durable directory for one metadata group.
// Installation exact-validates, writes and rereads a temporary, synchronizes it, renames without
// replacement, and synchronizes the directory. Failure after rename poisons this process owner
// because durability is uncertain.
class MetadataSnapshotStorage {
public:
  MetadataSnapshotStorage() = delete;
  ~MetadataSnapshotStorage();
  MetadataSnapshotStorage(const MetadataSnapshotStorage&) = delete;
  MetadataSnapshotStorage& operator=(const MetadataSnapshotStorage&) = delete;
  MetadataSnapshotStorage(MetadataSnapshotStorage&&) noexcept;
  MetadataSnapshotStorage& operator=(MetadataSnapshotStorage&&) noexcept;

  [[nodiscard]] static common::Result<MetadataSnapshotStorage>
  create(MetadataSnapshotStorageConfig config);
  [[nodiscard]] static common::Result<MetadataSnapshotStorage>
  open_existing(MetadataSnapshotStorageConfig config);

  [[nodiscard]] common::Result<InstalledMetadataSnapshot>
  install(const MetadataApplicationSnapshot& snapshot);
  [[nodiscard]] common::Result<LoadedMetadataSnapshot> load(LogIndex last_included_index) const;
  [[nodiscard]] common::Result<std::optional<LoadedMetadataSnapshot>> load_latest() const;
  [[nodiscard]] common::Result<MetadataSnapshotReclamationReport>
  reclaim_obsolete(std::optional<LogIndex> authoritative_index);

  [[nodiscard]] bool is_usable() const noexcept;
  [[nodiscard]] common::Status poison_status() const;
  [[nodiscard]] MetadataSnapshotCleanupMetrics cleanup_metrics() const noexcept;

private:
  class Impl;
  [[nodiscard]] static common::Result<MetadataSnapshotStorage>
  open_with(MetadataSnapshotStorageConfig config, bool create_lock,
            io::detail::PosixSyscalls& syscalls);
  explicit MetadataSnapshotStorage(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;

  friend class detail::MetadataSnapshotStorageTestAccess;
};

} // namespace chronos::raft

#endif // CHRONOS_RAFT_METADATA_SNAPSHOT_STORAGE_HPP_
