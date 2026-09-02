#ifndef CHRONOS_INGEST_RAFT_TABLET_SNAPSHOT_STORAGE_HPP_
#define CHRONOS_INGEST_RAFT_TABLET_SNAPSHOT_STORAGE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/ingest/raft_tablet_snapshot.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace chronos::io::detail {
class PosixSyscalls;
}

namespace chronos::ingest {

namespace detail {
class RaftTabletSnapshotStorageTestAccess;
}

struct RaftTabletSnapshotStorageConfig {
  std::string directory_path;
  raft::GroupId group_id;
  RaftTabletSnapshotCodecLimits codec_limits{};
  std::uint16_t file_permissions{0600U};
};

struct InstalledRaftTabletSnapshot {
  raft::LogIndex last_included_index{};
  std::string file_name;
  bool already_present{false};
};

struct LoadedRaftTabletSnapshot {
  std::string file_name;
  RaftTabletApplicationSnapshot snapshot;
  std::vector<std::byte> bytes;
};

struct RaftTabletSnapshotReclamationReport {
  std::optional<raft::LogIndex> authoritative_index;
  std::size_t reclaimed_files{};
};

// Process-local single-owner counters. Successful cleanup counters advance only after the related
// directory synchronization succeeds; failures can therefore leave uncounted namespace changes.
struct RaftTabletSnapshotCleanupMetrics {
  std::uint64_t temporary_files_removed{};
  std::uint64_t temporary_directory_syncs{};
  std::uint64_t reclamation_attempts{};
  std::uint64_t reclamation_failures{};
  std::uint64_t reclaimed_files{};
  std::uint64_t reclamation_directory_syncs{};

  friend bool operator==(const RaftTabletSnapshotCleanupMetrics&,
                         const RaftTabletSnapshotCleanupMetrics&) = default;
};

[[nodiscard]] common::Result<std::string>
raft_tablet_snapshot_file_name(raft::LogIndex last_included_index);

// Single-thread-affine owner of one lock-protected durable directory for a single Raft group.
// Installation validates bytes before and after writing, synchronizes the temporary file, uses an
// atomic no-replace rename, then synchronizes the directory. A failure after rename poisons the
// owner because durability is uncertain. Recognized interrupted temporary files are removed when
// ownership is acquired.
class RaftTabletSnapshotStorage {
public:
  RaftTabletSnapshotStorage() = delete;
  ~RaftTabletSnapshotStorage();
  RaftTabletSnapshotStorage(const RaftTabletSnapshotStorage&) = delete;
  RaftTabletSnapshotStorage& operator=(const RaftTabletSnapshotStorage&) = delete;
  RaftTabletSnapshotStorage(RaftTabletSnapshotStorage&&) noexcept;
  RaftTabletSnapshotStorage& operator=(RaftTabletSnapshotStorage&&) noexcept;

  [[nodiscard]] static common::Result<RaftTabletSnapshotStorage>
  create(RaftTabletSnapshotStorageConfig config);
  [[nodiscard]] static common::Result<RaftTabletSnapshotStorage>
  open_existing(RaftTabletSnapshotStorageConfig config);

  [[nodiscard]] common::Result<InstalledRaftTabletSnapshot>
  install(const RaftTabletApplicationSnapshot& snapshot);
  [[nodiscard]] common::Result<LoadedRaftTabletSnapshot>
  load(raft::LogIndex last_included_index) const;
  [[nodiscard]] common::Result<std::optional<LoadedRaftTabletSnapshot>> load_latest() const;
  // Revalidates the optional Raft-authoritative snapshot, then removes every other canonical final
  // and synchronizes the directory. A null authority reclaims all crash-orphaned finals.
  [[nodiscard]] common::Result<RaftTabletSnapshotReclamationReport>
  reclaim_obsolete(std::optional<raft::LogIndex> authoritative_index);

  [[nodiscard]] bool is_usable() const noexcept;
  [[nodiscard]] common::Status poison_status() const;
  [[nodiscard]] RaftTabletSnapshotCleanupMetrics cleanup_metrics() const noexcept;

private:
  class Impl;
  [[nodiscard]] static common::Result<RaftTabletSnapshotStorage>
  open_with(RaftTabletSnapshotStorageConfig config, bool create_lock,
            io::detail::PosixSyscalls& syscalls);
  explicit RaftTabletSnapshotStorage(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;

  friend class detail::RaftTabletSnapshotStorageTestAccess;
};

} // namespace chronos::ingest

#endif // CHRONOS_INGEST_RAFT_TABLET_SNAPSHOT_STORAGE_HPP_
