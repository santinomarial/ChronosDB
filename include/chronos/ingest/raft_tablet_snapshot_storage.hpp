#ifndef CHRONOS_INGEST_RAFT_TABLET_SNAPSHOT_STORAGE_HPP_
#define CHRONOS_INGEST_RAFT_TABLET_SNAPSHOT_STORAGE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/ingest/raft_tablet_snapshot.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace chronos::ingest {

struct RaftTabletSnapshotStorageConfig {
  std::string directory_path;
  raft::GroupId group_id;
  RaftTabletSnapshotCodecLimits codec_limits;
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

[[nodiscard]] common::Result<std::string>
raft_tablet_snapshot_file_name(raft::LogIndex last_included_index);

// One lock-protected durable directory for a single Raft group. Installation validates bytes before
// and after writing, synchronizes the temporary file, uses an atomic no-replace rename, then
// synchronizes the directory. A failure after rename poisons the owner because durability is
// uncertain. Recognized interrupted temporary files are removed when ownership is acquired.
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

  [[nodiscard]] bool is_usable() const noexcept;
  [[nodiscard]] common::Status poison_status() const;

private:
  class Impl;
  [[nodiscard]] static common::Result<RaftTabletSnapshotStorage>
  open(RaftTabletSnapshotStorageConfig config, bool create_lock);
  explicit RaftTabletSnapshotStorage(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::ingest

#endif // CHRONOS_INGEST_RAFT_TABLET_SNAPSHOT_STORAGE_HPP_
