#ifndef CHRONOS_RAFT_TABLET_MOVEMENT_SNAPSHOT_CHUNK_STORAGE_HPP_
#define CHRONOS_RAFT_TABLET_MOVEMENT_SNAPSHOT_CHUNK_STORAGE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/raft/tablet_movement_snapshot_chunk.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace chronos::raft {

inline constexpr std::size_t kMaximumTabletMovementSnapshotChunkFiles = std::size_t{1U} << 20U;

struct TabletMovementSnapshotChunkStorageConfig {
  std::string directory_path;
  TabletMovementSnapshotSession session;
  TabletMovementSnapshotChunkCodecLimits codec_limits;
  std::size_t maximum_chunks{std::size_t{256U} * 1024U};
  std::uint16_t file_permissions{0600U};
};

struct InstalledTabletMovementSnapshotChunk {
  std::uint64_t offset{};
  std::size_t payload_bytes{};
  std::string file_name;
  bool already_present{};
};

struct LoadedTabletMovementSnapshotChunk {
  std::string file_name;
  TabletMovementSnapshotChunk chunk;
  std::vector<std::byte> encoded_bytes;
};

[[nodiscard]] common::Result<std::string>
tablet_movement_snapshot_chunk_file_name(std::uint64_t offset);

// One locked directory owns one exact movement-snapshot session. Chunks are immutable and may be
// installed only at the current contiguous end. Installation exact-validates and synchronizes a
// temporary, renames without replacement, and synchronizes the directory before advancing the
// durable prefix. Reopen validates every recognized final into the same gap-free prefix. The owner
// is movable but not internally synchronized; callers serialize operations on one live instance.
class TabletMovementSnapshotChunkStorage {
public:
  TabletMovementSnapshotChunkStorage() = delete;
  ~TabletMovementSnapshotChunkStorage();
  TabletMovementSnapshotChunkStorage(const TabletMovementSnapshotChunkStorage&) = delete;
  TabletMovementSnapshotChunkStorage& operator=(const TabletMovementSnapshotChunkStorage&) = delete;
  TabletMovementSnapshotChunkStorage(TabletMovementSnapshotChunkStorage&&) noexcept;
  TabletMovementSnapshotChunkStorage& operator=(TabletMovementSnapshotChunkStorage&&) noexcept;

  [[nodiscard]] static common::Result<TabletMovementSnapshotChunkStorage>
  create(TabletMovementSnapshotChunkStorageConfig config);
  [[nodiscard]] static common::Result<TabletMovementSnapshotChunkStorage>
  open_existing(TabletMovementSnapshotChunkStorageConfig config);

  [[nodiscard]] common::Result<InstalledTabletMovementSnapshotChunk>
  install(const TabletMovementSnapshotChunk& chunk);
  [[nodiscard]] common::Result<LoadedTabletMovementSnapshotChunk>
  load_chunk(std::uint64_t offset) const;
  [[nodiscard]] common::Result<TabletMovementSnapshotSession> session() const;
  [[nodiscard]] common::Result<std::uint64_t> received_bytes() const;
  // Loads an exact installed chunk boundary. A durable suffix beyond that boundary is ignored.
  [[nodiscard]] common::Result<std::vector<std::byte>>
  load_prefix_through(std::uint64_t received_bytes) const;
  [[nodiscard]] common::Result<std::vector<std::byte>> load_received_prefix() const;
  // Completion requires the exact declared size and matching whole-snapshot content CRC32C.
  [[nodiscard]] common::Result<std::vector<std::byte>> finalize() const;

  [[nodiscard]] bool is_usable() const noexcept;
  [[nodiscard]] common::Status poison_status() const;

private:
  class Impl;
  [[nodiscard]] static common::Result<TabletMovementSnapshotChunkStorage>
  open(TabletMovementSnapshotChunkStorageConfig config, bool create_lock);
  explicit TabletMovementSnapshotChunkStorage(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::raft

#endif // CHRONOS_RAFT_TABLET_MOVEMENT_SNAPSHOT_CHUNK_STORAGE_HPP_
