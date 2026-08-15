#ifndef CHRONOS_CLUSTER_TABLET_PHYSICAL_PART_CHUNK_STORAGE_HPP_
#define CHRONOS_CLUSTER_TABLET_PHYSICAL_PART_CHUNK_STORAGE_HPP_

#include "chronos/cluster/tablet_physical_part_chunk.hpp"
#include "chronos/common/result.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace chronos::cluster {

inline constexpr std::size_t kMaximumTabletPhysicalPartChunkFiles = std::size_t{1U} << 20U;
inline constexpr std::size_t kDefaultMaximumTabletPhysicalPartChunkFiles =
    std::size_t{256U} * 1024U;

struct TabletPhysicalPartChunkStorageConfig {
  std::string directory_path;
  TabletPhysicalPartTransferSession session;
  TabletPhysicalPartChunkCodecLimits codec_limits;
  std::size_t maximum_chunks{kDefaultMaximumTabletPhysicalPartChunkFiles};
  std::uint16_t file_permissions{0600U};
};

struct InstalledTabletPhysicalPartChunk {
  std::uint64_t offset{};
  std::size_t payload_bytes{};
  std::string file_name;
  bool already_present{};
};

struct LoadedTabletPhysicalPartChunk {
  std::string file_name;
  TabletPhysicalPartChunk chunk;
  std::vector<std::byte> encoded_bytes;
};

struct CompletedTabletPhysicalPartTransfer {
  TabletPhysicalPartTransferSession session;
  std::uint64_t received_bytes{};
  std::size_t chunk_count{};
  ingest::Sha256Digest content_sha256;
};

struct ReclaimedTabletPhysicalPartReceipt {
  TabletPhysicalPartTransferSession session;
  std::size_t removed_chunks{};
  std::uint64_t removed_payload_bytes{};
  bool marker_already_present{};
};

[[nodiscard]] common::Result<std::string>
tablet_physical_part_chunk_file_name(std::uint64_t offset);

// One externally serialized, locked directory owns a single exact physical-part transfer. Every
// successful install is immutable, synchronized, and contiguous from offset zero. Reopen removes
// only canonical temporaries and exact-validates the complete durable prefix. finalize() rereads
// chunks one at a time and streams SHA-256; it never assembles the complete CSEG in memory.
class TabletPhysicalPartChunkStorage {
public:
  TabletPhysicalPartChunkStorage() = delete;
  ~TabletPhysicalPartChunkStorage();
  TabletPhysicalPartChunkStorage(const TabletPhysicalPartChunkStorage&) = delete;
  TabletPhysicalPartChunkStorage& operator=(const TabletPhysicalPartChunkStorage&) = delete;
  TabletPhysicalPartChunkStorage(TabletPhysicalPartChunkStorage&&) noexcept;
  TabletPhysicalPartChunkStorage& operator=(TabletPhysicalPartChunkStorage&&) noexcept;

  [[nodiscard]] static common::Result<TabletPhysicalPartChunkStorage>
  create(TabletPhysicalPartChunkStorageConfig config);
  [[nodiscard]] static common::Result<TabletPhysicalPartChunkStorage>
  open_existing(TabletPhysicalPartChunkStorageConfig config);

  [[nodiscard]] common::Result<InstalledTabletPhysicalPartChunk>
  install(const TabletPhysicalPartChunk& chunk);
  [[nodiscard]] common::Result<LoadedTabletPhysicalPartChunk>
  load_chunk(std::uint64_t offset) const;
  [[nodiscard]] common::Result<std::uint64_t> received_bytes() const;
  [[nodiscard]] common::Result<CompletedTabletPhysicalPartTransfer> finalize() const;

  // Installs a checksummed, session-bound durable reclamation marker before removing any receipt
  // chunk. Chunks are then removed from the highest offset downward with a directory sync after
  // each unlink, so every crash leaves either the complete transfer or one valid prefix. The
  // marker makes reopen continue cleanup and permanently rejects late chunk retries. Callers must
  // first obtain the cluster-level published-ownership/readiness proof.
  [[nodiscard]] common::Result<ReclaimedTabletPhysicalPartReceipt> reclaim();

  [[nodiscard]] common::Result<TabletPhysicalPartTransferSession> transfer_session() const;
  [[nodiscard]] bool is_reclaimed() const noexcept;

  [[nodiscard]] bool is_usable() const noexcept;
  [[nodiscard]] common::Status poison_status() const;

private:
  class Impl;
  [[nodiscard]] static common::Result<TabletPhysicalPartChunkStorage>
  open(TabletPhysicalPartChunkStorageConfig config, bool create_lock);
  explicit TabletPhysicalPartChunkStorage(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_TABLET_PHYSICAL_PART_CHUNK_STORAGE_HPP_
