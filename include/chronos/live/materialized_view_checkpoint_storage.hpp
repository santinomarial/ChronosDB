#ifndef CHRONOS_LIVE_MATERIALIZED_VIEW_CHECKPOINT_STORAGE_HPP_
#define CHRONOS_LIVE_MATERIALIZED_VIEW_CHECKPOINT_STORAGE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/live/materialized_view_checkpoint.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace chronos::live {

struct MaterializedViewCheckpointStorageConfig {
  std::string directory_path;
  MaterializedViewCheckpointIdentity identity;
  MaterializedViewCheckpointCodecLimits codec_limits;
  std::uint16_t file_permissions{0600U};
};

struct InstalledMaterializedViewCheckpoint {
  std::uint64_t record_sequence{};
  std::string file_name;
  bool already_present{false};
};

struct LoadedMaterializedViewCheckpoint {
  std::string file_name;
  BoundMaterializedViewCheckpoint checkpoint;
  std::vector<std::byte> bytes;
};

[[nodiscard]] common::Result<std::string>
materialized_view_checkpoint_file_name(std::uint64_t record_sequence);

// One lock-protected directory for one exact database/view/table/schema/plan identity. Installation
// exact-validates before and after writing, synchronizes the file, no-replace renames immutable
// sequence bytes, then synchronizes the directory. Recognized interrupted temporaries are removed
// when ownership is acquired.
class MaterializedViewCheckpointStorage {
public:
  MaterializedViewCheckpointStorage() = delete;
  ~MaterializedViewCheckpointStorage();
  MaterializedViewCheckpointStorage(const MaterializedViewCheckpointStorage&) = delete;
  MaterializedViewCheckpointStorage& operator=(const MaterializedViewCheckpointStorage&) = delete;
  MaterializedViewCheckpointStorage(MaterializedViewCheckpointStorage&&) noexcept;
  MaterializedViewCheckpointStorage& operator=(MaterializedViewCheckpointStorage&&) noexcept;

  [[nodiscard]] static common::Result<MaterializedViewCheckpointStorage>
  create(MaterializedViewCheckpointStorageConfig config);
  [[nodiscard]] static common::Result<MaterializedViewCheckpointStorage>
  open_existing(MaterializedViewCheckpointStorageConfig config);

  [[nodiscard]] common::Result<InstalledMaterializedViewCheckpoint>
  install(const BoundMaterializedViewCheckpoint& checkpoint);
  [[nodiscard]] common::Result<LoadedMaterializedViewCheckpoint>
  load(std::uint64_t record_sequence) const;
  [[nodiscard]] common::Result<std::optional<LoadedMaterializedViewCheckpoint>> load_latest() const;

  [[nodiscard]] bool is_usable() const noexcept;
  [[nodiscard]] common::Status poison_status() const;

private:
  class Impl;
  [[nodiscard]] static common::Result<MaterializedViewCheckpointStorage>
  open(MaterializedViewCheckpointStorageConfig config, bool create_lock);
  explicit MaterializedViewCheckpointStorage(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::live

#endif // CHRONOS_LIVE_MATERIALIZED_VIEW_CHECKPOINT_STORAGE_HPP_
