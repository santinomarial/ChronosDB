#ifndef CHRONOS_RAFT_TABLET_MOVEMENT_CHECKPOINT_STORAGE_HPP_
#define CHRONOS_RAFT_TABLET_MOVEMENT_CHECKPOINT_STORAGE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/raft/tablet_movement_checkpoint.hpp"
#include "chronos/raft/tablet_movement_checkpoint_reference.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace chronos::raft {

struct TabletMovementCheckpointStorageConfig {
  std::string directory_path;
  schema::TabletId tablet_id;
  TabletMovementCheckpointCodecLimits codec_limits;
  TabletMovementCheckpointReferenceCodecLimits reference_codec_limits;
  std::uint16_t file_permissions{0600U};
};

struct InstalledTabletMovementCheckpoint {
  std::uint64_t checkpoint_generation{};
  std::string file_name;
  bool already_present{};
};

struct LoadedTabletMovementCheckpoint {
  std::string file_name;
  TabletMovementCheckpointGeneration generation;
  std::vector<std::byte> bytes;
};

using TabletMovementCheckpointGenerationValue =
    std::variant<TabletMovementCheckpointGeneration, TabletMovementCheckpointReferenceGeneration>;

struct LoadedTabletMovementCheckpointGeneration {
  std::string file_name;
  TabletMovementCheckpointGenerationValue generation;
  std::vector<std::byte> bytes;
};

[[nodiscard]] common::Result<std::string>
tablet_movement_checkpoint_generation_file_name(std::uint64_t checkpoint_generation);

// One locked directory owns immutable, contiguous checkpoint generations for one exact tablet.
// Each final is exact-magic-dispatched as a self-contained checkpoint or external-prefix reference.
// Installation exact-validates a temporary, synchronizes it, atomically renames without replacing,
// and synchronizes the directory before reporting durable success. A directory-sync failure after
// rename poisons the live owner because the installed name's crash durability is uncertain.
class TabletMovementCheckpointStorage {
public:
  TabletMovementCheckpointStorage() = delete;
  ~TabletMovementCheckpointStorage();
  TabletMovementCheckpointStorage(const TabletMovementCheckpointStorage&) = delete;
  TabletMovementCheckpointStorage& operator=(const TabletMovementCheckpointStorage&) = delete;
  TabletMovementCheckpointStorage(TabletMovementCheckpointStorage&&) noexcept;
  TabletMovementCheckpointStorage& operator=(TabletMovementCheckpointStorage&&) noexcept;

  [[nodiscard]] static common::Result<TabletMovementCheckpointStorage>
  create(TabletMovementCheckpointStorageConfig config);
  [[nodiscard]] static common::Result<TabletMovementCheckpointStorage>
  open_existing(TabletMovementCheckpointStorageConfig config);

  [[nodiscard]] common::Result<InstalledTabletMovementCheckpoint>
  install(const TabletMovementCheckpointGeneration& generation);
  [[nodiscard]] common::Result<InstalledTabletMovementCheckpoint>
  install_reference(const TabletMovementCheckpointReferenceGeneration& generation);
  [[nodiscard]] common::Result<LoadedTabletMovementCheckpoint>
  load_generation(std::uint64_t checkpoint_generation) const;
  [[nodiscard]] common::Result<std::optional<LoadedTabletMovementCheckpoint>> load_latest() const;
  [[nodiscard]] common::Result<LoadedTabletMovementCheckpointGeneration>
  load_any_generation(std::uint64_t checkpoint_generation) const;
  [[nodiscard]] common::Result<std::optional<LoadedTabletMovementCheckpointGeneration>>
  load_latest_any() const;

  [[nodiscard]] bool is_usable() const noexcept;
  [[nodiscard]] common::Status poison_status() const;

private:
  class Impl;
  [[nodiscard]] static common::Result<TabletMovementCheckpointStorage>
  open(TabletMovementCheckpointStorageConfig config, bool create_lock);
  explicit TabletMovementCheckpointStorage(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::raft

#endif // CHRONOS_RAFT_TABLET_MOVEMENT_CHECKPOINT_STORAGE_HPP_
