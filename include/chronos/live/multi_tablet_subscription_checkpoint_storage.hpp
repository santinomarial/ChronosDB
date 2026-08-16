#ifndef CHRONOS_LIVE_MULTI_TABLET_SUBSCRIPTION_CHECKPOINT_STORAGE_HPP_
#define CHRONOS_LIVE_MULTI_TABLET_SUBSCRIPTION_CHECKPOINT_STORAGE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/live/multi_tablet_subscription_checkpoint.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace chronos::live {

struct MultiTabletSubscriptionCheckpointSourceIdentity {
  MultiTabletSubscriptionCheckpointSourceIdentity(schema::TabletId tablet, wal::WalId wal) noexcept
      : tablet_id(tablet), wal_id(wal) {}

  schema::TabletId tablet_id;
  wal::WalId wal_id;
  SubscriptionSourceKind source_kind{SubscriptionSourceKind::kWal};
  common::Uuid raft_group_id;

  [[nodiscard]] static MultiTabletSubscriptionCheckpointSourceIdentity
  raft(schema::TabletId tablet, common::Uuid group_id) noexcept {
    return {tablet, group_id};
  }

  [[nodiscard]] bool is_valid() const noexcept {
    if (source_kind == SubscriptionSourceKind::kWal)
      return SourcePosition::wal(tablet_id, wal_id, 0U).is_valid();
    return source_kind == SubscriptionSourceKind::kRaft &&
           SourcePosition::raft(tablet_id, raft_group_id, 0U).is_valid();
  }

  [[nodiscard]] SourcePosition position() const noexcept {
    if (source_kind == SubscriptionSourceKind::kRaft)
      return SourcePosition::raft(tablet_id, raft_group_id, 0U);
    return SourcePosition::wal(tablet_id, wal_id, 0U);
  }

private:
  MultiTabletSubscriptionCheckpointSourceIdentity(schema::TabletId tablet,
                                                  common::Uuid group_id) noexcept
      : tablet_id(tablet), source_kind(SubscriptionSourceKind::kRaft), raft_group_id(group_id) {}
};

struct MultiTabletSubscriptionCheckpointStorageIdentity {
  common::Uuid database_id;
  schema::TableId table_id;
  PlanFingerprint plan_fingerprint{};
  schema::SchemaId schema_id;
  schema::SchemaVersion schema_version;
  std::vector<MultiTabletSubscriptionCheckpointSourceIdentity> sources;
};

struct MultiTabletSubscriptionCheckpointStorageConfig {
  std::string directory_path;
  MultiTabletSubscriptionCheckpointStorageIdentity identity;
  MultiTabletSubscriptionCheckpointCodecLimits codec_limits;
  std::uint16_t file_permissions{0600U};
};

struct InstalledMultiTabletSubscriptionCheckpoint {
  std::uint64_t checkpoint_generation{};
  std::string file_name;
  bool already_present{};
};

struct LoadedMultiTabletSubscriptionCheckpoint {
  std::string file_name;
  BoundMultiTabletSubscriptionCheckpoint checkpoint;
  std::vector<std::byte> bytes;
};

[[nodiscard]] common::Result<std::string>
multi_tablet_subscription_checkpoint_generation_file_name(std::uint64_t checkpoint_generation);

// One locked directory for one exact database/table/plan/schema/source-set coordinator. Install
// writes and validates a temporary, synchronizes it, no-replace renames the next exact generation,
// then synchronizes the directory. A directory-sync failure after rename poisons this owner because
// crash durability is uncertain. Reopen removes only recognized canonical temporaries.
class MultiTabletSubscriptionCheckpointStorage {
public:
  MultiTabletSubscriptionCheckpointStorage() = delete;
  ~MultiTabletSubscriptionCheckpointStorage();
  MultiTabletSubscriptionCheckpointStorage(const MultiTabletSubscriptionCheckpointStorage&) =
      delete;
  MultiTabletSubscriptionCheckpointStorage&
  operator=(const MultiTabletSubscriptionCheckpointStorage&) = delete;
  MultiTabletSubscriptionCheckpointStorage(MultiTabletSubscriptionCheckpointStorage&&) noexcept;
  MultiTabletSubscriptionCheckpointStorage&
  operator=(MultiTabletSubscriptionCheckpointStorage&&) noexcept;

  [[nodiscard]] static common::Result<MultiTabletSubscriptionCheckpointStorage>
  create(MultiTabletSubscriptionCheckpointStorageConfig config);
  [[nodiscard]] static common::Result<MultiTabletSubscriptionCheckpointStorage>
  open_existing(MultiTabletSubscriptionCheckpointStorageConfig config);

  [[nodiscard]] common::Result<InstalledMultiTabletSubscriptionCheckpoint>
  install(const BoundMultiTabletSubscriptionCheckpoint& checkpoint);
  [[nodiscard]] common::Result<LoadedMultiTabletSubscriptionCheckpoint>
  load_generation(std::uint64_t checkpoint_generation) const;
  [[nodiscard]] common::Result<std::optional<LoadedMultiTabletSubscriptionCheckpoint>>
  load_latest() const;

  [[nodiscard]] bool is_usable() const noexcept;
  [[nodiscard]] common::Status poison_status() const;

private:
  class Impl;
  [[nodiscard]] static common::Result<MultiTabletSubscriptionCheckpointStorage>
  open(MultiTabletSubscriptionCheckpointStorageConfig config, bool create_lock);
  explicit MultiTabletSubscriptionCheckpointStorage(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::live

#endif // CHRONOS_LIVE_MULTI_TABLET_SUBSCRIPTION_CHECKPOINT_STORAGE_HPP_
