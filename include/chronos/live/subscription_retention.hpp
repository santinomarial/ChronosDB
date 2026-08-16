#ifndef CHRONOS_LIVE_SUBSCRIPTION_RETENTION_HPP_
#define CHRONOS_LIVE_SUBSCRIPTION_RETENTION_HPP_

#include "chronos/common/result.hpp"
#include "chronos/live/durable_multi_tablet_subscription.hpp"
#include "chronos/raft/metadata.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

namespace chronos::live {

struct SubscriptionRetentionMember {
  SubscriptionRetentionMember(schema::TabletId tablet, wal::WalId wal, std::uint64_t epoch) noexcept
      : tablet_id(tablet), wal_id(wal), placement_epoch(epoch) {}

  schema::TabletId tablet_id;
  wal::WalId wal_id;
  std::uint64_t placement_epoch{};
  SubscriptionSourceKind source_kind{SubscriptionSourceKind::kWal};
  common::Uuid raft_group_id;

  [[nodiscard]] static SubscriptionRetentionMember
  raft(schema::TabletId tablet, common::Uuid group_id, std::uint64_t epoch) noexcept {
    return SubscriptionRetentionMember{tablet, group_id, epoch};
  }

  [[nodiscard]] SourcePosition position(std::uint64_t sequence = 0U) const noexcept {
    return source_kind == SubscriptionSourceKind::kRaft
               ? SourcePosition::raft(tablet_id, raft_group_id, sequence)
               : SourcePosition::wal(tablet_id, wal_id, sequence);
  }

  [[nodiscard]] bool is_valid() const noexcept {
    return placement_epoch != 0U && position().is_valid();
  }

private:
  SubscriptionRetentionMember(schema::TabletId tablet, common::Uuid group_id,
                              std::uint64_t epoch) noexcept
      : tablet_id(tablet), placement_epoch(epoch), source_kind(SubscriptionSourceKind::kRaft),
        raft_group_id(group_id) {}
};

struct SubscriptionSourceReclamation {
  SourcePosition reclaim_through;
  std::uint64_t placement_epoch{};
  raft::LogIndex metadata_index{};

  friend bool operator==(const SubscriptionSourceReclamation&,
                         const SubscriptionSourceReclamation&) = default;
};

// Source-specific deletion owner. Implementations map logical source positions to their own
// physical log coordinates, prevalidate the complete batch, and perform idempotent prefix cleanup.
// Success means every returned logical authorization is durably safe to repeat; it does not require
// byte-granular deletion when an active segment still contains the boundary.
class SubscriptionSourceReclaimer {
public:
  virtual ~SubscriptionSourceReclaimer() = default;
  [[nodiscard]] virtual common::Status
  reclaim(std::span<const SubscriptionSourceReclamation> requests) = 0;
};

struct SubscriptionRetentionConfig {
  common::Uuid database_id;
  schema::TableId table_id;
  raft::NodeId local_node_id{};
  std::vector<SubscriptionRetentionMember> members;
  // This is the complete set of durable plan coordinators allowed to promise resume for the fixed
  // source set. They and their checkpoint stores outlive this authority.
  std::vector<const DurableMultiTabletSubscription*> subscription_owners;
  std::size_t maximum_subscription_owners{1024U};
};

struct SubscriptionRetentionReport {
  raft::LogIndex metadata_index{};
  bool blocked_on_subscription_checkpoint{};
  bool advanced{};
  std::vector<SourcePosition> authorized_frontiers;
};

// Thread-affine deletion authority for one fixed table/source topology. A caller supplies the
// storage/Raft-safe logical frontier on each attempt. The authority takes the component-wise
// minimum with every durable subscription coordinator and rejects placement epoch or replica drift
// before invoking the physical source owner.
class SubscriptionRetentionCoordinator {
public:
  SubscriptionRetentionCoordinator() = delete;
  ~SubscriptionRetentionCoordinator();
  SubscriptionRetentionCoordinator(const SubscriptionRetentionCoordinator&) = delete;
  SubscriptionRetentionCoordinator& operator=(const SubscriptionRetentionCoordinator&) = delete;
  SubscriptionRetentionCoordinator(SubscriptionRetentionCoordinator&&) noexcept;
  SubscriptionRetentionCoordinator& operator=(SubscriptionRetentionCoordinator&&) noexcept;

  [[nodiscard]] static common::Result<SubscriptionRetentionCoordinator>
  create(SubscriptionRetentionConfig config);

  [[nodiscard]] common::Result<SubscriptionRetentionReport>
  advance(const raft::MetadataStateMachine& metadata,
          std::span<const SourcePosition> storage_safe_frontiers,
          SubscriptionSourceReclaimer& reclaimer);

  [[nodiscard]] std::span<const SourcePosition> authorized_frontiers() const noexcept;

private:
  class Impl;
  explicit SubscriptionRetentionCoordinator(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::live

#endif // CHRONOS_LIVE_SUBSCRIPTION_RETENTION_HPP_
