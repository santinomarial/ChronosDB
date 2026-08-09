#ifndef CHRONOS_LIVE_MULTI_TABLET_SUBSCRIPTION_HPP_
#define CHRONOS_LIVE_MULTI_TABLET_SUBSCRIPTION_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/live/subscription.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace chronos::live {

struct MultiTabletSubscriptionMember {
  schema::TabletId tablet_id;
  wal::WalId wal_id;
  // The committed source boundary already represented when this coordinator is created. Earlier
  // positions are not assumed retained by this owner.
  std::uint64_t committed_record_sequence{};
};

struct MultiTabletSubscriptionSource {
  common::Uuid database_id;
  schema::TableId table_id;
  PlanFingerprint plan_fingerprint{};
  schema::SchemaId schema_id;
  schema::SchemaVersion schema_version;
  std::vector<MultiTabletSubscriptionMember> members;
  ResumeTokenMacKey token_key{};
};

struct MultiTabletSubscriptionRegistration {
  // Canonical tablet-identity order. The historical query must use this exact vector.
  std::vector<SourcePosition> snapshot_boundaries;
  std::vector<std::byte> initial_resume_token;
};

struct MultiTabletSubscriptionStatus {
  SubscriptionPhase phase{SubscriptionPhase::kSnapshot};
  std::vector<SourcePosition> snapshot_boundaries;
  std::uint64_t last_assigned_sequence{};
  std::uint64_t last_acknowledged_sequence{};
  std::size_t buffered_changes{};
  std::size_t buffered_bytes{};
};

struct MultiTabletSubscriptionCheckpointSource {
  SourcePosition latest_position;
  std::uint64_t expired_through_sequence{};

  friend bool operator==(const MultiTabletSubscriptionCheckpointSource&,
                         const MultiTabletSubscriptionCheckpointSource&) = default;
};

// Exact retained coordinator state in canonical source order and recorded cross-tablet admission
// order. Token MAC keys and ephemeral subscriber buffers are deliberately external to this value.
struct MultiTabletSubscriptionCheckpoint {
  common::Uuid database_id;
  schema::TableId table_id;
  PlanFingerprint plan_fingerprint{};
  schema::SchemaId schema_id;
  schema::SchemaVersion schema_version;
  std::vector<MultiTabletSubscriptionCheckpointSource> sources;
  std::vector<CommittedChange> retained_changes;

  friend bool operator==(const MultiTabletSubscriptionCheckpoint&,
                         const MultiTabletSubscriptionCheckpoint&) = default;
};

// Single-thread-affine coordinator for one database/table/plan/schema and a fixed source set.
// Per-tablet input must be consecutive committed log order. Cross-tablet call order becomes the
// authoritative subscription delivery order and is retained for replay; it is not a database-wide
// commit order. Topology/source-set changes require a new coordinator and snapshot.
class MultiTabletSubscriptionManager {
public:
  MultiTabletSubscriptionManager() = delete;
  ~MultiTabletSubscriptionManager();
  MultiTabletSubscriptionManager(const MultiTabletSubscriptionManager&) = delete;
  MultiTabletSubscriptionManager& operator=(const MultiTabletSubscriptionManager&) = delete;
  MultiTabletSubscriptionManager(MultiTabletSubscriptionManager&&) noexcept;
  MultiTabletSubscriptionManager& operator=(MultiTabletSubscriptionManager&&) noexcept;

  [[nodiscard]] static common::Result<MultiTabletSubscriptionManager>
  create(MultiTabletSubscriptionSource source, SubscriptionLimits limits = {});
  [[nodiscard]] static common::Result<MultiTabletSubscriptionManager>
  restore(MultiTabletSubscriptionSource source, const MultiTabletSubscriptionCheckpoint& checkpoint,
          SubscriptionLimits limits = {});

  [[nodiscard]] common::Result<MultiTabletSubscriptionRegistration>
  register_subscription(const SubscriptionRequest& request);
  [[nodiscard]] common::Result<MultiTabletSubscriptionRegistration>
  resume_subscription(common::ByteView encoded_token);
  [[nodiscard]] common::Status complete_snapshot(const common::Uuid& subscription_id);
  [[nodiscard]] common::Status publish_committed(CommittedChange change);
  [[nodiscard]] common::Result<std::vector<DeliveryRecord>>
  poll(const common::Uuid& subscription_id, std::size_t maximum_records) const;
  [[nodiscard]] common::Result<std::vector<std::byte>>
  acknowledge(const common::Uuid& subscription_id, std::uint64_t delivery_sequence);
  [[nodiscard]] common::Result<std::vector<std::byte>> cancel(const common::Uuid& subscription_id);
  void abandon(const common::Uuid& subscription_id) noexcept;
  [[nodiscard]] common::Result<MultiTabletSubscriptionStatus>
  status(const common::Uuid& subscription_id) const;
  [[nodiscard]] common::Result<std::vector<SourcePosition>> latest_positions() const;
  [[nodiscard]] common::Result<MultiTabletSubscriptionCheckpoint> checkpoint() const;
  [[nodiscard]] const MultiTabletSubscriptionSource& source() const noexcept;

private:
  class Impl;
  explicit MultiTabletSubscriptionManager(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::live

#endif // CHRONOS_LIVE_MULTI_TABLET_SUBSCRIPTION_HPP_
