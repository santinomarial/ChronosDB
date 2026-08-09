#ifndef CHRONOS_LIVE_SUBSCRIPTION_HPP_
#define CHRONOS_LIVE_SUBSCRIPTION_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/live/resume_token.hpp"
#include "chronos/schema/identity.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

namespace chronos::live {

enum class LogicalChangeOperation : std::uint8_t {
  kUpsert = 1,
  kDelete = 2,
};

// One immutable committed logical result change. Payload and result key are canonical bytes owned
// by this object. Construction/publication is allowed only after the source mutation was accepted
// and applied; callers must never submit prepared or merely WAL-appended work.
struct CommittedChange {
  SourcePosition position;
  schema::SchemaId schema_id;
  schema::SchemaVersion schema_version;
  LogicalChangeOperation operation{LogicalChangeOperation::kUpsert};
  std::vector<std::byte> result_key;
  std::vector<std::byte> payload;
};

struct DeliveryRecord {
  std::uint64_t delivery_sequence{};
  std::shared_ptr<const CommittedChange> change;
};

enum class SubscriptionPhase : std::uint8_t {
  kSnapshot,
  kLive,
  kOverflowed,
  kCancelled,
};

struct SubscriptionLimits {
  std::size_t maximum_subscriptions{1024U};
  std::size_t maximum_retained_changes{65'536U};
  std::size_t maximum_retained_bytes{64U * 1024U * 1024U};
  std::size_t maximum_buffered_changes_per_subscription{4096U};
  std::size_t maximum_buffered_bytes_per_subscription{8U * 1024U * 1024U};
  std::size_t maximum_change_bytes{16U * 1024U * 1024U};
};

struct SubscriptionSource {
  common::Uuid database_id;
  schema::TableId table_id;
  schema::TabletId tablet_id;
  wal::WalId wal_id;
  ResumeTokenMacKey token_key{};
};

struct SubscriptionRequest {
  common::Uuid subscription_id;
  PlanFingerprint plan_fingerprint{};
  schema::SchemaId schema_id;
  schema::SchemaVersion schema_version;
};

struct SubscriptionRegistration {
  SourcePosition snapshot_boundary;
  std::vector<std::byte> initial_resume_token;
};

struct SubscriptionStatus {
  SubscriptionPhase phase{SubscriptionPhase::kSnapshot};
  SourcePosition snapshot_boundary;
  std::uint64_t last_assigned_sequence{};
  std::uint64_t last_acknowledged_sequence{};
  std::size_t buffered_changes{};
  std::size_t buffered_bytes{};
};

// Single-owner committed-change retention and handoff state for one current tablet/WAL lineage.
// Register, publish, poll, acknowledge, resume, and cancel are shard-thread-affine. Returned
// change owners remain immutable and valid independently. Per-subscriber overflow disconnects that
// subscriber and never rejects a committed source mutation.
class SubscriptionManager {
public:
  SubscriptionManager() = delete;
  ~SubscriptionManager();

  SubscriptionManager(const SubscriptionManager&) = delete;
  SubscriptionManager& operator=(const SubscriptionManager&) = delete;
  SubscriptionManager(SubscriptionManager&&) noexcept;
  SubscriptionManager& operator=(SubscriptionManager&&) noexcept;

  [[nodiscard]] static common::Result<SubscriptionManager>
  create(SubscriptionSource source, SubscriptionLimits limits = {});

  // Atomically registers state and selects the manager's latest committed boundary. Changes
  // published after the call are owned before this subscription can leave snapshot phase.
  [[nodiscard]] common::Result<SubscriptionRegistration>
  register_subscription(const SubscriptionRequest& request);

  // Reconstructs a subscription from an authenticated safe token and retained committed suffix.
  // Returns NOT_FOUND when the required suffix has expired; it never starts at the current tail.
  [[nodiscard]] common::Result<SubscriptionRegistration>
  resume_subscription(common::ByteView encoded_token);

  [[nodiscard]] common::Status complete_snapshot(const common::Uuid& subscription_id);

  // Publishes one already-applied committed change. Positions must be the exact consecutive source
  // sequence. Slow subscribers may transition to kOverflowed, but source retention still advances.
  [[nodiscard]] common::Status publish_committed(CommittedChange change);

  // Returns the oldest unacknowledged prefix without removing it. Repeated polls therefore provide
  // at-least-once delivery until acknowledge() advances the safe checkpoint.
  [[nodiscard]] common::Result<std::vector<DeliveryRecord>>
  poll(const common::Uuid& subscription_id, std::size_t maximum_records) const;

  [[nodiscard]] common::Result<std::vector<std::byte>>
  acknowledge(const common::Uuid& subscription_id, std::uint64_t delivery_sequence);

  [[nodiscard]] common::Result<std::vector<std::byte>>
  cancel(const common::Uuid& subscription_id);

  [[nodiscard]] common::Result<SubscriptionStatus>
  status(const common::Uuid& subscription_id) const;

private:
  class Impl;
  explicit SubscriptionManager(std::unique_ptr<Impl> impl) noexcept;

  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::live

#endif // CHRONOS_LIVE_SUBSCRIPTION_HPP_
