#ifndef CHRONOS_LIVE_DURABLE_MULTI_TABLET_SUBSCRIPTION_HPP_
#define CHRONOS_LIVE_DURABLE_MULTI_TABLET_SUBSCRIPTION_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/live/multi_tablet_snapshot_subscription.hpp"
#include "chronos/live/multi_tablet_subscription.hpp"
#include "chronos/live/multi_tablet_subscription_checkpoint_storage.hpp"
#include "chronos/live/subscription_plan.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::live {

struct DurableMultiTabletSubscriptionConfig {
  MultiTabletSubscriptionCheckpointStorageConfig storage;
  MultiTabletSubscriptionSource source;
  SubscriptionLimits limits;
};

// Single-thread-affine composition of one exact multi-tablet coordinator and its locked checkpoint
// directory. Only publish_committed() changes durable logical state. checkpoint() exposes new
// per-source retention frontiers only after the immutable generation and directory are
// synchronized.
class DurableMultiTabletSubscription {
public:
  DurableMultiTabletSubscription() = delete;
  ~DurableMultiTabletSubscription();
  DurableMultiTabletSubscription(const DurableMultiTabletSubscription&) = delete;
  DurableMultiTabletSubscription& operator=(const DurableMultiTabletSubscription&) = delete;
  DurableMultiTabletSubscription(DurableMultiTabletSubscription&&) noexcept;
  DurableMultiTabletSubscription& operator=(DurableMultiTabletSubscription&&) noexcept;

  [[nodiscard]] static common::Result<DurableMultiTabletSubscription>
  create_new(DurableMultiTabletSubscriptionConfig config);
  [[nodiscard]] static common::Result<DurableMultiTabletSubscription>
  open_existing(DurableMultiTabletSubscriptionConfig config);

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

  // Starts the exact historical half from a prepared or durably recovered plan without exposing
  // the mutable manager. This owner and the query resource context must outlive the returned
  // driver.
  [[nodiscard]] common::Result<MultiTabletSnapshotSubscription>
  start_snapshot(const PreparedSubscriptionPlan& plan, common::Uuid subscription_id,
                 const query::QueryResourceContext& resources,
                 const manifest::ManifestStorage& storage,
                 const manifest::DatabaseStoragePublisher& publisher,
                 const schema::SchemaLineage& lineage, SnapshotSubscriptionLimits limits = {});

  [[nodiscard]] common::Result<InstalledMultiTabletSubscriptionCheckpoint> checkpoint();
  [[nodiscard]] common::Result<std::optional<std::vector<SourcePosition>>>
  durable_retention_frontiers() const;
  [[nodiscard]] std::uint64_t checkpoint_generation() const noexcept;
  [[nodiscard]] bool has_uncheckpointed_changes() const noexcept;

private:
  class Impl;
  explicit DurableMultiTabletSubscription(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::live

#endif // CHRONOS_LIVE_DURABLE_MULTI_TABLET_SUBSCRIPTION_HPP_
