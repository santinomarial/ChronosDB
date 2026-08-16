#ifndef CHRONOS_LIVE_MULTI_TABLET_SNAPSHOT_SUBSCRIPTION_HPP_
#define CHRONOS_LIVE_MULTI_TABLET_SNAPSHOT_SUBSCRIPTION_HPP_

#include "chronos/common/result.hpp"
#include "chronos/ingest/async_raft_tablet_application.hpp"
#include "chronos/live/multi_tablet_subscription.hpp"
#include "chronos/live/snapshot_subscription.hpp"
#include "chronos/manifest/publication.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/query/physical_plan.hpp"
#include "chronos/query/resource_context.hpp"
#include "chronos/schema/schema_lineage.hpp"

#include <memory>
#include <vector>

namespace chronos::live {

// Drives one historical plan over the coordinator's complete canonical tablet vector from one
// exact aggregate storage epoch. The global physical pipeline runs once above all tablet scans.
// Destruction or failure before READY cancels the registered subscription.
class MultiTabletSnapshotSubscription {
public:
  MultiTabletSnapshotSubscription() = delete;
  ~MultiTabletSnapshotSubscription();
  MultiTabletSnapshotSubscription(const MultiTabletSnapshotSubscription&) = delete;
  MultiTabletSnapshotSubscription& operator=(const MultiTabletSnapshotSubscription&) = delete;
  MultiTabletSnapshotSubscription(MultiTabletSnapshotSubscription&&) noexcept;
  MultiTabletSnapshotSubscription& operator=(MultiTabletSnapshotSubscription&&) noexcept;

  [[nodiscard]] static common::Result<MultiTabletSnapshotSubscription>
  start(MultiTabletSubscriptionManager& manager, const SubscriptionRequest& request,
        const query::QueryResourceContext& resources, const manifest::ManifestStorage& storage,
        const manifest::DatabaseStoragePublisher& publisher, const schema::SchemaLineage& lineage,
        schema::SchemaId destination_schema_id, const query::PhysicalPipelinePlan& plan,
        std::vector<SnapshotSubscriptionColumn> columns, SnapshotSubscriptionLimits limits = {});

  // Raft-backed variant. Registration still precedes acquisition; every worker-owned immutable
  // tablet publication must exactly equal its registered group/index boundary or the registration
  // is abandoned. Only homogeneous Raft source sets are accepted.
  [[nodiscard]] static common::Result<MultiTabletSnapshotSubscription> start_raft(
      MultiTabletSubscriptionManager& manager, const SubscriptionRequest& request,
      const query::QueryResourceContext& resources,
      const ingest::AsyncRaftTabletApplication& application, const schema::SchemaLineage& lineage,
      schema::SchemaId destination_schema_id, const query::PhysicalPipelinePlan& plan,
      std::vector<SnapshotSubscriptionColumn> columns, SnapshotSubscriptionLimits limits = {});

  [[nodiscard]] common::Result<SnapshotSubscriptionOutput> next();
  [[nodiscard]] bool ready() const noexcept;
  [[nodiscard]] const common::Uuid& subscription_id() const noexcept;

private:
  class Impl;
  explicit MultiTabletSnapshotSubscription(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::live

#endif // CHRONOS_LIVE_MULTI_TABLET_SNAPSHOT_SUBSCRIPTION_HPP_
