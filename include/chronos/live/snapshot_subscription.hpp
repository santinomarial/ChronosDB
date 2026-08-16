#ifndef CHRONOS_LIVE_SNAPSHOT_SUBSCRIPTION_HPP_
#define CHRONOS_LIVE_SNAPSHOT_SUBSCRIPTION_HPP_

#include "chronos/common/result.hpp"
#include "chronos/live/subscription.hpp"
#include "chronos/manifest/publication.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/network/protocol.hpp"
#include "chronos/network/subscription_messages.hpp"
#include "chronos/query/physical_plan.hpp"
#include "chronos/query/resource_context.hpp"
#include "chronos/query/snapshot_pipeline.hpp"
#include "chronos/query/tablet_state_pipeline.hpp"
#include "chronos/schema/schema_lineage.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace chronos::live {

struct SnapshotSubscriptionColumn {
  std::string name;
  schema::LogicalType type;
  bool nullable{};
};

struct SnapshotSubscriptionOutput {
  network::MessageType message_type{network::MessageType::kQueryResult};
  std::uint32_t flags{};
  std::vector<std::byte> payload;
};

struct SnapshotSubscriptionLimits {
  query::SnapshotTabletPipelineLimits pipeline{};
  query::TabletStatePipelineLimits raft_pipeline{};
  network::QueryResultLimits result{};
  network::SubscriptionMessageLimits subscription{};
};

// Drives one exact single-tablet historical plan from register-before-snapshot through Protocol
// 1.1 READY. The manager, resources, storage, publisher, lineage, and plan are borrowed only during
// start except for the manager and resources, which must outlive this thread-affine driver. The
// instantiated operator owns/pins its snapshot. Destroying the driver before READY cancels the
// registered subscription; after READY, the manager retains the live subscription independently.
class SnapshotSubscription {
public:
  SnapshotSubscription() = delete;
  ~SnapshotSubscription();
  SnapshotSubscription(const SnapshotSubscription&) = delete;
  SnapshotSubscription& operator=(const SnapshotSubscription&) = delete;
  SnapshotSubscription(SnapshotSubscription&&) noexcept;
  SnapshotSubscription& operator=(SnapshotSubscription&&) noexcept;

  [[nodiscard]] static common::Result<SnapshotSubscription>
  start(SubscriptionManager& manager, const SubscriptionRequest& request,
        const query::QueryResourceContext& resources, const manifest::ManifestStorage& storage,
        const manifest::DatabaseStoragePublisher& publisher, const schema::TabletId& target_tablet,
        const schema::SchemaLineage& lineage, schema::SchemaId destination_schema_id,
        const query::PhysicalPipelinePlan& plan, std::vector<SnapshotSubscriptionColumn> columns,
        SnapshotSubscriptionLimits limits = {});

  // Emits zero or more QUERY_RESULT batches, one empty END_STREAM QUERY_RESULT, then exactly one
  // SUBSCRIPTION_READY. Calling next after READY fails; live changes are polled from the manager.
  [[nodiscard]] common::Result<SnapshotSubscriptionOutput> next();
  [[nodiscard]] bool ready() const noexcept;
  [[nodiscard]] const common::Uuid& subscription_id() const noexcept;

private:
  class Impl;
  explicit SnapshotSubscription(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::live

#endif // CHRONOS_LIVE_SNAPSHOT_SUBSCRIPTION_HPP_
