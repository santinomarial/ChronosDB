#ifndef CHRONOS_SERVICE_SINGLE_NODE_SUBSCRIPTION_RUNTIME_HPP_
#define CHRONOS_SERVICE_SINGLE_NODE_SUBSCRIPTION_RUNTIME_HPP_

#include "chronos/common/result.hpp"
#include "chronos/live/subscription_service.hpp"
#include "chronos/service/single_node_committed_append_router.hpp"
#include "chronos/service/single_node_live_append_fanout.hpp"

#include <cstddef>
#include <memory>

namespace chronos::service {

struct SingleNodeSubscriptionRuntimeConfig {
  SingleNodeCommittedAppendRouter* observer_router{};
  const live::PreparedSubscriptionPlan* plan{};
  live::DurableMultiTabletSubscription* coordinator{};
  std::shared_ptr<const query::QueryCatalogSnapshot> catalog;
  const query::QueryResourceContext* resources{};
  const manifest::ManifestStorage* storage{};
  const manifest::DatabaseStoragePublisher* publisher{};
  const schema::SchemaLineage* lineage{};
  network::SpscNetworkTaskQueue* requests{};
  network::SpscNetworkTaskQueue* responses{};
  std::size_t maximum_active_subscriptions{1024U};
  std::size_t maximum_live_poll_records{64U};
  live::SubscriptionPlanLimits plan_limits{};
  live::SnapshotSubscriptionLimits snapshot_limits{};
  live::CommittedBatchEvaluatorLimits evaluator_limits{};
};

struct SingleNodeSubscriptionRuntimeMetrics {
  SingleNodeLiveAppendFanoutMetrics fanout;
  live::SubscriptionServiceMetrics service;
};

// One thread-affine composition for one durable plan: the stable database observer router, applied
// append fan-out, historical/live Protocol 1.1 lifecycle, and bounded internal request/response
// queues. Every configured owner and queue must outlive this runtime.
class SingleNodeSubscriptionRuntime {
public:
  SingleNodeSubscriptionRuntime() = delete;
  ~SingleNodeSubscriptionRuntime();
  SingleNodeSubscriptionRuntime(const SingleNodeSubscriptionRuntime&) = delete;
  SingleNodeSubscriptionRuntime& operator=(const SingleNodeSubscriptionRuntime&) = delete;
  SingleNodeSubscriptionRuntime(SingleNodeSubscriptionRuntime&&) noexcept;
  SingleNodeSubscriptionRuntime& operator=(SingleNodeSubscriptionRuntime&&) noexcept;

  [[nodiscard]] static common::Result<SingleNodeSubscriptionRuntime>
  create(SingleNodeSubscriptionRuntimeConfig config);

  [[nodiscard]] common::Status poll_once();
  void begin_shutdown() noexcept;
  [[nodiscard]] bool drained() const noexcept;
  [[nodiscard]] SingleNodeSubscriptionRuntimeMetrics metrics() const noexcept;

private:
  class Impl;
  explicit SingleNodeSubscriptionRuntime(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_SINGLE_NODE_SUBSCRIPTION_RUNTIME_HPP_
