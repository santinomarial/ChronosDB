#ifndef CHRONOS_LIVE_SUBSCRIPTION_SERVICE_HPP_
#define CHRONOS_LIVE_SUBSCRIPTION_SERVICE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/live/durable_multi_tablet_subscription.hpp"
#include "chronos/network/spsc_queue.hpp"
#include "chronos/query/catalog.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>

namespace chronos::live {

struct SubscriptionServiceConfig {
  DurableMultiTabletSubscription* owner{};
  const PreparedSubscriptionPlan* plan{};
  std::shared_ptr<const query::QueryCatalogSnapshot> catalog;
  const query::QueryResourceContext* resources{};
  const manifest::ManifestStorage* storage{};
  const manifest::DatabaseStoragePublisher* publisher{};
  const ingest::AsyncRaftTabletApplication* raft_application{};
  const schema::SchemaLineage* lineage{};
  network::SpscNetworkTaskQueue* requests{};
  network::SpscNetworkTaskQueue* responses{};
  std::size_t maximum_active_subscriptions{1024U};
  std::size_t maximum_live_poll_records{64U};
  SubscriptionPlanLimits plan_limits{};
  SnapshotSubscriptionLimits snapshot_limits{};
};

struct SubscriptionServiceMetrics {
  std::uint64_t accepted_new_subscriptions{};
  std::uint64_t resumed_subscriptions{};
  std::uint64_t snapshot_responses{};
  std::uint64_t live_change_responses{};
  std::uint64_t checkpoint_responses{};
  std::uint64_t terminal_responses{};
  std::uint64_t request_errors{};
  std::uint64_t response_backpressure{};
  std::size_t active_subscriptions{};
};

// One thread-affine reactor worker for one durable plan/coordinator. It consumes only subscription,
// acknowledgement, and cancellation NetworkTasks and produces complete negotiated response
// tasks. At most one response is retained internally when the SPSC response ring is full; no
// snapshot or live cursor advances again until that exact owned response is published.
//
// All configured owners and queues outlive the service. begin_shutdown() stops admission and
// converts every active session into a resumable terminal response. The caller continues calling
// poll_once() until drained(), then joins the producer before destroying the reactor queues.
class SubscriptionService {
public:
  SubscriptionService() = delete;
  ~SubscriptionService();
  SubscriptionService(const SubscriptionService&) = delete;
  SubscriptionService& operator=(const SubscriptionService&) = delete;
  SubscriptionService(SubscriptionService&&) noexcept;
  SubscriptionService& operator=(SubscriptionService&&) noexcept;

  [[nodiscard]] static common::Result<SubscriptionService> create(SubscriptionServiceConfig config);

  // Performs finite nonblocking work: retry one retained response, consume at most one request,
  // and advance at most one active subscription by one output.
  [[nodiscard]] common::Status poll_once();
  void begin_shutdown() noexcept;
  [[nodiscard]] bool drained() const noexcept;
  [[nodiscard]] bool accepting() const noexcept;
  [[nodiscard]] bool owns(std::uint64_t connection_id, std::uint64_t request_id) const noexcept;
  [[nodiscard]] SubscriptionServiceMetrics metrics() const noexcept;

private:
  class Impl;
  explicit SubscriptionService(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::live

#endif // CHRONOS_LIVE_SUBSCRIPTION_SERVICE_HPP_
