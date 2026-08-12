#include "chronos/service/single_node_subscription_runtime.hpp"

#include <memory>
#include <new>
#include <utility>

namespace chronos::service {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

} // namespace

class SingleNodeSubscriptionRuntime::Impl {
public:
  Impl(SingleNodeCommittedAppendRouter& configured_router,
       std::unique_ptr<SingleNodeLiveAppendFanout> configured_fanout,
       live::SubscriptionService configured_service) noexcept
      : router(configured_router), fanout(std::move(configured_fanout)),
        service(std::move(configured_service)) {}

  ~Impl() {
    router.unbind(*fanout);
  }

  SingleNodeCommittedAppendRouter& router;
  std::unique_ptr<SingleNodeLiveAppendFanout> fanout;
  live::SubscriptionService service;
};

SingleNodeSubscriptionRuntime::SingleNodeSubscriptionRuntime(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

SingleNodeSubscriptionRuntime::~SingleNodeSubscriptionRuntime() = default;
SingleNodeSubscriptionRuntime::SingleNodeSubscriptionRuntime(
    SingleNodeSubscriptionRuntime&&) noexcept = default;
SingleNodeSubscriptionRuntime&
SingleNodeSubscriptionRuntime::operator=(SingleNodeSubscriptionRuntime&&) noexcept = default;

common::Result<SingleNodeSubscriptionRuntime>
SingleNodeSubscriptionRuntime::create(SingleNodeSubscriptionRuntimeConfig config) {
  if (config.observer_router == nullptr || config.plan == nullptr ||
      config.coordinator == nullptr || !config.catalog || config.resources == nullptr ||
      config.storage == nullptr || config.publisher == nullptr || config.lineage == nullptr ||
      config.requests == nullptr || config.responses == nullptr)
    return common::make_unexpected(invalid("single-node subscription runtime owner is null"));
  auto fanout = SingleNodeLiveAppendFanout::create(
      {{config.plan, config.coordinator, config.resources, config.evaluator_limits}}, 1U);
  if (!fanout.has_value())
    return common::make_unexpected(fanout.error());
  auto service = live::SubscriptionService::create(
      {.owner = config.coordinator,
       .plan = config.plan,
       .catalog = std::move(config.catalog),
       .resources = config.resources,
       .storage = config.storage,
       .publisher = config.publisher,
       .lineage = config.lineage,
       .requests = config.requests,
       .responses = config.responses,
       .maximum_active_subscriptions = config.maximum_active_subscriptions,
       .maximum_live_poll_records = config.maximum_live_poll_records,
       .plan_limits = config.plan_limits,
       .snapshot_limits = config.snapshot_limits});
  if (!service.has_value())
    return common::make_unexpected(service.error());
  try {
    auto impl =
        std::make_unique<Impl>(*config.observer_router, std::move(*fanout), std::move(*service));
    const common::Status bound = config.observer_router->bind(*impl->fanout);
    if (!bound.is_ok())
      return common::make_unexpected(bound);
    return SingleNodeSubscriptionRuntime{std::move(impl)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("single-node subscription runtime allocation failed"));
  }
}

common::Status SingleNodeSubscriptionRuntime::poll_once() {
  return impl_->service.poll_once();
}

void SingleNodeSubscriptionRuntime::begin_shutdown() noexcept {
  impl_->service.begin_shutdown();
}

bool SingleNodeSubscriptionRuntime::drained() const noexcept {
  return impl_->service.drained();
}

SingleNodeSubscriptionRuntimeMetrics SingleNodeSubscriptionRuntime::metrics() const noexcept {
  return {.fanout = impl_->fanout->metrics(), .service = impl_->service.metrics()};
}

} // namespace chronos::service
