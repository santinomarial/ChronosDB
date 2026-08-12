#ifndef CHRONOS_SERVICE_SINGLE_NODE_LIVE_APPEND_FANOUT_HPP_
#define CHRONOS_SERVICE_SINGLE_NODE_LIVE_APPEND_FANOUT_HPP_

#include "chronos/common/result.hpp"
#include "chronos/live/committed_batch_evaluator.hpp"
#include "chronos/live/durable_multi_tablet_subscription.hpp"
#include "chronos/service/single_node_database.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace chronos::service {

struct SingleNodeLiveAppendBinding {
  const live::PreparedSubscriptionPlan* plan{};
  live::DurableMultiTabletSubscription* coordinator{};
  const query::QueryResourceContext* resources{};
  live::CommittedBatchEvaluatorLimits evaluator{};
};

struct SingleNodeLiveAppendFanoutMetrics {
  std::uint64_t observed_appends{};
  std::uint64_t evaluated_plans{};
  std::uint64_t published_changes{};
  std::uint64_t schema_invalidations{};
  std::uint64_t evaluation_failures{};
  std::uint64_t publication_failures{};
  std::uint64_t continuity_losses{};
  std::uint64_t containment_failures{};
  std::size_t configured_plans{};
  std::size_t disabled_plans{};
};

class SingleNodeLiveAppendFanout final : public SingleNodeCommittedAppendObserver {
public:
  [[nodiscard]] static common::Result<std::unique_ptr<SingleNodeLiveAppendFanout>>
  create(std::vector<SingleNodeLiveAppendBinding> bindings, std::size_t maximum_bindings = 1024U);

  void on_applied(AppliedSingleNodeColumnarAppend append) noexcept override;
  [[nodiscard]] SingleNodeLiveAppendFanoutMetrics metrics() const noexcept;

private:
  struct Entry {
    SingleNodeLiveAppendBinding binding;
    bool enabled{true};
  };
  explicit SingleNodeLiveAppendFanout(std::vector<Entry> entries) noexcept;
  std::vector<Entry> entries_;
  SingleNodeLiveAppendFanoutMetrics metrics_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_SINGLE_NODE_LIVE_APPEND_FANOUT_HPP_
