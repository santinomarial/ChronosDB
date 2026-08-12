#ifndef CHRONOS_SERVICE_SINGLE_NODE_COMMITTED_APPEND_ROUTER_HPP_
#define CHRONOS_SERVICE_SINGLE_NODE_COMMITTED_APPEND_ROUTER_HPP_

#include "chronos/common/status.hpp"
#include "chronos/service/single_node_database.hpp"

#include <cstdint>

namespace chronos::service {

struct SingleNodeCommittedAppendRouterMetrics {
  std::uint64_t forwarded_appends{};
  std::uint64_t unbound_appends{};
  bool bound{};
};

// Stable pre-database observer address whose one borrowed delegate may be attached after database
// recovery and before request admission. Bind, unbind, observation, and metrics are thread-affine.
class SingleNodeCommittedAppendRouter final : public SingleNodeCommittedAppendObserver {
public:
  [[nodiscard]] common::Status bind(SingleNodeCommittedAppendObserver& delegate);
  void unbind(const SingleNodeCommittedAppendObserver& delegate) noexcept;
  void on_applied(AppliedSingleNodeColumnarAppend append) noexcept override;
  [[nodiscard]] SingleNodeCommittedAppendRouterMetrics metrics() const noexcept;

private:
  SingleNodeCommittedAppendObserver* delegate_{};
  SingleNodeCommittedAppendRouterMetrics metrics_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_SINGLE_NODE_COMMITTED_APPEND_ROUTER_HPP_
