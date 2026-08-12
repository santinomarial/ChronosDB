#include "chronos/service/single_node_committed_append_router.hpp"

#include <memory>
#include <utility>

namespace chronos::service {

common::Status SingleNodeCommittedAppendRouter::bind(SingleNodeCommittedAppendObserver& delegate) {
  if (delegate_ != nullptr)
    return {common::StatusCode::kAlreadyExists, "committed append router is already bound"};
  if (std::addressof(delegate) == this)
    return {common::StatusCode::kInvalidArgument, "committed append router cannot bind itself"};
  delegate_ = std::addressof(delegate);
  metrics_.bound = true;
  return common::Status::ok();
}

void SingleNodeCommittedAppendRouter::unbind(
    const SingleNodeCommittedAppendObserver& delegate) noexcept {
  if (delegate_ == std::addressof(delegate)) {
    delegate_ = nullptr;
    metrics_.bound = false;
  }
}

void SingleNodeCommittedAppendRouter::on_applied(AppliedSingleNodeColumnarAppend append) noexcept {
  if (delegate_ == nullptr) {
    ++metrics_.unbound_appends;
    return;
  }
  ++metrics_.forwarded_appends;
  delegate_->on_applied(std::move(append));
}

SingleNodeCommittedAppendRouterMetrics SingleNodeCommittedAppendRouter::metrics() const noexcept {
  return metrics_;
}

} // namespace chronos::service
