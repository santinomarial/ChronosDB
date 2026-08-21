#ifndef CHRONOS_RAFT_DURABLE_BATCH_ADMISSION_HPP_
#define CHRONOS_RAFT_DURABLE_BATCH_ADMISSION_HPP_

#include "chronos/common/status.hpp"
#include "chronos/raft/durable_runtime.hpp"

#include <cstddef>
#include <span>
#include <type_traits>
#include <variant>

namespace chronos::raft::detail {

[[nodiscard]] inline std::size_t
maximum_outbound_for_operation(const DurableRaftOperation& operation,
                               const std::size_t maximum_voters) {
  const std::size_t maximum_core_outbound = maximum_voters > 1U ? maximum_voters - 1U : 1U;
  return std::visit(
      [maximum_core_outbound](const auto& value) -> std::size_t {
        using T = std::remove_cvref_t<decltype(value)>;
        if constexpr (std::is_same_v<T, ObserveGroupOperation> ||
                      std::is_same_v<T, CompactSnapshotOperation> ||
                      std::is_same_v<T, MarkAppliedOperation>) {
          return 0U;
        } else if constexpr (std::is_same_v<T, CompleteSnapshotInstallOperation>) {
          return 1U;
        } else {
          return maximum_core_outbound;
        }
      },
      operation);
}

[[nodiscard]] inline common::Status
admit_durable_batch_outbound(const std::span<const DurableRaftRequest> requests,
                             const DurableMultiRaftLimits& limits) {
  std::size_t reserved = 0U;
  for (const DurableRaftRequest& request : requests) {
    if (request.operation.valueless_by_exception()) {
      return {common::StatusCode::kInvalidArgument,
              "durable Multi-Raft batch contains a valueless operation"};
    }
    const std::size_t operation_bound =
        maximum_outbound_for_operation(request.operation, limits.runtime.raft.maximum_voters);
    if (operation_bound > limits.maximum_batch_outbound - reserved) {
      return {common::StatusCode::kResourceExhausted,
              "durable Multi-Raft batch exceeds its outbound-message bound"};
    }
    reserved += operation_bound;
  }
  return common::Status::ok();
}

} // namespace chronos::raft::detail

#endif // CHRONOS_RAFT_DURABLE_BATCH_ADMISSION_HPP_
