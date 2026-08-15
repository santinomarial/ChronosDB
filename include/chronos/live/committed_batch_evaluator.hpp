#ifndef CHRONOS_LIVE_COMMITTED_BATCH_EVALUATOR_HPP_
#define CHRONOS_LIVE_COMMITTED_BATCH_EVALUATOR_HPP_

#include "chronos/columnar/columnar_batch.hpp"
#include "chronos/common/result.hpp"
#include "chronos/live/subscription.hpp"
#include "chronos/live/subscription_plan.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/network/subscription_messages.hpp"
#include "chronos/query/columnar_batch_scan.hpp"
#include "chronos/query/resource_context.hpp"

#include <cstddef>
#include <memory>

namespace chronos::live {

inline constexpr std::size_t kCommittedBatchResultKeySize = 80U;

struct CommittedBatchEvaluatorLimits {
  query::ColumnarBatchScanLimits scan{};
  network::QueryResultLimits result{};
  network::SubscriptionMessageLimits subscription{};
  std::size_t maximum_output_chunks{4096U};
  std::size_t maximum_workspace_bytes{std::size_t{128U} * 1024U * 1024U};
};

// Checks whether the prepared pipeline is safe to evaluate independently per committed append.
[[nodiscard]] common::Status validate_committed_batch_plan(const PreparedSubscriptionPlan& plan);

// Evaluates one already-applied immutable append under one stateless row-preserving subscription
// plan. The returned UPSERT advances exactly one committed source position. Its result key binds
// the plan fingerprint and complete source position; its payload is one self-describing native
// QUERY_RESULT batch containing every selected row from the append, including zero rows.
//
// Aggregate, sort, latest, and limit stages are rejected because evaluating those independently
// per append would not preserve the plan's historical semantics. The caller publishes the result
// only after this function succeeds; this function does not mutate subscription state.
[[nodiscard]] common::Result<CommittedChange>
evaluate_committed_batch(const PreparedSubscriptionPlan& plan, SourcePosition position,
                         std::shared_ptr<const columnar::OwnedColumnarBatch> batch,
                         const query::QueryResourceContext& resources,
                         CommittedBatchEvaluatorLimits limits = {});

} // namespace chronos::live

#endif // CHRONOS_LIVE_COMMITTED_BATCH_EVALUATOR_HPP_
