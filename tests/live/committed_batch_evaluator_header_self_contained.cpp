#include "chronos/live/committed_batch_evaluator.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::live::CommittedBatchEvaluatorLimits>);
static_assert(chronos::live::CommittedBatchEvaluatorLimits{}.maximum_workspace_bytes ==
              (std::size_t{128U} << 20U));
