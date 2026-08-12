#include "chronos/live/committed_batch_evaluator.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::live::CommittedBatchEvaluatorLimits>);
