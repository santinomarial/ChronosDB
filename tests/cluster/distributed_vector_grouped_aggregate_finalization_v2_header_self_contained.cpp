#include "chronos/cluster/distributed_vector_grouped_aggregate_finalization_v2.hpp"

#include <type_traits>

static_assert(std::is_move_constructible_v<
              chronos::cluster::DistributedVectorGroupedAggregateFinalizationLimitsV2>);

namespace {
[[maybe_unused]] const auto kFinalize =
    &chronos::cluster::finalize_distributed_vector_grouped_aggregate_v2;
[[maybe_unused]] const auto kFinalizeWithProjection =
    &chronos::cluster::finalize_distributed_vector_grouped_aggregate_with_projection_v2;
} // namespace
