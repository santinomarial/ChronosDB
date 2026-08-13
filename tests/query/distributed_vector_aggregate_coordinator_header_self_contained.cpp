#include "chronos/query/distributed_vector_aggregate_coordinator.hpp"

#include <type_traits>

static_assert(
    std::is_move_constructible_v<chronos::query::DistributedVectorAggregateCoordinatorV2>);
static_assert(
    !std::is_copy_constructible_v<chronos::query::DistributedVectorAggregateCoordinatorV2>);
static_assert(std::is_aggregate_v<chronos::query::DistributedVectorAggregateCoordinatorLimitsV2>);

namespace {
using Create = chronos::common::Result<chronos::query::DistributedVectorAggregateCoordinatorV2> (*)(
    chronos::common::Uuid, std::vector<chronos::schema::TabletId>,
    std::vector<chronos::query::VectorAggregateDefinition>,
    chronos::query::DistributedVectorResultSchema,
    chronos::query::DistributedVectorAggregateCoordinatorLimitsV2);
[[maybe_unused]] const Create kCreate =
    &chronos::query::DistributedVectorAggregateCoordinatorV2::create;
} // namespace
