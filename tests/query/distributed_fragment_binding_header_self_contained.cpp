#include "chronos/query/distributed_fragment_binding.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::query::DistributedAggregateFragmentBinding>);
static_assert(std::is_aggregate_v<chronos::query::DistributedGroupedFloat64FragmentBinding>);

namespace {
[[maybe_unused]] const auto kBind = &chronos::query::bind_distributed_aggregate_fragment;
[[maybe_unused]] const auto kBindGrouped =
    &chronos::query::bind_distributed_grouped_float64_fragment;
} // namespace
