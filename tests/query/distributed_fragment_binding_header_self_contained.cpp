#include "chronos/query/distributed_fragment_binding.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::query::DistributedAggregateFragmentBinding>);
static_assert(std::is_aggregate_v<chronos::query::DistributedGroupedFloat64FragmentBinding>);
static_assert(std::is_aggregate_v<chronos::query::MetadataBackedDistributedVectorSnapshotBinding>);
static_assert(std::is_aggregate_v<chronos::query::GroupBackedDistributedVectorSnapshotBinding>);
static_assert(std::is_move_constructible_v<chronos::query::CompatibleDistributedVectorSnapshot>);

namespace {
[[maybe_unused]] const auto kBind = &chronos::query::bind_distributed_aggregate_fragment;
[[maybe_unused]] const auto kBindGrouped =
    &chronos::query::bind_distributed_grouped_float64_fragment;
[[maybe_unused]] const auto kBindGroupedDispatch =
    &chronos::query::bind_distributed_grouped_float64_fragment_dispatch;
[[maybe_unused]] const auto kBindVector = &chronos::query::bind_distributed_vector_fragment;
[[maybe_unused]] const auto kBindCompatibleVector =
    &chronos::query::bind_compatible_distributed_vector_snapshot;
[[maybe_unused]] const auto kBindMetadataVector =
    &chronos::query::bind_metadata_backed_distributed_vector_snapshot;
[[maybe_unused]] const auto kBindGroupVector =
    &chronos::query::bind_group_backed_distributed_vector_snapshot;
} // namespace
