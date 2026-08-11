#include "chronos/query/distributed_fragment_binding.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::query::DistributedAggregateFragmentBinding>);

namespace {
[[maybe_unused]] const auto kBind = &chronos::query::bind_distributed_aggregate_fragment;
}
