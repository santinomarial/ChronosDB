#include "chronos/live/incremental_aggregate.hpp"

#include <type_traits>

namespace {
[[maybe_unused]] constexpr auto kHeaderIsSelfContained = &chronos::live::AggregateSnapshot::count;
using RestoreSignature = chronos::common::Result<chronos::live::IncrementalAggregateSet> (*)(
    const chronos::live::IncrementalAggregateCheckpoint&);
static_assert(
    std::is_same_v<decltype(&chronos::live::IncrementalAggregateSet::restore), RestoreSignature>);
} // namespace
