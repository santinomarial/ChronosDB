#include "chronos/query/distributed_vector_grouped_aggregate_exchange.hpp"

#include <type_traits>

namespace {
[[maybe_unused]] constexpr std::size_t kHeaderLength =
    chronos::query::distributed_vector_grouped_aggregate_exchange_format::kHeaderLength;
static_assert(std::is_aggregate_v<chronos::query::DistributedVectorGroupedAggregateAuthority>);
} // namespace
