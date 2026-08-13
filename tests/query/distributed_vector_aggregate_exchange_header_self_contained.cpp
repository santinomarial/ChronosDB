#include "chronos/query/distributed_vector_aggregate_exchange.hpp"

#include <type_traits>

static_assert(
    !std::is_copy_constructible_v<chronos::query::DistributedVectorAggregateExchangeMessage>);
static_assert(
    std::is_move_constructible_v<chronos::query::DistributedVectorAggregateExchangeMessage>);
static_assert(
    !std::is_move_constructible_v<chronos::query::DistributedVectorAggregateExchangeReader>);
static_assert(
    std::is_move_constructible_v<chronos::query::DistributedVectorAggregateExchangeWriteCursor>);
