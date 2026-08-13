#include "chronos/query/distributed_vector_exchange.hpp"

#include <type_traits>

static_assert(
    !std::is_copy_constructible_v<chronos::query::EncodedDistributedVectorExchangeMessage>);
static_assert(!std::is_move_constructible_v<chronos::query::DistributedVectorExchangeReader>);
static_assert(std::is_move_constructible_v<chronos::query::DistributedVectorExchangeWriteCursor>);
