#include "chronos/query/distributed_vector_aggregate_state.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::query::EncodedMergeableVectorAggregateState>);
static_assert(std::is_move_constructible_v<chronos::query::EncodedMergeableVectorAggregateState>);
static_assert(!std::is_move_constructible_v<chronos::query::MergeableVectorAggregateStateReader>);
static_assert(
    std::is_move_constructible_v<chronos::query::MergeableVectorAggregateStateWriteCursor>);
