#include "chronos/query/distributed_vector_fragment.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::query::DistributedVectorFragmentDispatch>);
static_assert(
    !std::is_copy_constructible_v<chronos::query::EncodedDistributedVectorFragmentDispatch>);
static_assert(!std::is_move_constructible_v<chronos::query::DistributedVectorFragmentReader>);
static_assert(std::is_move_constructible_v<chronos::query::DistributedVectorFragmentWriteCursor>);
