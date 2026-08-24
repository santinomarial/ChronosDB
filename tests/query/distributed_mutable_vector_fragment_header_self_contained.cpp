#include "chronos/query/distributed_mutable_vector_fragment.hpp"

#include <type_traits>

static_assert(std::is_move_constructible_v<chronos::query::DistributedMutableVectorFragment>);
static_assert(std::is_aggregate_v<chronos::query::DistributedMutableVectorFragmentBinding>);
