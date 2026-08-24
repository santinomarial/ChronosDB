#include "chronos/cluster/distributed_vector_physical_rows_finalization_v2.hpp"

#include <type_traits>

static_assert(
    std::is_move_constructible_v<chronos::cluster::DistributedVectorRowsFinalizedResultV2>);
