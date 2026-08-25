#include "chronos/query/distributed_vector_pre_group_program.hpp"

#include <type_traits>

static_assert(
    std::is_move_constructible_v<chronos::query::EncodedDistributedVectorPreGroupProgram>);
static_assert(std::is_copy_constructible_v<chronos::query::DistributedVectorPreGroupProgram>);
