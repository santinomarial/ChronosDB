#include "chronos/service/replicated_read_barrier.hpp"

#include <type_traits>

static_assert(!std::is_default_constructible_v<chronos::service::ReplicatedReadBarrier>);
static_assert(std::is_move_constructible_v<chronos::service::ReplicatedReadBarrier>);
