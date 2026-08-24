#include "chronos/network/native_query_retry.hpp"

#include <type_traits>

static_assert(!std::is_default_constructible_v<chronos::network::NativeQueryRetry>);
static_assert(!std::is_copy_constructible_v<chronos::network::NativeQueryRetry>);
static_assert(std::is_move_constructible_v<chronos::network::NativeQueryRetry>);
