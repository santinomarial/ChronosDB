#include "chronos/network/native_query_tcp_execution.hpp"

#include <type_traits>

static_assert(!std::is_default_constructible_v<chronos::network::NativeQueryTcpExecution>);
static_assert(!std::is_copy_constructible_v<chronos::network::NativeQueryTcpExecution>);
static_assert(std::is_move_constructible_v<chronos::network::NativeQueryTcpExecution>);
