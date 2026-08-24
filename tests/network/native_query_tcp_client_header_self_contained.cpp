#include "chronos/network/native_query_tcp_client.hpp"

#include <type_traits>

static_assert(!std::is_default_constructible_v<chronos::network::NativeQueryTcpClient>);
static_assert(!std::is_copy_constructible_v<chronos::network::NativeQueryTcpClient>);
static_assert(std::is_move_constructible_v<chronos::network::NativeQueryTcpClient>);
