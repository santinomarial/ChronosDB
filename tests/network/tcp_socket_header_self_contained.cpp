#include "chronos/network/tcp_socket.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::network::TcpSocket>);
static_assert(std::is_nothrow_move_constructible_v<chronos::network::TcpSocket>);
static_assert(!std::is_copy_constructible_v<chronos::network::TcpListener>);
static_assert(std::is_nothrow_move_constructible_v<chronos::network::TcpListener>);
