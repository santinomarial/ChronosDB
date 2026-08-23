#include "chronos/service/native_client_route_authority.hpp"

#include <type_traits>

static_assert(!std::is_default_constructible_v<chronos::service::NativeClientRouteAuthority>);
static_assert(!std::is_copy_constructible_v<chronos::service::NativeClientRouteAuthority>);
static_assert(std::is_nothrow_move_constructible_v<chronos::service::NativeClientRouteAuthority>);
