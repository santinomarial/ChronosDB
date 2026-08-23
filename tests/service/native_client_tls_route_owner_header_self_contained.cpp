#include "chronos/service/native_client_tls_route_owner.hpp"

#include <type_traits>

static_assert(!std::is_default_constructible_v<chronos::service::NativeClientTlsRouteOwner>);
static_assert(!std::is_copy_constructible_v<chronos::service::NativeClientTlsRouteOwner>);
static_assert(std::is_nothrow_move_constructible_v<chronos::service::NativeClientTlsRouteOwner>);
