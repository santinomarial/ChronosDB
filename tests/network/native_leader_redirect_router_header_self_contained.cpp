#include "chronos/network/native_leader_redirect_router.hpp"

#include <type_traits>

static_assert(!std::is_default_constructible_v<chronos::network::NativeLeaderRedirectRouter>);
static_assert(!std::is_copy_constructible_v<chronos::network::NativeLeaderRedirectRouter>);
static_assert(std::is_move_constructible_v<chronos::network::NativeLeaderRedirectRouter>);
