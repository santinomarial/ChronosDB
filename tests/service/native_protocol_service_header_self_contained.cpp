#include "chronos/service/native_protocol_service.hpp"

#include <type_traits>

static_assert(std::is_constructible_v<chronos::service::NativeProtocolService,
                                      chronos::service::SingleNodeDatabase&>);
static_assert(std::is_constructible_v<chronos::service::NativeProtocolService,
                                      chronos::service::SingleNodeDatabase&,
                                      chronos::service::NativeIdentityGenerator&>);
