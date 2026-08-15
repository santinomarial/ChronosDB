#include "chronos/live/subscription_service.hpp"

#include <type_traits>

static_assert(std::is_trivially_copyable_v<chronos::live::SubscriptionServiceMetrics>);
