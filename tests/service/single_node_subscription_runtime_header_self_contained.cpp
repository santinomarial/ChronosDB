#include "chronos/service/single_node_subscription_runtime.hpp"

#include <type_traits>

static_assert(std::is_trivially_copyable_v<chronos::service::SingleNodeSubscriptionRuntimeMetrics>);
