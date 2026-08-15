#include "chronos/service/single_node_live_append_fanout.hpp"

#include <type_traits>

static_assert(std::is_trivially_copyable_v<chronos::service::SingleNodeLiveAppendFanoutMetrics>);
