#include "chronos/service/single_node_committed_append_router.hpp"

#include <type_traits>

static_assert(
    std::is_trivially_copyable_v<chronos::service::SingleNodeCommittedAppendRouterMetrics>);
