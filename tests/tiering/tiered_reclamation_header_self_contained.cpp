#include "chronos/tiering/tiered_reclamation.hpp"

#include <type_traits>

static_assert(std::is_aggregate_v<chronos::tiering::TieredLocalPartReclamationLimits>);
static_assert(std::is_aggregate_v<chronos::tiering::TieredLocalPartReclamationReport>);
