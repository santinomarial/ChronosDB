#include "chronos/manifest/compaction_planner.hpp"

#include <type_traits>

static_assert(std::is_move_constructible_v<chronos::manifest::PlannedAppendOnlyCompaction>);
