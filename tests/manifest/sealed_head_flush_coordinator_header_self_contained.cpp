#include "chronos/manifest/sealed_head_flush_coordinator.hpp"

#include <type_traits>

static_assert(!std::is_default_constructible_v<chronos::manifest::SealedHeadFlushCoordinator>);
