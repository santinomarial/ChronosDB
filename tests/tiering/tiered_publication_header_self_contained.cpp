#include "chronos/tiering/tiered_publication.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::tiering::TieredDatabaseStoragePublisher>);
static_assert(std::is_copy_constructible_v<chronos::tiering::TieredDatabaseStorageSnapshot>);
