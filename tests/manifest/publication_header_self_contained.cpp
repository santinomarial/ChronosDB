#include "chronos/manifest/publication.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::manifest::DatabaseStoragePublisher>);
static_assert(std::is_nothrow_move_constructible_v<chronos::manifest::DatabaseStoragePublisher>);
static_assert(std::is_copy_constructible_v<chronos::manifest::DatabaseStorageSnapshot>);
