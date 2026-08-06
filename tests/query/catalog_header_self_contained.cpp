#include "chronos/query/catalog.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::query::QueryCatalogSnapshot>);
static_assert(std::is_nothrow_move_constructible_v<chronos::query::QueryCatalogSnapshot>);
