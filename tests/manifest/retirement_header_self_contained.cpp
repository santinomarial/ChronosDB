#include "chronos/manifest/retirement.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::manifest::RetiredPartSet>);
static_assert(std::is_nothrow_move_constructible_v<chronos::manifest::RetiredPartSet>);
static_assert(std::is_copy_constructible_v<chronos::manifest::DatabaseStorageRetentionToken>);
static_assert(!std::is_copy_constructible_v<chronos::manifest::TemporalRetiredPartSet>);
static_assert(std::is_nothrow_move_constructible_v<chronos::manifest::TemporalRetiredPartSet>);
