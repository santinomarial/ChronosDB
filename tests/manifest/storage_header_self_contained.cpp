#include "chronos/manifest/storage.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::manifest::ManifestStorage>);
static_assert(std::is_nothrow_move_constructible_v<chronos::manifest::ManifestStorage>);
static_assert(!std::is_copy_constructible_v<chronos::manifest::SnapshotPartImage>);
static_assert(std::is_nothrow_move_constructible_v<chronos::manifest::SnapshotPartImage>);
