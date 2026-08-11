#include "chronos/tiering/cold_manifest_storage.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::tiering::ColdLocationManifestStorage>);
