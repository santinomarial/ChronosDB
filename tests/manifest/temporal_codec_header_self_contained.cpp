#include "chronos/manifest/temporal_codec.hpp"

#include <type_traits>

static_assert(!std::is_default_constructible_v<chronos::manifest::EncodedTemporalManifest>);
