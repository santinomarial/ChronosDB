#include "chronos/ingest/sha256.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::ingest::Sha256Hasher>);
