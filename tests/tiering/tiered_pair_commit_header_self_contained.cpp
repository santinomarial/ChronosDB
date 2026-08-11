#include "chronos/tiering/tiered_pair_commit.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::tiering::TieredPairCommitStorage>);
