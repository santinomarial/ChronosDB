#include "chronos/manifest/sealed_head_flush.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::manifest::EncodedSealedHeadPart>);
static_assert(std::is_nothrow_move_constructible_v<chronos::manifest::EncodedSealedHeadPart>);
