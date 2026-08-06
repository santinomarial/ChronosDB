#include "chronos/columnar/column_vector.hpp"

#include <type_traits>

static_assert(!std::is_default_constructible_v<chronos::columnar::OwnedPhysicalColumn>);
static_assert(!std::is_copy_constructible_v<chronos::columnar::OwnedPhysicalColumn>);
static_assert(std::is_nothrow_move_constructible_v<chronos::columnar::OwnedPhysicalColumn>);

namespace {
static_assert(chronos::columnar::bitmap_size(9U) == 2U);
}
