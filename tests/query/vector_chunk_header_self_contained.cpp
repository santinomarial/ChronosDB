#include "chronos/query/vector_chunk.hpp"

#include <type_traits>

static_assert(!std::is_default_constructible_v<chronos::query::VectorSelection>);
static_assert(!std::is_copy_constructible_v<chronos::query::VectorSelection>);
static_assert(std::is_nothrow_move_constructible_v<chronos::query::VectorSelection>);
static_assert(!std::is_default_constructible_v<chronos::query::VectorChunk>);
static_assert(!std::is_copy_constructible_v<chronos::query::VectorChunk>);
static_assert(std::is_nothrow_move_constructible_v<chronos::query::VectorChunk>);
static_assert(std::has_virtual_destructor_v<chronos::query::VectorChunkBacking>);
