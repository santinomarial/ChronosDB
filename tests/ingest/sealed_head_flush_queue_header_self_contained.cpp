#include "chronos/ingest/sealed_head_flush_queue.hpp"

#include <type_traits>

static_assert(!std::is_default_constructible_v<chronos::ingest::SealedHeadFlushQueue>);
static_assert(!std::is_copy_constructible_v<chronos::ingest::SealedHeadFlushQueue>);
static_assert(!std::is_default_constructible_v<chronos::ingest::SealedHeadFlushWork>);
static_assert(!std::is_copy_constructible_v<chronos::ingest::SealedHeadFlushWork>);
static_assert(std::is_nothrow_move_constructible_v<chronos::ingest::SealedHeadFlushWork>);
