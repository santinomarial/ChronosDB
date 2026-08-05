#include "chronos/ingest/retry_directory.hpp"

#include <type_traits>

namespace {
static_assert(!std::is_default_constructible_v<chronos::ingest::RetryDirectory>);
static_assert(!std::is_copy_constructible_v<chronos::ingest::RetryDirectory>);
static_assert(std::is_move_constructible_v<chronos::ingest::RetryDirectory>);
static_assert(!std::is_copy_constructible_v<chronos::ingest::RetryReservation>);
static_assert(std::is_move_constructible_v<chronos::ingest::RetryReservation>);
} // namespace
