#include "chronos/query/resource_context.hpp"

#include <type_traits>

static_assert(std::is_default_constructible_v<chronos::query::QueryMemoryReservation>);
static_assert(!std::is_copy_constructible_v<chronos::query::QueryMemoryReservation>);
static_assert(std::is_nothrow_move_constructible_v<chronos::query::QueryMemoryReservation>);
static_assert(std::is_default_constructible_v<chronos::query::QuerySharedMemoryReservation>);
static_assert(std::is_nothrow_copy_constructible_v<chronos::query::QuerySharedMemoryReservation>);
static_assert(!std::is_default_constructible_v<chronos::query::QueryResourceContext>);
static_assert(std::is_nothrow_copy_constructible_v<chronos::query::QueryResourceContext>);
static_assert(std::is_nothrow_move_constructible_v<chronos::query::QueryResourceContext>);
