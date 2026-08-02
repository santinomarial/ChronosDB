#include "chronos/io/posix_io.hpp"

#include <type_traits>

static_assert(!std::is_copy_constructible_v<chronos::io::PosixFile>);
static_assert(std::is_nothrow_move_constructible_v<chronos::io::PosixFile>);
static_assert(!std::is_copy_constructible_v<chronos::io::PosixDirectory>);
static_assert(std::is_nothrow_move_constructible_v<chronos::io::PosixDirectory>);
static_assert(!std::is_copy_constructible_v<chronos::io::PosixAdvisoryLock>);
static_assert(std::is_nothrow_move_constructible_v<chronos::io::PosixAdvisoryLock>);
