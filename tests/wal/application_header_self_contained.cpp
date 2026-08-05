#include "chronos/wal/application.hpp"

#include <type_traits>

namespace {
static_assert(!std::is_default_constructible_v<chronos::wal::EncodedApplicationPayload>);
static_assert(!std::is_copy_constructible_v<chronos::wal::EncodedApplicationPayload>);
static_assert(std::is_move_constructible_v<chronos::wal::EncodedApplicationPayload>);
} // namespace
