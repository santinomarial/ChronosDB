#include "chronos/head/mutable_head.hpp"

#include <type_traits>

namespace {
static_assert(!std::is_default_constructible_v<chronos::head::MutableHead>);
static_assert(!std::is_copy_constructible_v<chronos::head::MutableHead>);
static_assert(std::is_move_constructible_v<chronos::head::MutableHead>);
static_assert(!std::is_copy_constructible_v<chronos::head::PreparedHeadAppend>);
static_assert(std::is_move_constructible_v<chronos::head::PreparedHeadAppend>);
static_assert(std::is_copy_constructible_v<chronos::head::HeadSnapshot>);
} // namespace
