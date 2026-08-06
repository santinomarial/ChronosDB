#include "chronos/ingest/tablet_state.hpp"

#include <type_traits>

namespace {

static_assert(!std::is_default_constructible_v<chronos::ingest::TabletState>);
static_assert(!std::is_copy_constructible_v<chronos::ingest::TabletState>);
static_assert(std::is_move_constructible_v<chronos::ingest::TabletState>);
static_assert(!std::is_copy_constructible_v<chronos::ingest::PreparedTabletAppend>);
static_assert(std::is_move_constructible_v<chronos::ingest::PreparedTabletAppend>);
static_assert(std::is_copy_constructible_v<chronos::ingest::TabletSnapshot>);

} // namespace
