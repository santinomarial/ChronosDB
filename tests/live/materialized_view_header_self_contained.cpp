#include "chronos/live/materialized_view.hpp"

#include <type_traits>

namespace {
[[maybe_unused]] constexpr auto kHeaderIsSelfContained = &chronos::live::WindowKey::start;
using RestoreSignature = chronos::common::Result<chronos::live::WindowedMaterializedView> (*)(
    const chronos::live::WindowedMaterializedViewCheckpoint&);
static_assert(
    std::is_same_v<decltype(&chronos::live::WindowedMaterializedView::restore), RestoreSignature>);
} // namespace
