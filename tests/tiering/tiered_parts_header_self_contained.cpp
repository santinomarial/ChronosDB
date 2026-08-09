#include "chronos/tiering/tiered_parts.hpp"

namespace {
[[maybe_unused]] constexpr auto kHeaderIsSelfContained =
    &chronos::tiering::TieringLimits::maximum_parts;
}
