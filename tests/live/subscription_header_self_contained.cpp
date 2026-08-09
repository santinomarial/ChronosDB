#include "chronos/live/subscription.hpp"

namespace {
[[maybe_unused]] constexpr auto kHeaderIsSelfContained =
    chronos::live::LogicalChangeOperation::kUpsert;
}
