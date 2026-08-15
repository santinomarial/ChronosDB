#include "chronos/live/subscription.hpp"

namespace {
[[maybe_unused]] constexpr auto kHeaderIsSelfContained =
    chronos::live::LogicalChangeOperation::kUpsert;
static_assert(chronos::live::SubscriptionLimits{}.maximum_retained_bytes ==
              (std::size_t{64U} << 20U));
static_assert(chronos::live::SubscriptionLimits{}.maximum_buffered_bytes_per_subscription ==
              (std::size_t{8U} << 20U));
static_assert(chronos::live::SubscriptionLimits{}.maximum_change_bytes ==
              (std::size_t{16U} << 20U));
} // namespace
