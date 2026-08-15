#include "chronos/live/subscription_retention.hpp"

#include <type_traits>

static_assert(std::is_move_constructible_v<chronos::live::SubscriptionRetentionReport>);
