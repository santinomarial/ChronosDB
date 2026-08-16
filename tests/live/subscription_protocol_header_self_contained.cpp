#include "chronos/live/subscription_protocol.hpp"

namespace {
using Encode = chronos::common::Result<std::vector<std::byte>> (*)(
    const chronos::live::DeliveryRecord&, const chronos::network::SubscriptionMessageLimits&);
[[maybe_unused]] Encode const kEncode = &chronos::live::encode_subscription_delivery;
} // namespace
