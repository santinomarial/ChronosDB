#ifndef CHRONOS_LIVE_SUBSCRIPTION_PROTOCOL_HPP_
#define CHRONOS_LIVE_SUBSCRIPTION_PROTOCOL_HPP_

#include "chronos/common/result.hpp"
#include "chronos/live/subscription.hpp"
#include "chronos/network/subscription_messages.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace chronos::live {

// These helpers form the ownership boundary between shard-affine subscription state and Protocol
// 1.1. They return complete canonical payloads; framing and socket backpressure remain owned by the
// network reactor. Delivery stays at least once until an acknowledgement is accepted.
[[nodiscard]] common::Result<std::vector<std::byte>>
encode_subscription_registration(const SubscriptionRegistration& registration,
                                 const network::SubscriptionMessageLimits& limits = {});

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_subscription_delivery(const DeliveryRecord& delivery,
                             const network::SubscriptionMessageLimits& limits = {});

[[nodiscard]] common::Result<std::vector<std::byte>>
acknowledge_subscription_delivery(SubscriptionManager& manager, const common::Uuid& subscription_id,
                                  std::uint64_t delivery_sequence,
                                  const network::SubscriptionMessageLimits& limits = {});

[[nodiscard]] common::Result<std::vector<std::byte>>
terminate_subscription(SubscriptionManager& manager, const common::Uuid& subscription_id,
                       network::SubscriptionEndReason reason,
                       const network::SubscriptionMessageLimits& limits = {});

} // namespace chronos::live

#endif // CHRONOS_LIVE_SUBSCRIPTION_PROTOCOL_HPP_
