#include "chronos/live/multi_tablet_subscription_checkpoint.hpp"

namespace {
[[maybe_unused]] auto* const kEncode =
    &chronos::live::encode_multi_tablet_subscription_checkpoint_v1;
static_assert(chronos::live::kMaximumMultiTabletSubscriptionCheckpointSize ==
              (std::size_t{1U} << 30U));
static_assert(chronos::live::MultiTabletSubscriptionCheckpointCodecLimits{}
                  .maximum_checkpoint_bytes == (std::size_t{128U} << 20U));
static_assert(chronos::live::MultiTabletSubscriptionCheckpointCodecLimits{}
                  .maximum_result_key_bytes == (std::size_t{1U} << 20U));
static_assert(chronos::live::MultiTabletSubscriptionCheckpointCodecLimits{}.maximum_payload_bytes ==
              (std::size_t{16U} << 20U));
} // namespace
