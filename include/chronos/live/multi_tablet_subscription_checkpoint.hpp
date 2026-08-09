#ifndef CHRONOS_LIVE_MULTI_TABLET_SUBSCRIPTION_CHECKPOINT_HPP_
#define CHRONOS_LIVE_MULTI_TABLET_SUBSCRIPTION_CHECKPOINT_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/live/multi_tablet_subscription.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace chronos::live {

inline constexpr std::size_t kMultiTabletSubscriptionCheckpointHeaderSize = 128U;
inline constexpr std::size_t kMultiTabletSubscriptionCheckpointSourceSize = 48U;
inline constexpr std::size_t kMultiTabletSubscriptionCheckpointChangeEnvelopeSize = 80U;
inline constexpr std::size_t kMultiTabletSubscriptionCheckpointTrailerSize = 4U;
inline constexpr std::size_t kBoundMultiTabletSubscriptionCheckpointHeaderSize = 64U;
inline constexpr std::size_t kBoundMultiTabletSubscriptionCheckpointTrailerSize = 4U;
inline constexpr std::size_t kMaximumMultiTabletSubscriptionCheckpointSize = 1024U * 1024U * 1024U;

struct MultiTabletSubscriptionCheckpointCodecLimits {
  std::size_t maximum_checkpoint_bytes{128U * 1024U * 1024U};
  std::size_t maximum_sources{kMaximumResumeTokenSources};
  std::size_t maximum_retained_changes{65'536U};
  std::size_t maximum_result_key_bytes{1024U * 1024U};
  std::size_t maximum_payload_bytes{16U * 1024U * 1024U};
};

struct BoundMultiTabletSubscriptionCheckpoint {
  std::uint64_t checkpoint_generation{};
  MultiTabletSubscriptionCheckpoint state;

  friend bool operator==(const BoundMultiTabletSubscriptionCheckpoint&,
                         const BoundMultiTabletSubscriptionCheckpoint&) = default;
};

[[nodiscard]] common::Result<std::vector<std::byte>> encode_multi_tablet_subscription_checkpoint_v1(
    const MultiTabletSubscriptionCheckpoint& checkpoint,
    MultiTabletSubscriptionCheckpointCodecLimits limits = {});

[[nodiscard]] common::Result<MultiTabletSubscriptionCheckpoint>
decode_multi_tablet_subscription_checkpoint_v1(
    common::ByteView bytes, MultiTabletSubscriptionCheckpointCodecLimits limits = {});

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_bound_multi_tablet_subscription_checkpoint_v1(
    const BoundMultiTabletSubscriptionCheckpoint& checkpoint,
    MultiTabletSubscriptionCheckpointCodecLimits limits = {});

[[nodiscard]] common::Result<BoundMultiTabletSubscriptionCheckpoint>
decode_bound_multi_tablet_subscription_checkpoint_v1(
    common::ByteView bytes, MultiTabletSubscriptionCheckpointCodecLimits limits = {});

} // namespace chronos::live

#endif // CHRONOS_LIVE_MULTI_TABLET_SUBSCRIPTION_CHECKPOINT_HPP_
