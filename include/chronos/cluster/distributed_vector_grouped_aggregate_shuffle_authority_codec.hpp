#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_AUTHORITY_CODEC_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_AUTHORITY_CODEC_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_authority.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace chronos::cluster {

namespace distributed_vector_grouped_aggregate_shuffle_authority_format {
inline constexpr std::uint16_t kMajor = 1U;
inline constexpr std::uint16_t kMinor = 0U;
inline constexpr std::size_t kHeaderLength = 96U;
inline constexpr std::size_t kSourceLength = 24U;
inline constexpr std::size_t kDestinationLength = 16U;
inline constexpr std::size_t kKeyLength = 24U;
inline constexpr std::size_t kAggregateLength = 32U;
inline constexpr std::size_t kTrailerLength = 4U;
inline constexpr std::size_t kMinimumFrameLength =
    kHeaderLength + kSourceLength + kDestinationLength + kKeyLength + kTrailerLength;
inline constexpr std::size_t kMaximumFrameLength =
    kMaximumDistributedVectorGroupedAggregateShuffleAuthorityBytes + kHeaderLength + kTrailerLength;
} // namespace distributed_vector_grouped_aggregate_shuffle_authority_format

struct DistributedVectorGroupedAggregateShuffleAuthorityDecodeLimits {
  std::size_t maximum_frame_length{
      distributed_vector_grouped_aggregate_shuffle_authority_format::kMaximumFrameLength};
  DistributedVectorGroupedAggregateShuffleAuthorityLimits authority;
};

class EncodedDistributedVectorGroupedAggregateShuffleAuthority {
public:
  EncodedDistributedVectorGroupedAggregateShuffleAuthority() = delete;
  EncodedDistributedVectorGroupedAggregateShuffleAuthority(
      const EncodedDistributedVectorGroupedAggregateShuffleAuthority&) = delete;
  EncodedDistributedVectorGroupedAggregateShuffleAuthority&
  operator=(const EncodedDistributedVectorGroupedAggregateShuffleAuthority&) = delete;
  EncodedDistributedVectorGroupedAggregateShuffleAuthority(
      EncodedDistributedVectorGroupedAggregateShuffleAuthority&&) noexcept = default;
  EncodedDistributedVectorGroupedAggregateShuffleAuthority&
  operator=(EncodedDistributedVectorGroupedAggregateShuffleAuthority&&) noexcept = default;

  [[nodiscard]] common::ByteView bytes() const noexcept;

private:
  explicit EncodedDistributedVectorGroupedAggregateShuffleAuthority(
      std::vector<std::byte> bytes) noexcept;
  std::vector<std::byte> bytes_;

  friend common::Result<EncodedDistributedVectorGroupedAggregateShuffleAuthority>
  encode_distributed_vector_grouped_aggregate_shuffle_authority(
      const DistributedVectorGroupedAggregateShuffleAuthority& authority);
};

[[nodiscard]] common::Result<EncodedDistributedVectorGroupedAggregateShuffleAuthority>
encode_distributed_vector_grouped_aggregate_shuffle_authority(
    const DistributedVectorGroupedAggregateShuffleAuthority& authority);

[[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleAuthority>
decode_distributed_vector_grouped_aggregate_shuffle_authority_exact(
    common::ByteView bytes,
    DistributedVectorGroupedAggregateShuffleAuthorityDecodeLimits limits = {});

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_AUTHORITY_CODEC_HPP_
