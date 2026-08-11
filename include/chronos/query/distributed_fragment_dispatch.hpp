#ifndef CHRONOS_QUERY_DISTRIBUTED_FRAGMENT_DISPATCH_HPP_
#define CHRONOS_QUERY_DISTRIBUTED_FRAGMENT_DISPATCH_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/query/distributed_fragment.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace chronos::query {

namespace distributed_fragment_dispatch_format {
inline constexpr std::uint16_t kMajor = 1U;
inline constexpr std::uint16_t kMinor = 0U;
inline constexpr std::size_t kHeaderLength = 80U;
inline constexpr std::size_t kTrailerLength = 4U;
inline constexpr std::size_t kMaximumFrameLength =
    kHeaderLength + distributed_fragment_format::kMaximumFrameLength + kTrailerLength;
} // namespace distributed_fragment_dispatch_format

struct DistributedAggregateFragmentDispatch {
  common::Uuid raft_group_id;
  DistributedAggregateFragment fragment;

  friend bool operator==(const DistributedAggregateFragmentDispatch&,
                         const DistributedAggregateFragmentDispatch&) = default;
};

class EncodedDistributedAggregateFragmentDispatch {
public:
  [[nodiscard]] common::ByteView bytes() const noexcept;

private:
  explicit EncodedDistributedAggregateFragmentDispatch(std::vector<std::byte> bytes) noexcept;
  std::vector<std::byte> bytes_;

  friend common::Result<EncodedDistributedAggregateFragmentDispatch>
  encode_distributed_aggregate_fragment_dispatch(const DistributedAggregateFragmentDispatch&);
};

[[nodiscard]] common::Result<EncodedDistributedAggregateFragmentDispatch>
encode_distributed_aggregate_fragment_dispatch(
    const DistributedAggregateFragmentDispatch& dispatch);

[[nodiscard]] common::Result<DistributedAggregateFragmentDispatch>
decode_distributed_aggregate_fragment_dispatch_exact(common::ByteView bytes,
                                                     DistributedFragmentDecodeLimits limits = {});

} // namespace chronos::query

#endif // CHRONOS_QUERY_DISTRIBUTED_FRAGMENT_DISPATCH_HPP_
