#ifndef CHRONOS_QUERY_DISTRIBUTED_VECTOR_FRAGMENT_V2_HPP_
#define CHRONOS_QUERY_DISTRIBUTED_VECTOR_FRAGMENT_V2_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/distributed_vector_fragment.hpp"
#include "chronos/query/distributed_vector_result_schema.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace chronos::query {

namespace distributed_vector_fragment_v2_format {
inline constexpr std::uint16_t kMajor = 2U;
inline constexpr std::uint16_t kMinor = 0U;
inline constexpr std::size_t kHeaderLength = 64U;
inline constexpr std::size_t kTrailerLength = 4U;
inline constexpr std::size_t kMaximumFrameLength =
    kHeaderLength + distributed_vector_fragment_format::kMaximumFrameLength +
    distributed_vector_result_schema_format::kMaximumFrameLength + kTrailerLength;
} // namespace distributed_vector_fragment_v2_format

struct DistributedVectorFragmentDispatchV2 {
  DistributedVectorFragmentDispatch dispatch;
  DistributedVectorResultSchema result_schema;

  friend bool operator==(const DistributedVectorFragmentDispatchV2&,
                         const DistributedVectorFragmentDispatchV2&) = default;
};

struct DistributedVectorFragmentV2DecodeLimits {
  std::size_t maximum_frame_length{distributed_vector_fragment_v2_format::kMaximumFrameLength};
  DistributedVectorFragmentDecodeLimits dispatch;
  DistributedVectorResultSchemaDecodeLimits result_schema;
};

class EncodedDistributedVectorFragmentDispatchV2 {
public:
  EncodedDistributedVectorFragmentDispatchV2() = delete;
  EncodedDistributedVectorFragmentDispatchV2(const EncodedDistributedVectorFragmentDispatchV2&) =
      delete;
  EncodedDistributedVectorFragmentDispatchV2&
  operator=(const EncodedDistributedVectorFragmentDispatchV2&) = delete;
  EncodedDistributedVectorFragmentDispatchV2(
      EncodedDistributedVectorFragmentDispatchV2&&) noexcept = default;
  EncodedDistributedVectorFragmentDispatchV2&
  operator=(EncodedDistributedVectorFragmentDispatchV2&&) noexcept = default;

  [[nodiscard]] common::ByteView bytes() const noexcept;

private:
  explicit EncodedDistributedVectorFragmentDispatchV2(std::vector<std::byte> bytes) noexcept;
  std::vector<std::byte> bytes_;
  friend common::Result<EncodedDistributedVectorFragmentDispatchV2>
  encode_distributed_vector_fragment_dispatch_v2(const DistributedVectorFragmentDispatchV2&);
};

[[nodiscard]] common::Result<EncodedDistributedVectorFragmentDispatchV2>
encode_distributed_vector_fragment_dispatch_v2(const DistributedVectorFragmentDispatchV2& value);
[[nodiscard]] common::Result<DistributedVectorFragmentDispatchV2>
decode_distributed_vector_fragment_dispatch_v2_exact(
    common::ByteView bytes, DistributedVectorFragmentV2DecodeLimits limits = {});

} // namespace chronos::query

#endif // CHRONOS_QUERY_DISTRIBUTED_VECTOR_FRAGMENT_V2_HPP_
