#ifndef CHRONOS_QUERY_DISTRIBUTED_VECTOR_FRAGMENT_V2_HPP_
#define CHRONOS_QUERY_DISTRIBUTED_VECTOR_FRAGMENT_V2_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/distributed_vector_fragment.hpp"
#include "chronos/query/distributed_vector_result_schema.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
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

struct DistributedVectorFragmentV2ReadStep {
  std::size_t consumed_bytes{};
  std::optional<DistributedVectorFragmentDispatchV2> dispatch;
};

// One owner serializes consume calls. Only the current frame is retained; a coalesced successor
// remains caller-owned until the completed dispatch has been handled.
class DistributedVectorFragmentV2Reader {
public:
  explicit DistributedVectorFragmentV2Reader(
      DistributedVectorFragmentV2DecodeLimits limits = {}) noexcept;
  DistributedVectorFragmentV2Reader(const DistributedVectorFragmentV2Reader&) = delete;
  DistributedVectorFragmentV2Reader& operator=(const DistributedVectorFragmentV2Reader&) = delete;
  DistributedVectorFragmentV2Reader(DistributedVectorFragmentV2Reader&&) = delete;
  DistributedVectorFragmentV2Reader& operator=(DistributedVectorFragmentV2Reader&&) = delete;

  [[nodiscard]] common::Result<DistributedVectorFragmentV2ReadStep> consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  DistributedVectorFragmentV2DecodeLimits limits_;
  std::array<std::byte, distributed_vector_fragment_v2_format::kHeaderLength> header_{};
  std::size_t header_bytes_{};
  std::vector<std::byte> frame_;
  std::size_t frame_bytes_{};
  std::optional<common::Status> failure_;
};

// One thread owns a cursor and serializes socket progress acknowledgements.
class DistributedVectorFragmentV2WriteCursor {
public:
  DistributedVectorFragmentV2WriteCursor() = delete;
  DistributedVectorFragmentV2WriteCursor(const DistributedVectorFragmentV2WriteCursor&) = delete;
  DistributedVectorFragmentV2WriteCursor&
  operator=(const DistributedVectorFragmentV2WriteCursor&) = delete;
  DistributedVectorFragmentV2WriteCursor(DistributedVectorFragmentV2WriteCursor&& other) noexcept;
  DistributedVectorFragmentV2WriteCursor&
  operator=(DistributedVectorFragmentV2WriteCursor&& other) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorFragmentV2WriteCursor>
  create(const DistributedVectorFragmentDispatchV2& dispatch);
  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes) noexcept;
  [[nodiscard]] std::size_t written_bytes() const noexcept;
  [[nodiscard]] bool complete() const noexcept;

private:
  explicit DistributedVectorFragmentV2WriteCursor(
      EncodedDistributedVectorFragmentDispatchV2 encoded) noexcept;
  EncodedDistributedVectorFragmentDispatchV2 encoded_;
  std::size_t written_bytes_{};
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_DISTRIBUTED_VECTOR_FRAGMENT_V2_HPP_
