#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_TRANSPORT_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_TRANSPORT_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_authority.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/distributed_vector_grouped_aggregate_exchange.hpp"
#include "chronos/query/resource_context.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <span>
#include <vector>

namespace chronos::cluster {

inline constexpr std::size_t kDistributedVectorGroupedAggregateShuffleFrameV1HeaderSize = 128U;
inline constexpr std::size_t kDistributedVectorGroupedAggregateShuffleFrameV1TrailerSize = 4U;
inline constexpr std::size_t kMaximumDistributedVectorGroupedAggregateShuffleFrameV1Size =
    kDistributedVectorGroupedAggregateShuffleFrameV1HeaderSize +
    query::distributed_vector_grouped_aggregate_exchange_format::kMaximumFrameLength +
    kDistributedVectorGroupedAggregateShuffleFrameV1TrailerSize;

struct DistributedVectorGroupedAggregateShuffleFrameV1 {
  common::Uuid query_id;
  DistributedVectorGroupedAggregateShuffleEdge edge;
  std::uint32_t partition_count{};
  query::DistributedVectorGroupedAggregateExchangeMessage payload;
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_distributed_vector_grouped_aggregate_shuffle_frame_v1(
    const DistributedVectorGroupedAggregateShuffleFrameV1& frame,
    const DistributedVectorGroupedAggregateShuffleAuthority& authority);

[[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleFrameV1>
decode_distributed_vector_grouped_aggregate_shuffle_frame_v1_exact(
    common::ByteView bytes, const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const query::QueryResourceContext& resources,
    query::DistributedVectorGroupedAggregateExchangeDecodeLimits limits = {});

struct DistributedVectorGroupedAggregateShuffleFrameV1ReadStep {
  std::size_t consumed_bytes{};
  std::optional<DistributedVectorGroupedAggregateShuffleFrameV1> frame{std::nullopt};
};

// Header-first reader for one or more sequential shuffle frames. The immutable authority is
// borrowed and must outlive this single-thread-affine owner. Header integrity, route authority, and
// allocation-driving lengths pass before one exact frame is retained; failure is sticky.
class DistributedVectorGroupedAggregateShuffleFrameV1Reader {
public:
  DistributedVectorGroupedAggregateShuffleFrameV1Reader(
      const DistributedVectorGroupedAggregateShuffleAuthority& authority,
      query::QueryResourceContext resources,
      std::size_t maximum_frame_length =
          kMaximumDistributedVectorGroupedAggregateShuffleFrameV1Size,
      query::DistributedVectorGroupedAggregateExchangeDecodeLimits payload_limits = {}) noexcept;
  DistributedVectorGroupedAggregateShuffleFrameV1Reader(
      const DistributedVectorGroupedAggregateShuffleFrameV1Reader&) = delete;
  DistributedVectorGroupedAggregateShuffleFrameV1Reader&
  operator=(const DistributedVectorGroupedAggregateShuffleFrameV1Reader&) = delete;
  DistributedVectorGroupedAggregateShuffleFrameV1Reader(
      DistributedVectorGroupedAggregateShuffleFrameV1Reader&&) = delete;
  DistributedVectorGroupedAggregateShuffleFrameV1Reader&
  operator=(DistributedVectorGroupedAggregateShuffleFrameV1Reader&&) = delete;

  [[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleFrameV1ReadStep>
  consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  std::reference_wrapper<const DistributedVectorGroupedAggregateShuffleAuthority> authority_;
  query::QueryResourceContext resources_;
  std::size_t maximum_frame_length_{};
  query::DistributedVectorGroupedAggregateExchangeDecodeLimits payload_limits_;
  std::array<std::byte, kDistributedVectorGroupedAggregateShuffleFrameV1HeaderSize> header_{};
  std::size_t header_bytes_{};
  std::vector<std::byte> frame_;
  std::size_t frame_bytes_{};
  std::optional<common::Status> failure_;
};

class DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor {
public:
  DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor() = delete;
  DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor(
      const DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor&) = delete;
  DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor&
  operator=(const DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor&) = delete;
  DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor(
      DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor&& other) noexcept;
  DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor&
  operator=(DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor&& other) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor>
  create(const DistributedVectorGroupedAggregateShuffleFrameV1& frame,
         const DistributedVectorGroupedAggregateShuffleAuthority& authority);
  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes) noexcept;
  [[nodiscard]] std::size_t written_bytes() const noexcept;
  [[nodiscard]] bool complete() const noexcept;

private:
  explicit DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor(
      std::vector<std::byte> encoded_frame) noexcept;
  std::vector<std::byte> encoded_frame_;
  std::size_t written_bytes_{};
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_TRANSPORT_HPP_
