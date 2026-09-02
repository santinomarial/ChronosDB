#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_ACK_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_ACK_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_authority.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace chronos::cluster {

inline constexpr std::size_t kDistributedVectorGroupedAggregateShuffleAckV1HeaderSize = 128U;
inline constexpr std::size_t kDistributedVectorGroupedAggregateShuffleAckV1TrailerSize = 4U;
inline constexpr std::size_t kDistributedVectorGroupedAggregateShuffleAckV1Size = 132U;

struct DistributedVectorGroupedAggregateShuffleAckV1 {
  common::Uuid query_id;
  DistributedVectorGroupedAggregateShuffleEdge edge;
  std::uint32_t partition_count{};
  std::uint32_t accepted_frames{};
  std::uint64_t accepted_bytes{};
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_distributed_vector_grouped_aggregate_shuffle_ack_v1(
    const DistributedVectorGroupedAggregateShuffleAckV1& ack,
    const DistributedVectorGroupedAggregateShuffleAuthority& authority);

[[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleAckV1>
decode_distributed_vector_grouped_aggregate_shuffle_ack_v1_exact(
    common::ByteView bytes, const DistributedVectorGroupedAggregateShuffleAuthority& authority);

struct DistributedVectorGroupedAggregateShuffleAckV1ReadStep {
  std::size_t consumed_bytes{};
  std::optional<DistributedVectorGroupedAggregateShuffleAckV1> ack{std::nullopt};
};

class DistributedVectorGroupedAggregateShuffleAckV1Reader {
public:
  explicit DistributedVectorGroupedAggregateShuffleAckV1Reader(
      const DistributedVectorGroupedAggregateShuffleAuthority& authority) noexcept;
  DistributedVectorGroupedAggregateShuffleAckV1Reader(
      const DistributedVectorGroupedAggregateShuffleAckV1Reader&) = delete;
  DistributedVectorGroupedAggregateShuffleAckV1Reader&
  operator=(const DistributedVectorGroupedAggregateShuffleAckV1Reader&) = delete;
  DistributedVectorGroupedAggregateShuffleAckV1Reader(
      DistributedVectorGroupedAggregateShuffleAckV1Reader&&) = delete;
  DistributedVectorGroupedAggregateShuffleAckV1Reader&
  operator=(DistributedVectorGroupedAggregateShuffleAckV1Reader&&) = delete;

  [[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleAckV1ReadStep>
  consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  const DistributedVectorGroupedAggregateShuffleAuthority& authority_;
  std::array<std::byte, kDistributedVectorGroupedAggregateShuffleAckV1Size> bytes_{};
  std::size_t buffered_bytes_{};
  bool complete_{};
  std::optional<common::Status> failure_;
};

class DistributedVectorGroupedAggregateShuffleAckV1WriteCursor {
public:
  DistributedVectorGroupedAggregateShuffleAckV1WriteCursor() = delete;
  DistributedVectorGroupedAggregateShuffleAckV1WriteCursor(
      const DistributedVectorGroupedAggregateShuffleAckV1WriteCursor&) = delete;
  DistributedVectorGroupedAggregateShuffleAckV1WriteCursor&
  operator=(const DistributedVectorGroupedAggregateShuffleAckV1WriteCursor&) = delete;
  DistributedVectorGroupedAggregateShuffleAckV1WriteCursor(
      DistributedVectorGroupedAggregateShuffleAckV1WriteCursor&& other) noexcept;
  DistributedVectorGroupedAggregateShuffleAckV1WriteCursor&
  operator=(DistributedVectorGroupedAggregateShuffleAckV1WriteCursor&& other) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateShuffleAckV1WriteCursor>
  create(const DistributedVectorGroupedAggregateShuffleAckV1& ack,
         const DistributedVectorGroupedAggregateShuffleAuthority& authority);
  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes);
  [[nodiscard]] bool complete() const noexcept;
  [[nodiscard]] std::size_t written_bytes() const noexcept;

private:
  explicit DistributedVectorGroupedAggregateShuffleAckV1WriteCursor(
      std::vector<std::byte> bytes) noexcept;
  std::vector<std::byte> bytes_;
  std::size_t written_bytes_{};
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_ACK_HPP_
