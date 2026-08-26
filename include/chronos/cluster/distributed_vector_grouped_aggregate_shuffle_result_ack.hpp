#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RESULT_ACK_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RESULT_ACK_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_authority.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/distributed_vector_result_schema.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace chronos::cluster {

inline constexpr std::size_t kDistributedVectorGroupedAggregateShuffleResultAckV1HeaderSize = 128U;
inline constexpr std::size_t kDistributedVectorGroupedAggregateShuffleResultAckV1TrailerSize = 4U;
inline constexpr std::size_t kDistributedVectorGroupedAggregateShuffleResultAckV1Size = 132U;

struct DistributedVectorGroupedAggregateShuffleResultAckV1 {
  common::Uuid query_id;
  raft::NodeId source_node_id{};
  raft::NodeId target_node_id{};
  std::uint32_t partition_id{};
  std::uint32_t partition_count{};
  std::uint16_t hash_version{};
  std::uint32_t accepted_frames{};
  std::uint64_t accepted_bytes{};
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_distributed_vector_grouped_aggregate_shuffle_result_ack_v1(
    const DistributedVectorGroupedAggregateShuffleResultAckV1& ack,
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const query::DistributedVectorResultSchema& result_schema, raft::NodeId coordinator_node_id);

[[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleResultAckV1>
decode_distributed_vector_grouped_aggregate_shuffle_result_ack_v1_exact(
    common::ByteView bytes, const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const query::DistributedVectorResultSchema& result_schema, raft::NodeId coordinator_node_id);

struct DistributedVectorGroupedAggregateShuffleResultAckV1ReadStep {
  std::size_t consumed_bytes{};
  std::optional<DistributedVectorGroupedAggregateShuffleResultAckV1> ack;
};

class DistributedVectorGroupedAggregateShuffleResultAckV1Reader {
public:
  DistributedVectorGroupedAggregateShuffleResultAckV1Reader(
      const DistributedVectorGroupedAggregateShuffleAuthority& authority,
      const query::DistributedVectorResultSchema& result_schema,
      raft::NodeId coordinator_node_id) noexcept;
  DistributedVectorGroupedAggregateShuffleResultAckV1Reader(
      const DistributedVectorGroupedAggregateShuffleResultAckV1Reader&) = delete;
  DistributedVectorGroupedAggregateShuffleResultAckV1Reader&
  operator=(const DistributedVectorGroupedAggregateShuffleResultAckV1Reader&) = delete;
  DistributedVectorGroupedAggregateShuffleResultAckV1Reader(
      DistributedVectorGroupedAggregateShuffleResultAckV1Reader&&) = delete;
  DistributedVectorGroupedAggregateShuffleResultAckV1Reader&
  operator=(DistributedVectorGroupedAggregateShuffleResultAckV1Reader&&) = delete;

  [[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleResultAckV1ReadStep>
  consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  const DistributedVectorGroupedAggregateShuffleAuthority& authority_;
  const query::DistributedVectorResultSchema& result_schema_;
  raft::NodeId coordinator_node_id_{};
  std::array<std::byte, kDistributedVectorGroupedAggregateShuffleResultAckV1Size> bytes_{};
  std::size_t buffered_bytes_{};
  bool complete_{};
  std::optional<common::Status> failure_;
};

class DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor {
public:
  DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor() = delete;
  DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor(
      const DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor&) = delete;
  DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor&
  operator=(const DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor&) = delete;
  DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor(
      DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor&& other) noexcept;
  DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor&
  operator=(DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor&& other) noexcept;

  [[nodiscard]] static common::Result<
      DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor>
  create(const DistributedVectorGroupedAggregateShuffleResultAckV1& ack,
         const DistributedVectorGroupedAggregateShuffleAuthority& authority,
         const query::DistributedVectorResultSchema& result_schema,
         raft::NodeId coordinator_node_id);
  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes) noexcept;
  [[nodiscard]] bool complete() const noexcept;
  [[nodiscard]] std::size_t written_bytes() const noexcept;

private:
  explicit DistributedVectorGroupedAggregateShuffleResultAckV1WriteCursor(
      std::vector<std::byte> bytes) noexcept;
  std::vector<std::byte> bytes_;
  std::size_t written_bytes_{};
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RESULT_ACK_HPP_
