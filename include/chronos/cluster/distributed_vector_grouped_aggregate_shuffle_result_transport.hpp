#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RESULT_TRANSPORT_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RESULT_TRANSPORT_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_authority.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/query/distributed_vector_result_schema.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <vector>

namespace chronos::cluster {

namespace distributed_vector_grouped_aggregate_shuffle_result_format {
inline constexpr std::uint16_t kMajor = 1U;
inline constexpr std::uint16_t kMinor = 0U;
inline constexpr std::size_t kHeaderLength = 128U;
inline constexpr std::size_t kTrailerLength = 4U;
inline constexpr std::size_t kMaximumFrameLength =
    kHeaderLength + network::kDefaultMaximumPayloadSize + kTrailerLength;
} // namespace distributed_vector_grouped_aggregate_shuffle_result_format

struct DistributedVectorGroupedAggregateShuffleResultFrame {
  common::Uuid query_id;
  raft::NodeId source_node_id{};
  raft::NodeId target_node_id{};
  std::uint32_t partition_id{};
  std::uint32_t partition_count{};
  std::uint16_t hash_version{};
  std::uint64_t sequence{};
  bool terminal{};
  // Empty only for the terminal marker of an empty partition. Otherwise exactly one canonical
  // Native QUERY_RESULT payload whose descriptors match the proof-bound raw grouped schema.
  std::vector<std::byte> encoded_result_batch;

  friend bool operator==(const DistributedVectorGroupedAggregateShuffleResultFrame&,
                         const DistributedVectorGroupedAggregateShuffleResultFrame&) = default;
};

struct DistributedVectorGroupedAggregateShuffleResultDecodeLimits {
  std::size_t maximum_frame_length{
      distributed_vector_grouped_aggregate_shuffle_result_format::kMaximumFrameLength};
  network::QueryResultLimits result_batch;
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_distributed_vector_grouped_aggregate_shuffle_result_frame(
    const DistributedVectorGroupedAggregateShuffleResultFrame& frame,
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const query::DistributedVectorResultSchema& result_schema, raft::NodeId coordinator_node_id);

[[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleResultFrame>
decode_distributed_vector_grouped_aggregate_shuffle_result_frame_exact(
    common::ByteView bytes, const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const query::DistributedVectorResultSchema& result_schema, raft::NodeId coordinator_node_id,
    DistributedVectorGroupedAggregateShuffleResultDecodeLimits limits = {});

struct DistributedVectorGroupedAggregateShuffleResultReadStep {
  std::size_t consumed_bytes{};
  std::optional<DistributedVectorGroupedAggregateShuffleResultFrame> frame{std::nullopt};
};

// Header-first reader for sequential result frames. Authority and schema are borrowed and must
// outlive this single-thread-affine owner. Failure is sticky and no partial frame is published.
class DistributedVectorGroupedAggregateShuffleResultReader {
public:
  DistributedVectorGroupedAggregateShuffleResultReader(
      const DistributedVectorGroupedAggregateShuffleAuthority& authority,
      const query::DistributedVectorResultSchema& result_schema, raft::NodeId coordinator_node_id,
      DistributedVectorGroupedAggregateShuffleResultDecodeLimits limits = {}) noexcept;
  DistributedVectorGroupedAggregateShuffleResultReader(
      const DistributedVectorGroupedAggregateShuffleResultReader&) = delete;
  DistributedVectorGroupedAggregateShuffleResultReader&
  operator=(const DistributedVectorGroupedAggregateShuffleResultReader&) = delete;
  DistributedVectorGroupedAggregateShuffleResultReader(
      DistributedVectorGroupedAggregateShuffleResultReader&&) = delete;
  DistributedVectorGroupedAggregateShuffleResultReader&
  operator=(DistributedVectorGroupedAggregateShuffleResultReader&&) = delete;

  [[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleResultReadStep>
  consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  std::reference_wrapper<const DistributedVectorGroupedAggregateShuffleAuthority> authority_;
  std::reference_wrapper<const query::DistributedVectorResultSchema> result_schema_;
  raft::NodeId coordinator_node_id_{};
  DistributedVectorGroupedAggregateShuffleResultDecodeLimits limits_;
  std::array<std::byte, distributed_vector_grouped_aggregate_shuffle_result_format::kHeaderLength>
      header_{};
  std::size_t header_bytes_{};
  std::vector<std::byte> frame_;
  std::size_t frame_bytes_{};
  std::optional<common::Status> failure_;
};

class DistributedVectorGroupedAggregateShuffleResultWriteCursor {
public:
  DistributedVectorGroupedAggregateShuffleResultWriteCursor() = delete;
  DistributedVectorGroupedAggregateShuffleResultWriteCursor(
      const DistributedVectorGroupedAggregateShuffleResultWriteCursor&) = delete;
  DistributedVectorGroupedAggregateShuffleResultWriteCursor&
  operator=(const DistributedVectorGroupedAggregateShuffleResultWriteCursor&) = delete;
  DistributedVectorGroupedAggregateShuffleResultWriteCursor(
      DistributedVectorGroupedAggregateShuffleResultWriteCursor&& other) noexcept;
  DistributedVectorGroupedAggregateShuffleResultWriteCursor&
  operator=(DistributedVectorGroupedAggregateShuffleResultWriteCursor&& other) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateShuffleResultWriteCursor>
  create(const DistributedVectorGroupedAggregateShuffleResultFrame& frame,
         const DistributedVectorGroupedAggregateShuffleAuthority& authority,
         const query::DistributedVectorResultSchema& result_schema,
         raft::NodeId coordinator_node_id);
  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes) noexcept;
  [[nodiscard]] std::size_t written_bytes() const noexcept;
  [[nodiscard]] bool complete() const noexcept;

private:
  explicit DistributedVectorGroupedAggregateShuffleResultWriteCursor(
      std::vector<std::byte> encoded) noexcept;
  std::vector<std::byte> encoded_;
  std::size_t written_bytes_{};
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RESULT_TRANSPORT_HPP_
