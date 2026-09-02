#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RESULT_STREAM_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RESULT_STREAM_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_transport.hpp"
#include "chronos/cluster/remote_tablet_reconfiguration.hpp"
#include "chronos/network/security.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace chronos::cluster {

inline constexpr std::size_t kDefaultDistributedVectorGroupedAggregateShuffleResultStreamBytes =
    std::size_t{256U} * 1024U * 1024U;
inline constexpr std::size_t kMaximumDistributedVectorGroupedAggregateShuffleResultStreamBytes =
    std::size_t{1024U} * 1024U * 1024U;
inline constexpr std::uint32_t kMaximumDistributedVectorGroupedAggregateShuffleResultStreamFrames =
    65'536U;

struct DistributedVectorGroupedAggregateShuffleResultStreamLimits {
  std::uint32_t maximum_frames{4096U};
  std::size_t maximum_encoded_bytes{
      kDefaultDistributedVectorGroupedAggregateShuffleResultStreamBytes};
  DistributedVectorGroupedAggregateShuffleResultDecodeLimits frame{};
};

[[nodiscard]] bool validate_distributed_vector_grouped_aggregate_shuffle_result_stream_limits(
    const DistributedVectorGroupedAggregateShuffleResultStreamLimits& limits) noexcept;

struct DistributedVectorGroupedAggregateShuffleCompleteResultStream {
  common::Uuid query_id;
  raft::NodeId source_node_id{};
  raft::NodeId target_node_id{};
  std::uint32_t partition_id{};
  std::vector<std::vector<std::byte>> encoded_result_batches;
  std::uint32_t frame_count{};
  std::size_t encoded_bytes{};
};

// Constructs the in-process representation of a reducer result when the reducer and coordinator
// are the same node. The wire format deliberately rejects this self-route; this helper applies the
// same authority, schema, frame-count, payload, and equivalent encoded-extent limits without
// manufacturing a peer identity or network frame.
[[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleCompleteResultStream>
create_distributed_vector_grouped_aggregate_shuffle_local_result_stream(
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const query::DistributedVectorResultSchema& result_schema, std::uint32_t partition_id,
    raft::NodeId local_node_id, std::vector<std::vector<std::byte>> encoded_result_batches,
    DistributedVectorGroupedAggregateShuffleResultStreamLimits limits = {});
[[nodiscard]] common::Status
validate_distributed_vector_grouped_aggregate_shuffle_local_result_stream(
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const query::DistributedVectorResultSchema& result_schema,
    const DistributedVectorGroupedAggregateShuffleCompleteResultStream& stream,
    DistributedVectorGroupedAggregateShuffleResultStreamLimits limits = {});

struct DistributedVectorGroupedAggregateShuffleResultStreamReadStep {
  std::size_t consumed_bytes{};
  bool complete{};
};

// Authenticates one authority destination and privately retains one contiguous partition result.
// No batch is observable before the exact terminal closes. Authority, schema, and authorizer are
// borrowed and outlive this single-thread-affine owner.
class DistributedVectorGroupedAggregateShuffleResultStreamReceiver {
public:
  DistributedVectorGroupedAggregateShuffleResultStreamReceiver() = delete;
  DistributedVectorGroupedAggregateShuffleResultStreamReceiver(
      const DistributedVectorGroupedAggregateShuffleResultStreamReceiver&) = delete;
  DistributedVectorGroupedAggregateShuffleResultStreamReceiver&
  operator=(const DistributedVectorGroupedAggregateShuffleResultStreamReceiver&) = delete;
  DistributedVectorGroupedAggregateShuffleResultStreamReceiver(
      DistributedVectorGroupedAggregateShuffleResultStreamReceiver&&) noexcept = default;
  DistributedVectorGroupedAggregateShuffleResultStreamReceiver&
  operator=(DistributedVectorGroupedAggregateShuffleResultStreamReceiver&&) noexcept = default;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateShuffleResultStreamReceiver>
  create(const DistributedVectorGroupedAggregateShuffleAuthority& authority,
         const query::DistributedVectorResultSchema& result_schema,
         raft::NodeId coordinator_node_id, const ClusterNodePrincipalAuthorizer& authorizer,
         network::PeerAuthenticationResult authenticated_peer,
         DistributedVectorGroupedAggregateShuffleResultStreamLimits limits = {});

  [[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleResultStreamReadStep>
  consume(common::ByteView bytes);
  [[nodiscard]] common::Status finish_input();
  [[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleCompleteResultStream>
  take_complete_stream();
  [[nodiscard]] bool complete() const noexcept;
  [[nodiscard]] bool failed() const noexcept;
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] std::size_t accepted_frames() const noexcept;
  [[nodiscard]] std::size_t accepted_bytes() const noexcept;

private:
  DistributedVectorGroupedAggregateShuffleResultStreamReceiver(
      const DistributedVectorGroupedAggregateShuffleAuthority& authority,
      const query::DistributedVectorResultSchema& result_schema, raft::NodeId coordinator_node_id,
      const ClusterNodePrincipalAuthorizer& authorizer,
      network::PeerAuthenticationResult authenticated_peer,
      DistributedVectorGroupedAggregateShuffleResultStreamLimits limits,
      std::unique_ptr<DistributedVectorGroupedAggregateShuffleResultReader> reader) noexcept;
  [[nodiscard]] common::Status fail(common::Status status);
  [[nodiscard]] common::Status
  accept_frame(DistributedVectorGroupedAggregateShuffleResultFrame frame);

  std::reference_wrapper<const DistributedVectorGroupedAggregateShuffleAuthority> authority_;
  std::reference_wrapper<const query::DistributedVectorResultSchema> result_schema_;
  raft::NodeId coordinator_node_id_{};
  std::reference_wrapper<const ClusterNodePrincipalAuthorizer> authorizer_;
  network::PeerAuthenticationResult authenticated_peer_;
  DistributedVectorGroupedAggregateShuffleResultStreamLimits limits_;
  std::unique_ptr<DistributedVectorGroupedAggregateShuffleResultReader> reader_;
  std::optional<raft::NodeId> source_node_id_;
  std::optional<std::uint32_t> partition_id_;
  std::vector<std::vector<std::byte>> batches_;
  std::size_t accepted_frames_{};
  std::size_t accepted_bytes_{};
  bool complete_{};
  bool taken_{};
  std::optional<common::Status> failure_;
};

// Constructs every frame before exposing bytes. Empty batches produce one canonical empty
// terminal; otherwise each supplied nonempty batch receives contiguous sequence and the last is
// terminal. The resulting single-thread-affine owner is self-contained.
class DistributedVectorGroupedAggregateShuffleResultStreamSender {
public:
  DistributedVectorGroupedAggregateShuffleResultStreamSender() = delete;
  DistributedVectorGroupedAggregateShuffleResultStreamSender(
      const DistributedVectorGroupedAggregateShuffleResultStreamSender&) = delete;
  DistributedVectorGroupedAggregateShuffleResultStreamSender&
  operator=(const DistributedVectorGroupedAggregateShuffleResultStreamSender&) = delete;
  DistributedVectorGroupedAggregateShuffleResultStreamSender(
      DistributedVectorGroupedAggregateShuffleResultStreamSender&&) noexcept = default;
  DistributedVectorGroupedAggregateShuffleResultStreamSender&
  operator=(DistributedVectorGroupedAggregateShuffleResultStreamSender&&) noexcept = default;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateShuffleResultStreamSender>
  create(const DistributedVectorGroupedAggregateShuffleAuthority& authority,
         const query::DistributedVectorResultSchema& result_schema, std::uint32_t partition_id,
         raft::NodeId source_node_id, raft::NodeId coordinator_node_id,
         std::span<const std::vector<std::byte>> encoded_result_batches,
         DistributedVectorGroupedAggregateShuffleResultStreamLimits limits = {});

  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes);
  [[nodiscard]] bool complete() const noexcept;
  [[nodiscard]] std::size_t frame_count() const noexcept;
  [[nodiscard]] std::size_t encoded_bytes() const noexcept;
  [[nodiscard]] std::size_t written_bytes() const noexcept;
  [[nodiscard]] std::uint32_t partition_id() const noexcept;
  [[nodiscard]] raft::NodeId source_node_id() const noexcept;
  [[nodiscard]] raft::NodeId coordinator_node_id() const noexcept;

private:
  struct StreamIdentity {
    std::uint32_t partition_id{};
    raft::NodeId source_node_id{};
    raft::NodeId coordinator_node_id{};
  };

  DistributedVectorGroupedAggregateShuffleResultStreamSender(
      StreamIdentity identity,
      std::vector<DistributedVectorGroupedAggregateShuffleResultWriteCursor> writers,
      std::size_t encoded_bytes) noexcept;

  std::uint32_t partition_id_{};
  raft::NodeId source_node_id_{};
  raft::NodeId coordinator_node_id_{};
  std::vector<DistributedVectorGroupedAggregateShuffleResultWriteCursor> writers_;
  std::size_t encoded_bytes_{};
  std::size_t writer_index_{};
  std::size_t written_bytes_{};
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RESULT_STREAM_HPP_
