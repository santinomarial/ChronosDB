#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_STREAM_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_STREAM_HPP_

#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_transport.hpp"
#include "chronos/cluster/remote_tablet_reconfiguration.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/query/distributed_vector_grouped_aggregate_partitioner.hpp"
#include "chronos/query/resource_context.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace chronos::cluster {

inline constexpr std::size_t kDefaultDistributedVectorGroupedAggregateShuffleStreamBytes =
    std::size_t{256U} * 1024U * 1024U;
inline constexpr std::size_t kMaximumDistributedVectorGroupedAggregateShuffleStreamBytes =
    std::size_t{1024U} * 1024U * 1024U;

struct DistributedVectorGroupedAggregateShuffleStreamLimits {
  std::uint32_t maximum_frames{
      query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups};
  std::size_t maximum_encoded_bytes{kDefaultDistributedVectorGroupedAggregateShuffleStreamBytes};
  query::DistributedVectorGroupedAggregateExchangeDecodeLimits payload;
};

struct DistributedVectorGroupedAggregateShuffleCompleteStream {
  DistributedVectorGroupedAggregateShuffleEdge edge;
  std::vector<query::DistributedVectorGroupedAggregateExchangeMessage> messages;
  std::size_t encoded_bytes{};
};

struct DistributedVectorGroupedAggregateShuffleStreamReadStep {
  std::size_t consumed_bytes{};
  bool complete{};
};

// Authenticates one remote source and privately retains exactly one contiguous source-partition
// stream. The immutable authority and principal authorizer are borrowed and must outlive this
// single-thread-affine owner. No message is observable before the canonical terminal closes.
class DistributedVectorGroupedAggregateShuffleStreamReceiver {
public:
  DistributedVectorGroupedAggregateShuffleStreamReceiver() = delete;
  DistributedVectorGroupedAggregateShuffleStreamReceiver(
      const DistributedVectorGroupedAggregateShuffleStreamReceiver&) = delete;
  DistributedVectorGroupedAggregateShuffleStreamReceiver&
  operator=(const DistributedVectorGroupedAggregateShuffleStreamReceiver&) = delete;
  DistributedVectorGroupedAggregateShuffleStreamReceiver(
      DistributedVectorGroupedAggregateShuffleStreamReceiver&&) noexcept = default;
  DistributedVectorGroupedAggregateShuffleStreamReceiver&
  operator=(DistributedVectorGroupedAggregateShuffleStreamReceiver&&) noexcept = default;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateShuffleStreamReceiver>
  create(const DistributedVectorGroupedAggregateShuffleAuthority& authority,
         raft::NodeId local_node_id, const ClusterNodePrincipalAuthorizer& authorizer,
         network::PeerAuthenticationResult authenticated_peer,
         query::QueryResourceContext resources,
         DistributedVectorGroupedAggregateShuffleStreamLimits limits = {});

  [[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleStreamReadStep>
  consume(common::ByteView bytes);
  [[nodiscard]] common::Status finish_input();
  [[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleCompleteStream>
  take_complete_stream();
  [[nodiscard]] bool complete() const noexcept;
  [[nodiscard]] bool failed() const noexcept;
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] std::size_t accepted_frames() const noexcept;
  [[nodiscard]] std::size_t accepted_bytes() const noexcept;

private:
  DistributedVectorGroupedAggregateShuffleStreamReceiver(
      const DistributedVectorGroupedAggregateShuffleAuthority& authority,
      raft::NodeId local_node_id, const ClusterNodePrincipalAuthorizer& authorizer,
      network::PeerAuthenticationResult authenticated_peer,
      DistributedVectorGroupedAggregateShuffleStreamLimits limits,
      std::unique_ptr<DistributedVectorGroupedAggregateShuffleFrameV1Reader> reader) noexcept;
  [[nodiscard]] common::Status fail(common::Status status);
  [[nodiscard]] common::Status accept_frame(DistributedVectorGroupedAggregateShuffleFrameV1 frame);

  std::reference_wrapper<const DistributedVectorGroupedAggregateShuffleAuthority> authority_;
  raft::NodeId local_node_id_{};
  std::reference_wrapper<const ClusterNodePrincipalAuthorizer> authorizer_;
  network::PeerAuthenticationResult authenticated_peer_;
  DistributedVectorGroupedAggregateShuffleStreamLimits limits_;
  std::unique_ptr<DistributedVectorGroupedAggregateShuffleFrameV1Reader> reader_;
  std::optional<DistributedVectorGroupedAggregateShuffleEdge> edge_;
  std::optional<std::uint32_t> expected_group_count_;
  std::vector<query::DistributedVectorGroupedAggregateExchangeMessage> messages_;
  std::size_t accepted_bytes_{};
  bool complete_{};
  bool taken_{};
  std::optional<common::Status> failure_;
};

// Exact-decodes one complete canonical partition stream, binds every nested message to the same
// immutable remote edge, and privately constructs every outer-frame cursor before exposing bytes.
// Construction is synchronous; the resulting single-thread-affine owner is self-contained.
class DistributedVectorGroupedAggregateShuffleStreamSender {
public:
  DistributedVectorGroupedAggregateShuffleStreamSender() = delete;
  DistributedVectorGroupedAggregateShuffleStreamSender(
      const DistributedVectorGroupedAggregateShuffleStreamSender&) = delete;
  DistributedVectorGroupedAggregateShuffleStreamSender&
  operator=(const DistributedVectorGroupedAggregateShuffleStreamSender&) = delete;
  DistributedVectorGroupedAggregateShuffleStreamSender(
      DistributedVectorGroupedAggregateShuffleStreamSender&&) noexcept = default;
  DistributedVectorGroupedAggregateShuffleStreamSender&
  operator=(DistributedVectorGroupedAggregateShuffleStreamSender&&) noexcept = default;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateShuffleStreamSender>
  create(const DistributedVectorGroupedAggregateShuffleAuthority& authority,
         DistributedVectorGroupedAggregateShuffleEdge edge,
         std::span<const query::EncodedDistributedVectorGroupedAggregateExchangeMessage> messages,
         const query::QueryResourceContext& resources,
         DistributedVectorGroupedAggregateShuffleStreamLimits limits = {});

  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes);
  [[nodiscard]] bool complete() const noexcept;
  [[nodiscard]] std::size_t frame_count() const noexcept;
  [[nodiscard]] std::size_t encoded_bytes() const noexcept;
  [[nodiscard]] std::size_t written_bytes() const noexcept;
  [[nodiscard]] const DistributedVectorGroupedAggregateShuffleEdge& edge() const noexcept;

private:
  DistributedVectorGroupedAggregateShuffleStreamSender(
      DistributedVectorGroupedAggregateShuffleEdge edge,
      std::vector<DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor> writers,
      std::size_t encoded_bytes) noexcept;

  DistributedVectorGroupedAggregateShuffleEdge edge_;
  std::vector<DistributedVectorGroupedAggregateShuffleFrameV1WriteCursor> writers_;
  std::size_t encoded_bytes_{};
  std::size_t writer_index_{};
  std::size_t written_bytes_{};
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_STREAM_HPP_
