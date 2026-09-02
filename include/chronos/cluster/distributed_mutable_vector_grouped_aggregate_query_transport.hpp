#ifndef CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_GROUPED_AGGREGATE_QUERY_TRANSPORT_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_GROUPED_AGGREGATE_QUERY_TRANSPORT_HPP_

#include "chronos/cluster/distributed_mutable_vector_query_transport.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_query_transport_v2.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/distributed_fragment_worker.hpp"
#include "chronos/query/resource_context.hpp"

#include <chrono>
#include <cstddef>
#include <optional>
#include <span>
#include <vector>

namespace chronos::cluster {

// Mutable grouped requests use the exact CHDMREQ1 carrier and grouped responses use CHDVGRP2.
// The combination is valid only at this endpoint: neither request nor response reinterprets
// Manifest-backed Fragment-v2 authority.
[[nodiscard]] common::Status validate_distributed_mutable_vector_grouped_aggregate_query_authority(
    const query::DistributedMutableVectorFragment& fragment,
    std::span<const query::VectorGroupKeyDefinition> keys,
    std::span<const query::VectorAggregateDefinition> aggregates);

class DistributedMutableVectorGroupedAggregateQueryWorkerService {
public:
  virtual ~DistributedMutableVectorGroupedAggregateQueryWorkerService() = default;
  [[nodiscard]] virtual common::Result<query::DistributedVectorGroupedAggregateAuthority>
  bind_authority(const query::DistributedMutableVectorFragment& fragment) = 0;
  [[nodiscard]] virtual common::Result<query::DistributedVectorGroupedAggregateWorkerResultV2>
  execute(const query::DistributedMutableVectorFragment& fragment) = 0;
};

struct DistributedMutableVectorGroupedAggregateQueryReceiverConfig {
  raft::NodeId local_node_id{};
  const ClusterNodePrincipalAuthorizer* authorizer{};
  DistributedMutableVectorGroupedAggregateQueryWorkerService* worker{};
  const DistributedQueryLeaderHintProvider* leader_hint_provider{};
  std::size_t maximum_response_frames{
      query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups};
  std::size_t maximum_response_bytes{kDefaultDistributedVectorGroupedAggregateQueryV2ResponseBytes};
  std::size_t maximum_decode_memory_bytes{
      kDefaultDistributedVectorGroupedAggregateQueryV2DecodeMemoryBytes};
  query::DistributedVectorGroupedAggregateExchangeDecodeLimits payload{};
};

struct DistributedMutableVectorGroupedAggregateQueryBoundResponses {
  query::DistributedVectorGroupedAggregateAuthority authority;
  std::vector<std::vector<std::byte>> encoded_responses;
};

// Authentication and claimed-source authorization precede fresh mutable authority binding. The
// complete worker stream is re-decoded and re-encoded before any response vector is published.
class DistributedMutableVectorGroupedAggregateQueryReceiver {
public:
  DistributedMutableVectorGroupedAggregateQueryReceiver() = delete;

  [[nodiscard]] static common::Result<DistributedMutableVectorGroupedAggregateQueryReceiver>
  create(DistributedMutableVectorGroupedAggregateQueryReceiverConfig config);
  [[nodiscard]] common::Result<std::vector<std::vector<std::byte>>>
  receive(common::ByteView request_bytes,
          const network::PeerAuthenticationResult& authenticated_peer);
  [[nodiscard]] common::Result<DistributedMutableVectorGroupedAggregateQueryBoundResponses>
  receive_bound(common::ByteView request_bytes,
                const network::PeerAuthenticationResult& authenticated_peer);

private:
  explicit DistributedMutableVectorGroupedAggregateQueryReceiver(
      DistributedMutableVectorGroupedAggregateQueryReceiverConfig config) noexcept;
  DistributedMutableVectorGroupedAggregateQueryReceiverConfig config_;
};

struct DistributedMutableVectorGroupedAggregateQueryAttempt {
  std::size_t attempt_number{};
  raft::NodeId target_node_id{};
  std::vector<std::byte> request_bytes;
};

struct DistributedMutableVectorGroupedAggregateQuerySenderLimits {
  DistributedQueryRetryLimits retry{};
  std::size_t maximum_response_frames{
      query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups};
  std::size_t maximum_response_bytes{kDefaultDistributedVectorGroupedAggregateQueryV2ResponseBytes};
  query::DistributedVectorGroupedAggregateExchangeDecodeLimits payload{};
};

// Finite single-threaded retry owner. Attempts retain byte-identical mutable authority and only a
// complete canonical grouped stream becomes visible to the caller.
class DistributedMutableVectorGroupedAggregateQuerySender {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedMutableVectorGroupedAggregateQuerySender() = delete;
  DistributedMutableVectorGroupedAggregateQuerySender(
      const DistributedMutableVectorGroupedAggregateQuerySender&) = delete;
  DistributedMutableVectorGroupedAggregateQuerySender&
  operator=(const DistributedMutableVectorGroupedAggregateQuerySender&) = delete;
  DistributedMutableVectorGroupedAggregateQuerySender(
      DistributedMutableVectorGroupedAggregateQuerySender&&) noexcept = default;
  DistributedMutableVectorGroupedAggregateQuerySender&
  operator=(DistributedMutableVectorGroupedAggregateQuerySender&&) noexcept = default;

  [[nodiscard]] static common::Result<DistributedMutableVectorGroupedAggregateQuerySender>
  create(raft::NodeId source_node_id, query::DistributedMutableVectorFragment fragment,
         std::vector<query::VectorGroupKeyDefinition>&& keys,
         std::vector<query::VectorAggregateDefinition>&& aggregates,
         query::QueryResourceContext resources,
         DistributedMutableVectorGroupedAggregateQuerySenderLimits limits = {});
  // Creates the same finite sender authority without a CHDMREQ1 self-route. Only execute_local()
  // may advance this sender, through the explicitly supplied in-process worker.
  [[nodiscard]] static common::Result<DistributedMutableVectorGroupedAggregateQuerySender>
  create_local(raft::NodeId local_node_id, query::DistributedMutableVectorFragment fragment,
               std::vector<query::VectorGroupKeyDefinition>&& keys,
               std::vector<query::VectorAggregateDefinition>&& aggregates,
               query::QueryResourceContext resources,
               DistributedMutableVectorGroupedAggregateQuerySenderLimits limits = {});
  [[nodiscard]] common::Result<DistributedMutableVectorGroupedAggregateQueryAttempt>
  begin_attempt(TimePoint now);
  [[nodiscard]] common::Status
  execute_local(DistributedMutableVectorGroupedAggregateQueryWorkerService& worker, TimePoint now);
  [[nodiscard]] common::Status
  accept_responses(std::span<const DistributedVectorGroupedAggregateQueryResponseV2> responses,
                   TimePoint now);
  [[nodiscard]] common::Status record_transport_failure(common::StatusCode code, TimePoint now);

  [[nodiscard]] DistributedQuerySenderState state() const noexcept;
  [[nodiscard]] std::size_t attempts_started() const noexcept;
  [[nodiscard]] std::optional<TimePoint> next_attempt_not_before() const noexcept;
  [[nodiscard]] std::optional<common::StatusCode> last_status_code() const noexcept;
  [[nodiscard]] std::optional<DistributedQueryLeaderHint> suggested_leader() const noexcept;
  [[nodiscard]] std::span<const query::VectorGroupKeyDefinition> keys() const noexcept;
  [[nodiscard]] std::span<const query::VectorAggregateDefinition> aggregates() const noexcept;
  [[nodiscard]] const std::optional<
      std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage>>&
  result() const noexcept;
  // Transfers the complete canonical stream exactly once after success. The sender remains
  // terminal and retains its diagnostic authority.
  [[nodiscard]] common::Result<
      std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage>>
  take_result();

private:
  DistributedMutableVectorGroupedAggregateQuerySender(
      raft::NodeId source_node_id, query::DistributedMutableVectorFragment fragment,
      std::vector<query::VectorGroupKeyDefinition>&& keys,
      std::vector<query::VectorAggregateDefinition>&& aggregates,
      query::QueryResourceContext resources, std::vector<std::byte>&& request_bytes,
      DistributedMutableVectorGroupedAggregateQuerySenderLimits limits, bool local) noexcept;
  [[nodiscard]] common::Status schedule(common::StatusCode code, TimePoint now);

  raft::NodeId source_node_id_{};
  query::DistributedMutableVectorFragment fragment_;
  std::vector<query::VectorGroupKeyDefinition> keys_;
  std::vector<query::VectorAggregateDefinition> aggregates_;
  query::QueryResourceContext resources_;
  std::vector<std::byte> request_bytes_;
  DistributedMutableVectorGroupedAggregateQuerySenderLimits limits_;
  DistributedQuerySenderState state_{DistributedQuerySenderState::kReady};
  std::size_t attempts_started_{};
  std::chrono::milliseconds next_backoff_{};
  std::optional<TimePoint> next_attempt_not_before_;
  std::optional<common::StatusCode> last_status_code_;
  std::optional<DistributedQueryLeaderHint> suggested_leader_;
  std::optional<std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage>>
      result_;
  bool local_{};
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_GROUPED_AGGREGATE_QUERY_TRANSPORT_HPP_
