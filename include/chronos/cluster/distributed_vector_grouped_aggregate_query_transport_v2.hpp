#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_QUERY_TRANSPORT_V2_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_QUERY_TRANSPORT_V2_HPP_

#include "chronos/cluster/distributed_vector_query_transport_v2.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/distributed_fragment_worker.hpp"
#include "chronos/query/distributed_vector_grouped_aggregate_exchange.hpp"
#include "chronos/query/resource_context.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <optional>
#include <span>
#include <vector>

namespace chronos::cluster {

// Grouped sufficient-state requests reuse the exact Fragment-v2 CHDVREQ2 carrier. This distinct
// response envelope prevents row, ungrouped-state, and grouped-state payload confusion.
inline constexpr std::size_t kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize = 112U;
inline constexpr std::size_t kDistributedVectorGroupedAggregateQueryResponseV2TrailerSize = 4U;
inline constexpr std::size_t kMaximumDistributedVectorGroupedAggregateQueryResponseV2Size =
    kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize +
    query::distributed_vector_grouped_aggregate_exchange_format::kMaximumFrameLength +
    kDistributedVectorGroupedAggregateQueryResponseV2TrailerSize;

struct DistributedVectorGroupedAggregateQueryResponseV2 {
  raft::NodeId source_node_id{};
  raft::NodeId target_node_id{};
  common::Uuid query_id;
  schema::TabletId tablet_id;
  common::StatusCode status_code{common::StatusCode::kInternal};
  std::optional<query::DistributedVectorGroupedAggregateExchangeMessage> payload;
  std::optional<DistributedQueryLeaderHint> leader_hint;
};

// Exact Fragment-v2 plan/result-shape validation shared by receiver and sender. It does not derive
// authority; callers must supply the vector obtained from a proof-revalidated binder.
[[nodiscard]] common::Status validate_distributed_vector_grouped_aggregate_query_authority_v2(
    const query::DistributedVectorFragmentDispatchV2& dispatch,
    std::span<const query::VectorGroupKeyDefinition> keys,
    std::span<const query::VectorAggregateDefinition> aggregates);

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_distributed_vector_grouped_aggregate_query_response_v2(
    const DistributedVectorGroupedAggregateQueryResponseV2& response,
    std::span<const query::VectorGroupKeyDefinition> expected_keys,
    std::span<const query::VectorAggregateDefinition> expected_aggregates);

[[nodiscard]] common::Result<DistributedVectorGroupedAggregateQueryResponseV2>
decode_distributed_vector_grouped_aggregate_query_response_v2_exact(
    common::ByteView bytes, std::span<const query::VectorGroupKeyDefinition> expected_keys,
    std::span<const query::VectorAggregateDefinition> expected_aggregates,
    const query::QueryResourceContext& resources,
    query::DistributedVectorGroupedAggregateExchangeDecodeLimits limits = {});

struct DistributedVectorGroupedAggregateQueryResponseV2ReadStep {
  std::size_t consumed_bytes{};
  std::optional<DistributedVectorGroupedAggregateQueryResponseV2> response;
};

// One connection owner retains the complete grouped authority and query resource context. Only a
// checksummed fixed header may drive exact frame allocation; every failure remains sticky.
class DistributedVectorGroupedAggregateQueryResponseV2Reader {
public:
  DistributedVectorGroupedAggregateQueryResponseV2Reader(
      std::vector<query::VectorGroupKeyDefinition>&& expected_keys,
      std::vector<query::VectorAggregateDefinition>&& expected_aggregates,
      query::QueryResourceContext resources,
      std::size_t maximum_frame_length =
          kMaximumDistributedVectorGroupedAggregateQueryResponseV2Size,
      query::DistributedVectorGroupedAggregateExchangeDecodeLimits payload_limits = {}) noexcept;
  DistributedVectorGroupedAggregateQueryResponseV2Reader(
      const DistributedVectorGroupedAggregateQueryResponseV2Reader&) = delete;
  DistributedVectorGroupedAggregateQueryResponseV2Reader&
  operator=(const DistributedVectorGroupedAggregateQueryResponseV2Reader&) = delete;
  DistributedVectorGroupedAggregateQueryResponseV2Reader(
      DistributedVectorGroupedAggregateQueryResponseV2Reader&&) = delete;
  DistributedVectorGroupedAggregateQueryResponseV2Reader&
  operator=(DistributedVectorGroupedAggregateQueryResponseV2Reader&&) = delete;

  [[nodiscard]] common::Result<DistributedVectorGroupedAggregateQueryResponseV2ReadStep>
  consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  std::vector<query::VectorGroupKeyDefinition> expected_keys_;
  std::vector<query::VectorAggregateDefinition> expected_aggregates_;
  query::QueryResourceContext resources_;
  std::size_t maximum_frame_length_{};
  query::DistributedVectorGroupedAggregateExchangeDecodeLimits payload_limits_;
  std::array<std::byte, kDistributedVectorGroupedAggregateQueryResponseV2HeaderSize> header_{};
  std::size_t header_bytes_{};
  std::vector<std::byte> frame_;
  std::size_t frame_bytes_{};
  std::optional<common::Status> failure_;
};

// Typed construction validates the complete grouped authority before any byte becomes writable.
class DistributedVectorGroupedAggregateQueryResponseV2WriteCursor {
public:
  DistributedVectorGroupedAggregateQueryResponseV2WriteCursor() = delete;
  DistributedVectorGroupedAggregateQueryResponseV2WriteCursor(
      const DistributedVectorGroupedAggregateQueryResponseV2WriteCursor&) = delete;
  DistributedVectorGroupedAggregateQueryResponseV2WriteCursor&
  operator=(const DistributedVectorGroupedAggregateQueryResponseV2WriteCursor&) = delete;
  DistributedVectorGroupedAggregateQueryResponseV2WriteCursor(
      DistributedVectorGroupedAggregateQueryResponseV2WriteCursor&& other) noexcept;
  DistributedVectorGroupedAggregateQueryResponseV2WriteCursor&
  operator=(DistributedVectorGroupedAggregateQueryResponseV2WriteCursor&& other) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateQueryResponseV2WriteCursor>
  create(const DistributedVectorGroupedAggregateQueryResponseV2& response,
         std::span<const query::VectorGroupKeyDefinition> expected_keys,
         std::span<const query::VectorAggregateDefinition> expected_aggregates);
  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes) noexcept;
  [[nodiscard]] std::size_t written_bytes() const noexcept;
  [[nodiscard]] bool complete() const noexcept;

private:
  explicit DistributedVectorGroupedAggregateQueryResponseV2WriteCursor(
      std::vector<std::byte> encoded_frame) noexcept;
  std::vector<std::byte> encoded_frame_;
  std::size_t written_bytes_{};
};

inline constexpr std::size_t kDefaultDistributedVectorGroupedAggregateQueryV2ResponseBytes =
    std::size_t{64U} * 1024U * 1024U;
inline constexpr std::size_t kMaximumDistributedVectorGroupedAggregateQueryV2ResponseBytes =
    std::size_t{1024U} * 1024U * 1024U;
inline constexpr std::size_t kDefaultDistributedVectorGroupedAggregateQueryV2DecodeMemoryBytes =
    std::size_t{64U} * 1024U * 1024U;
inline constexpr std::size_t kMaximumDistributedVectorGroupedAggregateQueryV2DecodeMemoryBytes =
    std::size_t{1024U} * 1024U * 1024U;

// The embedding first rebinds exact current local authority, then independently executes and
// returns its proof-derived authority beside a complete canonical stream. It must outlive the
// receiver; one caller serializes both methods for each request.
class DistributedVectorGroupedAggregateQueryWorkerServiceV2 {
public:
  virtual ~DistributedVectorGroupedAggregateQueryWorkerServiceV2() = default;
  [[nodiscard]] virtual common::Result<query::DistributedVectorGroupedAggregateAuthority>
  bind_authority(const query::DistributedVectorFragmentDispatchV2& dispatch) = 0;
  [[nodiscard]] virtual common::Result<query::DistributedVectorGroupedAggregateWorkerResultV2>
  execute(const query::DistributedVectorFragmentDispatchV2& dispatch) = 0;
};

struct DistributedVectorGroupedAggregateQueryReceiverV2Config {
  raft::NodeId local_node_id{};
  const ClusterNodePrincipalAuthorizer* authorizer{};
  DistributedVectorGroupedAggregateQueryWorkerServiceV2* worker{};
  const DistributedQueryLeaderHintProvider* leader_hint_provider{};
  std::size_t maximum_response_frames{
      query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups};
  std::size_t maximum_response_bytes{kDefaultDistributedVectorGroupedAggregateQueryV2ResponseBytes};
  std::size_t maximum_decode_memory_bytes{
      kDefaultDistributedVectorGroupedAggregateQueryV2DecodeMemoryBytes};
  query::DistributedVectorGroupedAggregateExchangeDecodeLimits payload;
};

struct DistributedVectorGroupedAggregateQueryBoundResponsesV2 {
  query::DistributedVectorGroupedAggregateAuthority authority;
  std::vector<std::vector<std::byte>> encoded_responses;
};

// Authentication, source authorization, target validation, and fresh authority binding precede
// worker execution. No success prefix is returned before the complete contiguous terminal stream
// is decoded and encoded under independent count, byte, and query-memory bounds.
class DistributedVectorGroupedAggregateQueryReceiverV2 {
public:
  DistributedVectorGroupedAggregateQueryReceiverV2() = delete;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateQueryReceiverV2>
  create(DistributedVectorGroupedAggregateQueryReceiverV2Config config);
  [[nodiscard]] common::Result<std::vector<std::vector<std::byte>>>
  receive(common::ByteView request_bytes,
          const network::PeerAuthenticationResult& authenticated_peer);
  [[nodiscard]] common::Result<DistributedVectorGroupedAggregateQueryBoundResponsesV2>
  receive_bound(common::ByteView request_bytes,
                const network::PeerAuthenticationResult& authenticated_peer);

private:
  explicit DistributedVectorGroupedAggregateQueryReceiverV2(
      DistributedVectorGroupedAggregateQueryReceiverV2Config config) noexcept;
  DistributedVectorGroupedAggregateQueryReceiverV2Config config_;
};

struct DistributedVectorGroupedAggregateQueryAttemptV2 {
  std::size_t attempt_number{};
  raft::NodeId target_node_id{};
  std::vector<std::byte> request_bytes;
};

struct DistributedVectorGroupedAggregateQuerySenderLimitsV2 {
  DistributedQueryRetryLimits retry;
  std::size_t maximum_response_frames{
      query::distributed_vector_grouped_aggregate_exchange_format::kMaximumGroups};
  std::size_t maximum_response_bytes{kDefaultDistributedVectorGroupedAggregateQueryV2ResponseBytes};
  query::DistributedVectorGroupedAggregateExchangeDecodeLimits payload;
};

// Single-threaded finite policy owner for one immutable grouped Fragment-v2 dispatch. A complete
// success stream is canonically reconstructed under owned query resources before publication;
// retries replace whole attempts and never retain prefixes.
class DistributedVectorGroupedAggregateQuerySenderV2 {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedVectorGroupedAggregateQuerySenderV2() = delete;
  DistributedVectorGroupedAggregateQuerySenderV2(
      const DistributedVectorGroupedAggregateQuerySenderV2&) = delete;
  DistributedVectorGroupedAggregateQuerySenderV2&
  operator=(const DistributedVectorGroupedAggregateQuerySenderV2&) = delete;
  DistributedVectorGroupedAggregateQuerySenderV2(
      DistributedVectorGroupedAggregateQuerySenderV2&&) noexcept = default;
  DistributedVectorGroupedAggregateQuerySenderV2&
  operator=(DistributedVectorGroupedAggregateQuerySenderV2&&) noexcept = default;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateQuerySenderV2>
  create(raft::NodeId source_node_id, query::DistributedVectorFragmentDispatchV2 dispatch,
         std::vector<query::VectorGroupKeyDefinition>&& keys,
         std::vector<query::VectorAggregateDefinition>&& aggregates,
         query::QueryResourceContext resources,
         DistributedVectorGroupedAggregateQuerySenderLimitsV2 limits = {});
  [[nodiscard]] common::Result<DistributedVectorGroupedAggregateQueryAttemptV2>
  begin_attempt(TimePoint now);
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

private:
  DistributedVectorGroupedAggregateQuerySenderV2(
      raft::NodeId source_node_id, query::DistributedVectorFragmentDispatchV2 dispatch,
      std::vector<query::VectorGroupKeyDefinition>&& keys,
      std::vector<query::VectorAggregateDefinition>&& aggregates,
      query::QueryResourceContext resources, std::vector<std::byte>&& request_bytes,
      DistributedVectorGroupedAggregateQuerySenderLimitsV2 limits) noexcept;
  [[nodiscard]] common::Status schedule(common::StatusCode code, TimePoint now);

  raft::NodeId source_node_id_{};
  query::DistributedVectorFragmentDispatchV2 dispatch_;
  std::vector<query::VectorGroupKeyDefinition> keys_;
  std::vector<query::VectorAggregateDefinition> aggregates_;
  query::QueryResourceContext resources_;
  std::vector<std::byte> request_bytes_;
  DistributedVectorGroupedAggregateQuerySenderLimitsV2 limits_;
  DistributedQuerySenderState state_{DistributedQuerySenderState::kReady};
  std::size_t attempts_started_{};
  std::chrono::milliseconds next_backoff_{};
  std::optional<TimePoint> next_attempt_not_before_;
  std::optional<common::StatusCode> last_status_code_;
  std::optional<DistributedQueryLeaderHint> suggested_leader_;
  std::optional<std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage>>
      result_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_QUERY_TRANSPORT_V2_HPP_
