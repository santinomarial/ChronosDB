#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_AGGREGATE_QUERY_TRANSPORT_V2_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_AGGREGATE_QUERY_TRANSPORT_V2_HPP_

#include "chronos/cluster/distributed_vector_query_transport_v2.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/distributed_fragment_worker.hpp"
#include "chronos/query/distributed_vector_aggregate_exchange.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <optional>
#include <span>
#include <vector>

namespace chronos::cluster {

// Aggregate requests use the exact Fragment-v2 CHDVREQ2 carrier. The distinct response magic and
// typed API prevent row-result and merge-state response formats from being reinterpreted.
inline constexpr std::size_t kDistributedVectorAggregateQueryResponseV2HeaderSize = 112U;
inline constexpr std::size_t kDistributedVectorAggregateQueryResponseV2TrailerSize = 4U;
inline constexpr std::size_t kMaximumDistributedVectorAggregateQueryResponseV2Size =
    kDistributedVectorAggregateQueryResponseV2HeaderSize +
    query::distributed_vector_aggregate_exchange_format::kMaximumFrameLength +
    kDistributedVectorAggregateQueryResponseV2TrailerSize;

struct DistributedVectorAggregateQueryResponseV2 {
  raft::NodeId source_node_id{};
  raft::NodeId target_node_id{};
  common::Uuid query_id;
  schema::TabletId tablet_id;
  common::StatusCode status_code{common::StatusCode::kInternal};
  std::optional<query::DistributedVectorAggregateExchangeMessage> payload;
  std::optional<DistributedQueryLeaderHint> leader_hint;
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_distributed_vector_aggregate_query_response_v2(
    const DistributedVectorAggregateQueryResponseV2& response,
    std::span<const query::VectorAggregateDefinition> expected_definitions);

[[nodiscard]] common::Result<DistributedVectorAggregateQueryResponseV2>
decode_distributed_vector_aggregate_query_response_v2_exact(
    common::ByteView bytes, std::span<const query::VectorAggregateDefinition> expected_definitions,
    const query::QueryResourceContext& resources,
    query::DistributedVectorAggregateExchangeDecodeLimits limits = {});

struct DistributedVectorAggregateQueryResponseV2ReadStep {
  std::size_t consumed_bytes{};
  std::optional<DistributedVectorAggregateQueryResponseV2> response;
};

// One connection owner transfers the admitted aggregate definitions and query resource authority.
// Only a checksummed fixed header may drive exact frame allocation; failure remains sticky.
class DistributedVectorAggregateQueryResponseV2Reader {
public:
  DistributedVectorAggregateQueryResponseV2Reader(
      std::vector<query::VectorAggregateDefinition>&& expected_definitions,
      query::QueryResourceContext resources,
      std::size_t maximum_frame_length = kMaximumDistributedVectorAggregateQueryResponseV2Size,
      query::DistributedVectorAggregateExchangeDecodeLimits payload_limits = {}) noexcept;
  DistributedVectorAggregateQueryResponseV2Reader(
      const DistributedVectorAggregateQueryResponseV2Reader&) = delete;
  DistributedVectorAggregateQueryResponseV2Reader&
  operator=(const DistributedVectorAggregateQueryResponseV2Reader&) = delete;
  DistributedVectorAggregateQueryResponseV2Reader(
      DistributedVectorAggregateQueryResponseV2Reader&&) = delete;
  DistributedVectorAggregateQueryResponseV2Reader&
  operator=(DistributedVectorAggregateQueryResponseV2Reader&&) = delete;

  [[nodiscard]] common::Result<DistributedVectorAggregateQueryResponseV2ReadStep>
  consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  std::vector<query::VectorAggregateDefinition> expected_definitions_;
  query::QueryResourceContext resources_;
  std::size_t maximum_frame_length_{};
  query::DistributedVectorAggregateExchangeDecodeLimits payload_limits_;
  std::array<std::byte, kDistributedVectorAggregateQueryResponseV2HeaderSize> header_{};
  std::size_t header_bytes_{};
  std::vector<std::byte> frame_;
  std::size_t frame_bytes_{};
  std::optional<common::Status> failure_;
};

// Typed construction requires definition-bound payload validation before any byte is writable.
class DistributedVectorAggregateQueryResponseV2WriteCursor {
public:
  DistributedVectorAggregateQueryResponseV2WriteCursor() = delete;
  DistributedVectorAggregateQueryResponseV2WriteCursor(
      const DistributedVectorAggregateQueryResponseV2WriteCursor&) = delete;
  DistributedVectorAggregateQueryResponseV2WriteCursor&
  operator=(const DistributedVectorAggregateQueryResponseV2WriteCursor&) = delete;
  DistributedVectorAggregateQueryResponseV2WriteCursor(
      DistributedVectorAggregateQueryResponseV2WriteCursor&& other) noexcept;
  DistributedVectorAggregateQueryResponseV2WriteCursor&
  operator=(DistributedVectorAggregateQueryResponseV2WriteCursor&& other) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorAggregateQueryResponseV2WriteCursor>
  create(const DistributedVectorAggregateQueryResponseV2& response,
         std::span<const query::VectorAggregateDefinition> expected_definitions);
  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes) noexcept;
  [[nodiscard]] std::size_t written_bytes() const noexcept;
  [[nodiscard]] bool complete() const noexcept;

private:
  explicit DistributedVectorAggregateQueryResponseV2WriteCursor(
      std::vector<std::byte> encoded_frame) noexcept;
  std::vector<std::byte> encoded_frame_;
  std::size_t written_bytes_{};
};

inline constexpr std::size_t kDefaultDistributedVectorAggregateQueryV2ResponseBytes =
    std::size_t{64U} * 1024U * 1024U;
inline constexpr std::size_t kMaximumDistributedVectorAggregateQueryV2ResponseBytes =
    std::size_t{1024U} * 1024U * 1024U;

// The embedding first binds definitions from current local authority, then independently executes
// and returns the proof-revalidated definitions beside the complete canonical state vector. It
// must outlive the receiver; one caller serializes both methods for a request.
class DistributedVectorAggregateQueryWorkerServiceV2 {
public:
  virtual ~DistributedVectorAggregateQueryWorkerServiceV2() = default;
  [[nodiscard]] virtual common::Result<std::vector<query::VectorAggregateDefinition>>
  bind_definitions(const query::DistributedVectorFragmentDispatchV2& dispatch) = 0;
  [[nodiscard]] virtual common::Result<query::DistributedVectorAggregateWorkerResultV2>
  execute(const query::DistributedVectorFragmentDispatchV2& dispatch) = 0;
};

struct DistributedVectorAggregateQueryReceiverV2Config {
  raft::NodeId local_node_id{};
  const ClusterNodePrincipalAuthorizer* authorizer{};
  DistributedVectorAggregateQueryWorkerServiceV2* worker{};
  const DistributedQueryLeaderHintProvider* leader_hint_provider{};
  std::size_t maximum_response_frames{query::kMaximumUngroupedAggregateWidth};
  std::size_t maximum_response_bytes{kDefaultDistributedVectorAggregateQueryV2ResponseBytes};
};

// Authentication, source authorization, local-target validation, and definition binding precede
// execution. No encoded success prefix is returned before the complete exact state vector passes.
class DistributedVectorAggregateQueryReceiverV2 {
public:
  DistributedVectorAggregateQueryReceiverV2() = delete;

  [[nodiscard]] static common::Result<DistributedVectorAggregateQueryReceiverV2>
  create(DistributedVectorAggregateQueryReceiverV2Config config);
  [[nodiscard]] common::Result<std::vector<std::vector<std::byte>>>
  receive(common::ByteView request_bytes,
          const network::PeerAuthenticationResult& authenticated_peer);

private:
  explicit DistributedVectorAggregateQueryReceiverV2(
      DistributedVectorAggregateQueryReceiverV2Config config) noexcept;
  DistributedVectorAggregateQueryReceiverV2Config config_;
};

struct DistributedVectorAggregateQueryAttemptV2 {
  std::size_t attempt_number{};
  raft::NodeId target_node_id{};
  std::vector<std::byte> request_bytes;
};

struct DistributedVectorAggregateQuerySenderLimitsV2 {
  DistributedQueryRetryLimits retry;
  std::size_t maximum_response_frames{query::kMaximumUngroupedAggregateWidth};
  std::size_t maximum_response_bytes{kDefaultDistributedVectorAggregateQueryV2ResponseBytes};
  query::DistributedVectorAggregateExchangeDecodeLimits payload;
};

// Single-threaded policy owner for one immutable definition-bound aggregate dispatch. Accepted
// states are reconstructed under the owned query resource authority before all-or-nothing publish.
class DistributedVectorAggregateQuerySenderV2 {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedVectorAggregateQuerySenderV2() = delete;
  DistributedVectorAggregateQuerySenderV2(const DistributedVectorAggregateQuerySenderV2&) = delete;
  DistributedVectorAggregateQuerySenderV2&
  operator=(const DistributedVectorAggregateQuerySenderV2&) = delete;
  DistributedVectorAggregateQuerySenderV2(DistributedVectorAggregateQuerySenderV2&&) noexcept =
      default;
  DistributedVectorAggregateQuerySenderV2&
  operator=(DistributedVectorAggregateQuerySenderV2&&) noexcept = default;

  [[nodiscard]] static common::Result<DistributedVectorAggregateQuerySenderV2>
  create(raft::NodeId source_node_id, query::DistributedVectorFragmentDispatchV2 dispatch,
         std::vector<query::VectorAggregateDefinition>&& definitions,
         query::QueryResourceContext resources,
         DistributedVectorAggregateQuerySenderLimitsV2 limits = {});
  [[nodiscard]] common::Result<DistributedVectorAggregateQueryAttemptV2>
  begin_attempt(TimePoint now);
  [[nodiscard]] common::Status
  accept_responses(std::span<const DistributedVectorAggregateQueryResponseV2> responses,
                   TimePoint now);
  [[nodiscard]] common::Status record_transport_failure(common::StatusCode code, TimePoint now);

  [[nodiscard]] DistributedQuerySenderState state() const noexcept;
  [[nodiscard]] std::size_t attempts_started() const noexcept;
  [[nodiscard]] std::optional<TimePoint> next_attempt_not_before() const noexcept;
  [[nodiscard]] std::optional<common::StatusCode> last_status_code() const noexcept;
  [[nodiscard]] std::optional<DistributedQueryLeaderHint> suggested_leader() const noexcept;
  [[nodiscard]] const std::vector<query::VectorAggregateDefinition>& definitions() const noexcept;
  [[nodiscard]]
  const std::optional<std::vector<query::DistributedVectorAggregateExchangeMessage>>&
  result() const noexcept;

private:
  DistributedVectorAggregateQuerySenderV2(
      raft::NodeId source_node_id, query::DistributedVectorFragmentDispatchV2 dispatch,
      std::vector<query::VectorAggregateDefinition>&& definitions,
      query::QueryResourceContext resources, std::vector<std::byte>&& request_bytes,
      DistributedVectorAggregateQuerySenderLimitsV2 limits) noexcept;
  [[nodiscard]] common::Status schedule(common::StatusCode code, TimePoint now);

  raft::NodeId source_node_id_{};
  query::DistributedVectorFragmentDispatchV2 dispatch_;
  std::vector<query::VectorAggregateDefinition> definitions_;
  query::QueryResourceContext resources_;
  std::vector<std::byte> request_bytes_;
  DistributedVectorAggregateQuerySenderLimitsV2 limits_;
  DistributedQuerySenderState state_{DistributedQuerySenderState::kReady};
  std::size_t attempts_started_{};
  std::chrono::milliseconds next_backoff_{};
  std::optional<TimePoint> next_attempt_not_before_;
  std::optional<common::StatusCode> last_status_code_;
  std::optional<DistributedQueryLeaderHint> suggested_leader_;
  std::optional<std::vector<query::DistributedVectorAggregateExchangeMessage>> result_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_AGGREGATE_QUERY_TRANSPORT_V2_HPP_
