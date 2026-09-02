#ifndef CHRONOS_CLUSTER_DISTRIBUTED_QUERY_TRANSPORT_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_QUERY_TRANSPORT_HPP_

#include "chronos/cluster/remote_tablet_reconfiguration.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/query/distributed.hpp"
#include "chronos/query/distributed_fragment_dispatch.hpp"
#include "chronos/raft/types.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace chronos::cluster {

inline constexpr std::size_t kDistributedQueryRequestHeaderSize = 80U;
inline constexpr std::size_t kDistributedQueryRequestTrailerSize = 4U;
inline constexpr std::size_t kMaximumDistributedQueryRequestSize =
    kDistributedQueryRequestHeaderSize +
    query::distributed_fragment_dispatch_format::kMaximumFrameLength +
    kDistributedQueryRequestTrailerSize;
inline constexpr std::size_t kDistributedQueryResponseHeaderSize = 112U;
inline constexpr std::size_t kDistributedQueryResponseTrailerSize = 4U;
inline constexpr std::size_t kMaximumDistributedQueryResponseSize =
    kDistributedQueryResponseHeaderSize + query::distributed_format::kExchangeMessageLength +
    kDistributedQueryResponseTrailerSize;

struct DistributedQueryRequest {
  raft::NodeId source_node_id{};
  raft::NodeId target_node_id{};
  query::DistributedAggregateFragmentDispatch dispatch;
};

struct DistributedQueryLeaderHint {
  raft::NodeId node_id{};
  std::uint64_t placement_epoch{};

  friend bool operator==(const DistributedQueryLeaderHint&,
                         const DistributedQueryLeaderHint&) = default;
};

struct DistributedQueryResponse {
  raft::NodeId source_node_id{};
  raft::NodeId target_node_id{};
  common::Uuid query_id;
  schema::TabletId tablet_id;
  common::StatusCode status_code{common::StatusCode::kInternal};
  std::optional<query::ExchangeMessage> message{std::nullopt};
  std::optional<DistributedQueryLeaderHint> leader_hint{std::nullopt};
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_distributed_query_request_v1(const DistributedQueryRequest& request);
[[nodiscard]] common::Result<DistributedQueryRequest>
decode_distributed_query_request_v1(common::ByteView bytes);
[[nodiscard]] common::Result<std::vector<std::byte>>
encode_distributed_query_response_v1(const DistributedQueryResponse& response);
[[nodiscard]] common::Result<DistributedQueryResponse>
decode_distributed_query_response_v1(common::ByteView bytes);

// Embedding-owned execution boundary. Implementations acquire the current local barrier, placement,
// schema, and storage snapshot and call execute_distributed_aggregate_fragment. They must provide
// their own synchronization and outlive the receiver.
class DistributedQueryWorkerService {
public:
  virtual ~DistributedQueryWorkerService() = default;
  [[nodiscard]] virtual common::Result<query::ExchangeMessage>
  execute(const query::DistributedAggregateFragmentDispatch& dispatch) = 0;
};

// Embedding-owned committed metadata view used only after an authenticated, correlated worker
// reports that its authority is unavailable. Returned hints are advisory and never replace fresh
// admission and snapshot binding. Implementations provide their own synchronization.
class DistributedQueryLeaderHintProvider {
public:
  virtual ~DistributedQueryLeaderHintProvider() = default;
  [[nodiscard]] virtual common::Result<std::optional<DistributedQueryLeaderHint>>
  current_leader_hint(const schema::TabletId& tablet_id, const raft::GroupId& group_id) const = 0;
};

struct DistributedQueryReceiverConfig {
  raft::NodeId local_node_id{};
  const ClusterNodePrincipalAuthorizer* authorizer{};
  DistributedQueryWorkerService* worker{};
  const DistributedQueryLeaderHintProvider* leader_hint_provider{};
};

class DistributedQueryReceiver {
public:
  DistributedQueryReceiver() = delete;

  [[nodiscard]] static common::Result<DistributedQueryReceiver>
  create(DistributedQueryReceiverConfig config);

  // Authentication and claimed-source authorization precede worker invocation. Auth/codec/route
  // failures return directly; a worker result or failure is emitted as one correlated response.
  [[nodiscard]] common::Result<std::vector<std::byte>>
  receive(common::ByteView request_bytes,
          const network::PeerAuthenticationResult& authenticated_peer,
          std::optional<DistributedQueryLeaderHint> leader_hint = std::nullopt);

private:
  explicit DistributedQueryReceiver(DistributedQueryReceiverConfig config) noexcept;
  DistributedQueryReceiverConfig config_;
};

struct DistributedQueryRequestReadStep {
  std::size_t consumed_bytes{};
  std::optional<DistributedQueryRequest> request{std::nullopt};
};

// One-frame constant-storage stream reader. It integrity-checks the complete fixed header before
// trusting the declared length, consumes at most one frame, and leaves any coalesced suffix with
// the caller. Failure is sticky.
class DistributedQueryRequestReader {
public:
  DistributedQueryRequestReader() = default;
  DistributedQueryRequestReader(const DistributedQueryRequestReader&) = delete;
  DistributedQueryRequestReader& operator=(const DistributedQueryRequestReader&) = delete;
  DistributedQueryRequestReader(DistributedQueryRequestReader&&) = delete;
  DistributedQueryRequestReader& operator=(DistributedQueryRequestReader&&) = delete;

  [[nodiscard]] common::Result<DistributedQueryRequestReadStep> consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] std::optional<std::size_t> expected_frame_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  std::array<std::byte, kMaximumDistributedQueryRequestSize> bytes_{};
  std::size_t buffered_bytes_{};
  std::optional<std::size_t> expected_frame_bytes_;
  std::optional<common::Status> failure_;
};

struct DistributedQueryResponseReadStep {
  std::size_t consumed_bytes{};
  std::optional<DistributedQueryResponse> response{std::nullopt};
};

class DistributedQueryResponseReader {
public:
  DistributedQueryResponseReader() = default;
  DistributedQueryResponseReader(const DistributedQueryResponseReader&) = delete;
  DistributedQueryResponseReader& operator=(const DistributedQueryResponseReader&) = delete;
  DistributedQueryResponseReader(DistributedQueryResponseReader&&) = delete;
  DistributedQueryResponseReader& operator=(DistributedQueryResponseReader&&) = delete;

  [[nodiscard]] common::Result<DistributedQueryResponseReadStep> consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] std::optional<std::size_t> expected_frame_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  std::array<std::byte, kMaximumDistributedQueryResponseSize> bytes_{};
  std::size_t buffered_bytes_{};
  std::optional<std::size_t> expected_frame_bytes_;
  std::optional<common::Status> failure_;
};

// Validates and owns one already encoded request or response, then exposes only its unwritten
// suffix. Moving transfers the sole write obligation and leaves the source complete.
class DistributedQueryFrameWriteCursor {
public:
  DistributedQueryFrameWriteCursor() = delete;
  DistributedQueryFrameWriteCursor(const DistributedQueryFrameWriteCursor&) = delete;
  DistributedQueryFrameWriteCursor& operator=(const DistributedQueryFrameWriteCursor&) = delete;
  DistributedQueryFrameWriteCursor(DistributedQueryFrameWriteCursor&& other) noexcept;
  DistributedQueryFrameWriteCursor& operator=(DistributedQueryFrameWriteCursor&& other) noexcept;

  [[nodiscard]] static common::Result<DistributedQueryFrameWriteCursor>
  create(std::vector<std::byte> encoded_frame);
  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes) noexcept;
  [[nodiscard]] std::size_t written_bytes() const noexcept;
  [[nodiscard]] bool complete() const noexcept;

private:
  explicit DistributedQueryFrameWriteCursor(std::vector<std::byte> encoded_frame) noexcept;
  std::vector<std::byte> encoded_frame_;
  std::size_t written_bytes_{};
};

struct DistributedQueryRetryLimits {
  std::size_t maximum_attempts{5U};
  std::chrono::milliseconds initial_backoff{50};
  std::chrono::milliseconds maximum_backoff{5000};
};

enum class DistributedQuerySenderState : std::uint8_t {
  kReady = 1,
  kWaitingForResponse = 2,
  kBackoff = 3,
  kSucceeded = 4,
  kFailed = 5,
};

struct DistributedQueryAttempt {
  std::size_t attempt_number{};
  raft::NodeId target_node_id{};
  std::vector<std::byte> request_bytes;
};

// Single-owner deterministic retry state for one immutable proof-bound dispatch. It owns no socket
// or clock. Leader hints are advisory: changing authority requires explicit coordinator rebinding,
// never mutation of this sender's dispatch.
class DistributedQuerySender {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedQuerySender() = delete;
  DistributedQuerySender(const DistributedQuerySender&) = delete;
  DistributedQuerySender& operator=(const DistributedQuerySender&) = delete;
  DistributedQuerySender(DistributedQuerySender&&) noexcept = default;
  DistributedQuerySender& operator=(DistributedQuerySender&&) noexcept = default;

  [[nodiscard]] static common::Result<DistributedQuerySender>
  create(raft::NodeId source_node_id, query::DistributedAggregateFragmentDispatch dispatch,
         DistributedQueryRetryLimits limits = {});
  [[nodiscard]] common::Result<DistributedQueryAttempt> begin_attempt(TimePoint now);
  [[nodiscard]] common::Status accept_response(common::ByteView response_bytes, TimePoint now);
  [[nodiscard]] common::Status record_transport_failure(common::StatusCode code, TimePoint now);

  [[nodiscard]] DistributedQuerySenderState state() const noexcept;
  [[nodiscard]] std::size_t attempts_started() const noexcept;
  [[nodiscard]] std::optional<TimePoint> next_attempt_not_before() const noexcept;
  [[nodiscard]] std::optional<common::StatusCode> last_status_code() const noexcept;
  [[nodiscard]] std::optional<DistributedQueryLeaderHint> suggested_leader() const noexcept;
  [[nodiscard]] const std::optional<query::ExchangeMessage>& result() const noexcept;

private:
  DistributedQuerySender(raft::NodeId source_node_id,
                         query::DistributedAggregateFragmentDispatch dispatch,
                         DistributedQueryRetryLimits limits) noexcept;
  [[nodiscard]] common::Status schedule(common::StatusCode code, TimePoint now);

  raft::NodeId source_node_id_{};
  query::DistributedAggregateFragmentDispatch dispatch_;
  DistributedQueryRetryLimits limits_;
  DistributedQuerySenderState state_{DistributedQuerySenderState::kReady};
  std::size_t attempts_started_{};
  std::chrono::milliseconds next_backoff_{};
  std::optional<TimePoint> next_attempt_not_before_;
  std::optional<common::StatusCode> last_status_code_;
  std::optional<DistributedQueryLeaderHint> suggested_leader_;
  std::optional<query::ExchangeMessage> result_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_QUERY_TRANSPORT_HPP_
