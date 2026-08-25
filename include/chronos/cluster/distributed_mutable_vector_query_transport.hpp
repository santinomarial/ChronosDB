#ifndef CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_QUERY_TRANSPORT_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_QUERY_TRANSPORT_HPP_

#include "chronos/cluster/distributed_vector_query_transport_v2.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/distributed_mutable_vector_fragment.hpp"
#include "chronos/raft/types.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <optional>
#include <span>
#include <vector>

namespace chronos::cluster {

inline constexpr std::array<std::byte, 8U> kDistributedMutableVectorQueryRequestMagicV1{
    std::byte{'C'}, std::byte{'H'}, std::byte{'D'}, std::byte{'M'},
    std::byte{'R'}, std::byte{'E'}, std::byte{'Q'}, std::byte{'1'}};
inline constexpr std::size_t kDistributedMutableVectorQueryRequestHeaderSize = 80U;
inline constexpr std::size_t kDistributedMutableVectorQueryRequestTrailerSize = 4U;
inline constexpr std::size_t kMaximumDistributedMutableVectorQueryRequestSize =
    kDistributedMutableVectorQueryRequestHeaderSize +
    query::distributed_mutable_vector_fragment_format::kMaximumFrameLength +
    kDistributedMutableVectorQueryRequestTrailerSize;

struct DistributedMutableVectorQueryRequest {
  raft::NodeId source_node_id{};
  raft::NodeId target_node_id{};
  query::DistributedMutableVectorFragment fragment;

  friend bool operator==(const DistributedMutableVectorQueryRequest&,
                         const DistributedMutableVectorQueryRequest&) = default;
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_distributed_mutable_vector_query_request(
    const DistributedMutableVectorQueryRequest& request);
[[nodiscard]] common::Result<DistributedMutableVectorQueryRequest>
decode_distributed_mutable_vector_query_request_exact(common::ByteView bytes);

struct DistributedMutableVectorQueryRequestReadStep {
  std::size_t consumed_bytes{};
  std::optional<DistributedMutableVectorQueryRequest> request;
};

// Owns at most one bounded request frame. One connection thread serializes consume calls;
// coalesced successor bytes remain caller-owned.
class DistributedMutableVectorQueryRequestReader {
public:
  explicit DistributedMutableVectorQueryRequestReader(
      std::size_t maximum_frame_length = kMaximumDistributedMutableVectorQueryRequestSize) noexcept;
  DistributedMutableVectorQueryRequestReader(const DistributedMutableVectorQueryRequestReader&) =
      delete;
  DistributedMutableVectorQueryRequestReader&
  operator=(const DistributedMutableVectorQueryRequestReader&) = delete;
  DistributedMutableVectorQueryRequestReader(DistributedMutableVectorQueryRequestReader&&) = delete;
  DistributedMutableVectorQueryRequestReader&
  operator=(DistributedMutableVectorQueryRequestReader&&) = delete;

  [[nodiscard]] common::Result<DistributedMutableVectorQueryRequestReadStep>
  consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  std::size_t maximum_frame_length_{};
  std::array<std::byte, kDistributedMutableVectorQueryRequestHeaderSize> header_{};
  std::size_t header_bytes_{};
  std::vector<std::byte> frame_;
  std::size_t frame_bytes_{};
  std::optional<common::Status> failure_;
};

class DistributedMutableVectorQueryRequestWriteCursor {
public:
  DistributedMutableVectorQueryRequestWriteCursor() = delete;
  DistributedMutableVectorQueryRequestWriteCursor(
      const DistributedMutableVectorQueryRequestWriteCursor&) = delete;
  DistributedMutableVectorQueryRequestWriteCursor&
  operator=(const DistributedMutableVectorQueryRequestWriteCursor&) = delete;
  DistributedMutableVectorQueryRequestWriteCursor(
      DistributedMutableVectorQueryRequestWriteCursor&& other) noexcept;
  DistributedMutableVectorQueryRequestWriteCursor&
  operator=(DistributedMutableVectorQueryRequestWriteCursor&& other) noexcept;

  [[nodiscard]] static common::Result<DistributedMutableVectorQueryRequestWriteCursor>
  create(const DistributedMutableVectorQueryRequest& request);
  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes) noexcept;
  [[nodiscard]] std::size_t written_bytes() const noexcept;
  [[nodiscard]] bool complete() const noexcept;

private:
  explicit DistributedMutableVectorQueryRequestWriteCursor(
      std::vector<std::byte> encoded_frame) noexcept;
  std::vector<std::byte> encoded_frame_;
  std::size_t written_bytes_{};
};

// Embedding-owned execution boundary. The implementation must reacquire current local authority
// and return one complete schema-bound terminal stream. It must outlive the receiver.
class DistributedMutableVectorQueryWorkerService {
public:
  virtual ~DistributedMutableVectorQueryWorkerService() = default;
  [[nodiscard]] virtual common::Result<std::vector<DistributedVectorResultExchangeMessage>>
  execute(const query::DistributedMutableVectorFragment& fragment) = 0;
};

struct DistributedMutableVectorQueryReceiverConfig {
  raft::NodeId local_node_id{};
  const ClusterNodePrincipalAuthorizer* authorizer{};
  DistributedMutableVectorQueryWorkerService* worker{};
  const DistributedQueryLeaderHintProvider* leader_hint_provider{};
  std::size_t maximum_response_frames{1024U};
  std::size_t maximum_response_bytes{kDefaultDistributedVectorQueryV2ResponseBytes};
};

// The authenticated peer is authorized before worker execution. Returned bytes reuse the existing
// schema-bound v2 response frame; the request retains distinct mutable authority and wire magic.
class DistributedMutableVectorQueryReceiver {
public:
  DistributedMutableVectorQueryReceiver() = delete;

  [[nodiscard]] static common::Result<DistributedMutableVectorQueryReceiver>
  create(DistributedMutableVectorQueryReceiverConfig config);
  [[nodiscard]] common::Result<std::vector<std::vector<std::byte>>>
  receive(common::ByteView request_bytes,
          const network::PeerAuthenticationResult& authenticated_peer);

private:
  explicit DistributedMutableVectorQueryReceiver(
      DistributedMutableVectorQueryReceiverConfig config) noexcept;
  DistributedMutableVectorQueryReceiverConfig config_;
};

struct DistributedMutableVectorQueryAttempt {
  std::size_t attempt_number{};
  raft::NodeId target_node_id{};
  std::vector<std::byte> request_bytes;
};

struct DistributedMutableVectorQuerySenderLimits {
  DistributedQueryRetryLimits retry;
  std::size_t maximum_response_frames{1024U};
  std::size_t maximum_response_bytes{kDefaultDistributedVectorQueryV2ResponseBytes};
};

// Finite retry owner for one immutable proof-bound mutable fragment. Accepted output is retained
// only after the complete correlated schema-valid terminal response stream arrives.
class DistributedMutableVectorQuerySender {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedMutableVectorQuerySender() = delete;
  DistributedMutableVectorQuerySender(const DistributedMutableVectorQuerySender&) = delete;
  DistributedMutableVectorQuerySender&
  operator=(const DistributedMutableVectorQuerySender&) = delete;
  DistributedMutableVectorQuerySender(DistributedMutableVectorQuerySender&&) noexcept = default;
  DistributedMutableVectorQuerySender&
  operator=(DistributedMutableVectorQuerySender&&) noexcept = default;

  [[nodiscard]] static common::Result<DistributedMutableVectorQuerySender>
  create(raft::NodeId source_node_id, query::DistributedMutableVectorFragment fragment,
         DistributedMutableVectorQuerySenderLimits limits = {});
  [[nodiscard]] common::Result<DistributedMutableVectorQueryAttempt> begin_attempt(TimePoint now);
  [[nodiscard]] common::Status
  accept_responses(std::span<const DistributedVectorQueryResponseV2> responses, TimePoint now);
  [[nodiscard]] common::Status record_transport_failure(common::StatusCode code, TimePoint now);

  [[nodiscard]] DistributedQuerySenderState state() const noexcept;
  [[nodiscard]] std::size_t attempts_started() const noexcept;
  [[nodiscard]] std::optional<TimePoint> next_attempt_not_before() const noexcept;
  [[nodiscard]] std::optional<common::StatusCode> last_status_code() const noexcept;
  [[nodiscard]] std::optional<DistributedQueryLeaderHint> suggested_leader() const noexcept;
  [[nodiscard]] const std::optional<std::vector<DistributedVectorResultExchangeMessage>>&
  result() const noexcept;

private:
  DistributedMutableVectorQuerySender(raft::NodeId source_node_id,
                                      query::DistributedMutableVectorFragment fragment,
                                      DistributedMutableVectorQuerySenderLimits limits) noexcept;
  [[nodiscard]] common::Status schedule(common::StatusCode code, TimePoint now);

  raft::NodeId source_node_id_{};
  query::DistributedMutableVectorFragment fragment_;
  DistributedMutableVectorQuerySenderLimits limits_;
  DistributedQuerySenderState state_{DistributedQuerySenderState::kReady};
  std::size_t attempts_started_{};
  std::chrono::milliseconds next_backoff_{};
  std::optional<TimePoint> next_attempt_not_before_;
  std::optional<common::StatusCode> last_status_code_;
  std::optional<DistributedQueryLeaderHint> suggested_leader_;
  std::optional<std::vector<DistributedVectorResultExchangeMessage>> result_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_MUTABLE_VECTOR_QUERY_TRANSPORT_HPP_
