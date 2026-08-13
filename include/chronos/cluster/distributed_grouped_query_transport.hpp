#ifndef CHRONOS_CLUSTER_DISTRIBUTED_GROUPED_QUERY_TRANSPORT_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_GROUPED_QUERY_TRANSPORT_HPP_

#include "chronos/cluster/distributed_query_transport.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/distributed_fragment_dispatch.hpp"
#include "chronos/query/distributed_fragment_worker.hpp"
#include "chronos/query/distributed_grouped_exchange.hpp"
#include "chronos/raft/types.hpp"

#include <array>
#include <cstddef>
#include <optional>
#include <variant>
#include <vector>

namespace chronos::cluster {

inline constexpr std::size_t kDistributedGroupedQueryRequestHeaderSize = 80U;
inline constexpr std::size_t kDistributedGroupedQueryRequestTrailerSize = 4U;
inline constexpr std::size_t kMaximumDistributedGroupedQueryRequestSize =
    kDistributedGroupedQueryRequestHeaderSize +
    query::distributed_grouped_float64_fragment_dispatch_format::kMaximumFrameLength +
    kDistributedGroupedQueryRequestTrailerSize;
inline constexpr std::size_t kDistributedGroupedQueryResponseHeaderSize = 112U;
inline constexpr std::size_t kDistributedGroupedQueryResponseTrailerSize = 4U;
inline constexpr std::size_t kMaximumDistributedGroupedQueryResponseSize =
    kDistributedGroupedQueryResponseHeaderSize +
    query::grouped_float64_exchange_format::kFrameLength +
    kDistributedGroupedQueryResponseTrailerSize;

struct DistributedGroupedQueryRequest {
  raft::NodeId source_node_id{};
  raft::NodeId target_node_id{};
  query::DistributedGroupedFloat64FragmentDispatch dispatch;
};

using DistributedGroupedQueryResponsePayload =
    std::variant<query::GroupedFloat64ExchangeMessage, query::GroupedExchangeTerminalMessage>;

struct DistributedGroupedQueryResponse {
  raft::NodeId source_node_id{};
  raft::NodeId target_node_id{};
  common::Uuid query_id;
  schema::TabletId tablet_id;
  common::StatusCode status_code{common::StatusCode::kInternal};
  std::optional<DistributedGroupedQueryResponsePayload> payload;
  std::optional<DistributedQueryLeaderHint> leader_hint;
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_distributed_grouped_query_request_v1(const DistributedGroupedQueryRequest& request);
[[nodiscard]] common::Result<DistributedGroupedQueryRequest>
decode_distributed_grouped_query_request_v1(common::ByteView bytes);
[[nodiscard]] common::Result<std::vector<std::byte>>
encode_distributed_grouped_query_response_v1(const DistributedGroupedQueryResponse& response);
[[nodiscard]] common::Result<DistributedGroupedQueryResponse>
decode_distributed_grouped_query_response_v1(common::ByteView bytes);

struct DistributedGroupedQueryRequestReadStep {
  std::size_t consumed_bytes{};
  std::optional<DistributedGroupedQueryRequest> request;
};

// One-frame fixed-storage reader. The complete header is integrity-checked before its bounded total
// length controls further input. A coalesced successor remains caller-owned and failure is sticky.
class DistributedGroupedQueryRequestReader {
public:
  DistributedGroupedQueryRequestReader() = default;
  DistributedGroupedQueryRequestReader(const DistributedGroupedQueryRequestReader&) = delete;
  DistributedGroupedQueryRequestReader&
  operator=(const DistributedGroupedQueryRequestReader&) = delete;
  DistributedGroupedQueryRequestReader(DistributedGroupedQueryRequestReader&&) = delete;
  DistributedGroupedQueryRequestReader& operator=(DistributedGroupedQueryRequestReader&&) = delete;

  [[nodiscard]] common::Result<DistributedGroupedQueryRequestReadStep>
  consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] std::optional<std::size_t> expected_frame_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  std::array<std::byte, kMaximumDistributedGroupedQueryRequestSize> bytes_{};
  std::size_t buffered_bytes_{};
  std::optional<std::size_t> expected_frame_bytes_;
  std::optional<common::Status> failure_;
};

struct DistributedGroupedQueryResponseReadStep {
  std::size_t consumed_bytes{};
  std::optional<DistributedGroupedQueryResponse> response;
};

class DistributedGroupedQueryResponseReader {
public:
  DistributedGroupedQueryResponseReader() = default;
  DistributedGroupedQueryResponseReader(const DistributedGroupedQueryResponseReader&) = delete;
  DistributedGroupedQueryResponseReader&
  operator=(const DistributedGroupedQueryResponseReader&) = delete;
  DistributedGroupedQueryResponseReader(DistributedGroupedQueryResponseReader&&) = delete;
  DistributedGroupedQueryResponseReader&
  operator=(DistributedGroupedQueryResponseReader&&) = delete;

  [[nodiscard]] common::Result<DistributedGroupedQueryResponseReadStep>
  consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] std::optional<std::size_t> expected_frame_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  std::array<std::byte, kMaximumDistributedGroupedQueryResponseSize> bytes_{};
  std::size_t buffered_bytes_{};
  std::optional<std::size_t> expected_frame_bytes_;
  std::optional<common::Status> failure_;
};

// Validates and owns one grouped request or response, exposing only its unwritten suffix. Moving
// transfers the sole write obligation and leaves the source complete.
class DistributedGroupedQueryFrameWriteCursor {
public:
  DistributedGroupedQueryFrameWriteCursor() = delete;
  DistributedGroupedQueryFrameWriteCursor(const DistributedGroupedQueryFrameWriteCursor&) = delete;
  DistributedGroupedQueryFrameWriteCursor&
  operator=(const DistributedGroupedQueryFrameWriteCursor&) = delete;
  DistributedGroupedQueryFrameWriteCursor(DistributedGroupedQueryFrameWriteCursor&& other) noexcept;
  DistributedGroupedQueryFrameWriteCursor&
  operator=(DistributedGroupedQueryFrameWriteCursor&& other) noexcept;

  [[nodiscard]] static common::Result<DistributedGroupedQueryFrameWriteCursor>
  create(std::vector<std::byte> encoded_frame);
  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes) noexcept;
  [[nodiscard]] std::size_t written_bytes() const noexcept;
  [[nodiscard]] bool complete() const noexcept;

private:
  explicit DistributedGroupedQueryFrameWriteCursor(std::vector<std::byte> encoded_frame) noexcept;
  std::vector<std::byte> encoded_frame_;
  std::size_t written_bytes_{};
};

// Embedding-owned synchronous grouped execution boundary. Implementations acquire current local
// authority and invoke the proof-revalidating grouped worker. They must outlive the receiver.
class DistributedGroupedQueryWorkerService {
public:
  virtual ~DistributedGroupedQueryWorkerService() = default;
  [[nodiscard]] virtual common::Result<query::DistributedGroupedFloat64WorkerResult>
  execute(const query::DistributedGroupedFloat64FragmentDispatch& dispatch) = 0;
};

struct DistributedGroupedQueryReceiverConfig {
  raft::NodeId local_node_id{};
  const ClusterNodePrincipalAuthorizer* authorizer{};
  DistributedGroupedQueryWorkerService* worker{};
  const DistributedQueryLeaderHintProvider* leader_hint_provider{};
  std::size_t maximum_response_frames{1024U};
};

// Authenticates and authorizes before worker invocation, validates the complete grouped worker
// stream, then publishes all encoded response frames together. No partial response vector escapes.
class DistributedGroupedQueryReceiver {
public:
  DistributedGroupedQueryReceiver() = delete;

  [[nodiscard]] static common::Result<DistributedGroupedQueryReceiver>
  create(DistributedGroupedQueryReceiverConfig config);
  [[nodiscard]] common::Result<std::vector<std::vector<std::byte>>>
  receive(common::ByteView request_bytes,
          const network::PeerAuthenticationResult& authenticated_peer);

private:
  explicit DistributedGroupedQueryReceiver(DistributedGroupedQueryReceiverConfig config) noexcept;
  DistributedGroupedQueryReceiverConfig config_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_GROUPED_QUERY_TRANSPORT_HPP_
