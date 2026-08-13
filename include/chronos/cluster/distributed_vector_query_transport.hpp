#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_QUERY_TRANSPORT_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_QUERY_TRANSPORT_HPP_

#include "chronos/cluster/distributed_query_transport.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/distributed_vector_exchange.hpp"
#include "chronos/query/distributed_vector_fragment.hpp"
#include "chronos/raft/types.hpp"

#include <array>
#include <cstddef>
#include <optional>
#include <vector>

namespace chronos::cluster {

inline constexpr std::size_t kDistributedVectorQueryRequestHeaderSize = 80U;
inline constexpr std::size_t kDistributedVectorQueryRequestTrailerSize = 4U;
inline constexpr std::size_t kMaximumDistributedVectorQueryRequestSize =
    kDistributedVectorQueryRequestHeaderSize +
    query::distributed_vector_fragment_format::kMaximumFrameLength +
    kDistributedVectorQueryRequestTrailerSize;
inline constexpr std::size_t kDistributedVectorQueryResponseHeaderSize = 112U;
inline constexpr std::size_t kDistributedVectorQueryResponseTrailerSize = 4U;
inline constexpr std::size_t kMaximumDistributedVectorQueryResponseSize =
    kDistributedVectorQueryResponseHeaderSize +
    query::distributed_vector_exchange_format::kMaximumFrameLength +
    kDistributedVectorQueryResponseTrailerSize;

struct DistributedVectorQueryRequest {
  raft::NodeId source_node_id{};
  raft::NodeId target_node_id{};
  query::DistributedVectorFragmentDispatch dispatch;

  friend bool operator==(const DistributedVectorQueryRequest&,
                         const DistributedVectorQueryRequest&) = default;
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_distributed_vector_query_request_v1(const DistributedVectorQueryRequest& request);

[[nodiscard]] common::Result<DistributedVectorQueryRequest>
decode_distributed_vector_query_request_v1(common::ByteView bytes);

struct DistributedVectorQueryResponse {
  raft::NodeId source_node_id{};
  raft::NodeId target_node_id{};
  common::Uuid query_id;
  schema::TabletId tablet_id;
  common::StatusCode status_code{common::StatusCode::kInternal};
  std::optional<query::DistributedVectorExchangeMessage> payload;
  std::optional<DistributedQueryLeaderHint> leader_hint;
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_distributed_vector_query_response_v1(const DistributedVectorQueryResponse& response);

[[nodiscard]] common::Result<DistributedVectorQueryResponse>
decode_distributed_vector_query_response_v1(common::ByteView bytes);

struct DistributedVectorQueryRequestReadStep {
  std::size_t consumed_bytes{};
  std::optional<DistributedVectorQueryRequest> request;
};

class DistributedVectorQueryRequestReader {
public:
  explicit DistributedVectorQueryRequestReader(
      std::size_t maximum_frame_length = kMaximumDistributedVectorQueryRequestSize);
  DistributedVectorQueryRequestReader(const DistributedVectorQueryRequestReader&) = delete;
  DistributedVectorQueryRequestReader&
  operator=(const DistributedVectorQueryRequestReader&) = delete;
  DistributedVectorQueryRequestReader(DistributedVectorQueryRequestReader&&) = delete;
  DistributedVectorQueryRequestReader& operator=(DistributedVectorQueryRequestReader&&) = delete;

  [[nodiscard]] common::Result<DistributedVectorQueryRequestReadStep>
  consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  std::size_t maximum_frame_length_{};
  std::array<std::byte, kDistributedVectorQueryRequestHeaderSize> header_{};
  std::size_t header_bytes_{};
  std::vector<std::byte> frame_;
  std::size_t frame_bytes_{};
  std::optional<common::Status> failure_;
};

struct DistributedVectorQueryResponseReadStep {
  std::size_t consumed_bytes{};
  std::optional<DistributedVectorQueryResponse> response;
};

class DistributedVectorQueryResponseReader {
public:
  explicit DistributedVectorQueryResponseReader(
      std::size_t maximum_frame_length = kMaximumDistributedVectorQueryResponseSize);
  DistributedVectorQueryResponseReader(const DistributedVectorQueryResponseReader&) = delete;
  DistributedVectorQueryResponseReader&
  operator=(const DistributedVectorQueryResponseReader&) = delete;
  DistributedVectorQueryResponseReader(DistributedVectorQueryResponseReader&&) = delete;
  DistributedVectorQueryResponseReader& operator=(DistributedVectorQueryResponseReader&&) = delete;

  [[nodiscard]] common::Result<DistributedVectorQueryResponseReadStep>
  consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  std::size_t maximum_frame_length_{};
  std::array<std::byte, kDistributedVectorQueryResponseHeaderSize> header_{};
  std::size_t header_bytes_{};
  std::vector<std::byte> frame_;
  std::size_t frame_bytes_{};
  std::optional<common::Status> failure_;
};

class DistributedVectorQueryFrameWriteCursor {
public:
  DistributedVectorQueryFrameWriteCursor() = delete;
  DistributedVectorQueryFrameWriteCursor(const DistributedVectorQueryFrameWriteCursor&) = delete;
  DistributedVectorQueryFrameWriteCursor&
  operator=(const DistributedVectorQueryFrameWriteCursor&) = delete;
  DistributedVectorQueryFrameWriteCursor(DistributedVectorQueryFrameWriteCursor&& other) noexcept;
  DistributedVectorQueryFrameWriteCursor&
  operator=(DistributedVectorQueryFrameWriteCursor&& other) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorQueryFrameWriteCursor>
  create(std::vector<std::byte> encoded_frame);
  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes) noexcept;
  [[nodiscard]] std::size_t written_bytes() const noexcept;
  [[nodiscard]] bool complete() const noexcept;

private:
  explicit DistributedVectorQueryFrameWriteCursor(std::vector<std::byte> encoded_frame) noexcept;
  std::vector<std::byte> encoded_frame_;
  std::size_t written_bytes_{};
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_QUERY_TRANSPORT_HPP_
