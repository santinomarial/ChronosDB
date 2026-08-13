#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_QUERY_TRANSPORT_V2_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_QUERY_TRANSPORT_V2_HPP_

#include "chronos/cluster/distributed_query_transport.hpp"
#include "chronos/cluster/distributed_vector_result_exchange.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/distributed_vector_fragment_v2.hpp"
#include "chronos/raft/types.hpp"

#include <array>
#include <cstddef>
#include <optional>
#include <vector>

namespace chronos::cluster {

inline constexpr std::size_t kDistributedVectorQueryRequestV2HeaderSize = 80U;
inline constexpr std::size_t kDistributedVectorQueryRequestV2TrailerSize = 4U;
inline constexpr std::size_t kMaximumDistributedVectorQueryRequestV2Size =
    kDistributedVectorQueryRequestV2HeaderSize +
    query::distributed_vector_fragment_v2_format::kMaximumFrameLength +
    kDistributedVectorQueryRequestV2TrailerSize;
inline constexpr std::size_t kDistributedVectorQueryResponseV2HeaderSize = 112U;
inline constexpr std::size_t kDistributedVectorQueryResponseV2TrailerSize = 4U;
inline constexpr std::size_t kMaximumDistributedVectorQueryResponseV2Size =
    kDistributedVectorQueryResponseV2HeaderSize +
    distributed_vector_result_exchange_v2_format::kMaximumFrameLength +
    kDistributedVectorQueryResponseV2TrailerSize;

struct DistributedVectorQueryRequestV2 {
  raft::NodeId source_node_id{};
  raft::NodeId target_node_id{};
  query::DistributedVectorFragmentDispatchV2 dispatch;

  friend bool operator==(const DistributedVectorQueryRequestV2&,
                         const DistributedVectorQueryRequestV2&) = default;
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_distributed_vector_query_request_v2(const DistributedVectorQueryRequestV2& request);
[[nodiscard]] common::Result<DistributedVectorQueryRequestV2>
decode_distributed_vector_query_request_v2_exact(common::ByteView bytes);

struct DistributedVectorQueryResponseV2 {
  raft::NodeId source_node_id{};
  raft::NodeId target_node_id{};
  common::Uuid query_id;
  schema::TabletId tablet_id;
  common::StatusCode status_code{common::StatusCode::kInternal};
  std::optional<DistributedVectorResultExchangeMessage> payload;
  std::optional<DistributedQueryLeaderHint> leader_hint;
};

[[nodiscard]] common::Result<std::vector<std::byte>> encode_distributed_vector_query_response_v2(
    const DistributedVectorQueryResponseV2& response,
    const query::DistributedVectorResultSchema& expected_schema);
[[nodiscard]] common::Result<DistributedVectorQueryResponseV2>
decode_distributed_vector_query_response_v2_exact(
    common::ByteView bytes, const query::DistributedVectorResultSchema& expected_schema);

struct DistributedVectorQueryRequestV2ReadStep {
  std::size_t consumed_bytes{};
  std::optional<DistributedVectorQueryRequestV2> request;
};

class DistributedVectorQueryRequestV2Reader {
public:
  explicit DistributedVectorQueryRequestV2Reader(
      std::size_t maximum_frame_length = kMaximumDistributedVectorQueryRequestV2Size) noexcept;
  DistributedVectorQueryRequestV2Reader(const DistributedVectorQueryRequestV2Reader&) = delete;
  DistributedVectorQueryRequestV2Reader&
  operator=(const DistributedVectorQueryRequestV2Reader&) = delete;
  DistributedVectorQueryRequestV2Reader(DistributedVectorQueryRequestV2Reader&&) = delete;
  DistributedVectorQueryRequestV2Reader&
  operator=(DistributedVectorQueryRequestV2Reader&&) = delete;

  [[nodiscard]] common::Result<DistributedVectorQueryRequestV2ReadStep>
  consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  std::size_t maximum_frame_length_{};
  std::array<std::byte, kDistributedVectorQueryRequestV2HeaderSize> header_{};
  std::size_t header_bytes_{};
  std::vector<std::byte> frame_;
  std::size_t frame_bytes_{};
  std::optional<common::Status> failure_;
};

struct DistributedVectorQueryResponseV2ReadStep {
  std::size_t consumed_bytes{};
  std::optional<DistributedVectorQueryResponseV2> response;
};

// The caller transfers the admitted Fragment-v2 result schema into the connection reader. One
// owner serializes consume calls; coalesced successor bytes remain caller-owned.
class DistributedVectorQueryResponseV2Reader {
public:
  explicit DistributedVectorQueryResponseV2Reader(
      query::DistributedVectorResultSchema&& expected_schema,
      std::size_t maximum_frame_length = kMaximumDistributedVectorQueryResponseV2Size) noexcept;
  DistributedVectorQueryResponseV2Reader(const DistributedVectorQueryResponseV2Reader&) = delete;
  DistributedVectorQueryResponseV2Reader&
  operator=(const DistributedVectorQueryResponseV2Reader&) = delete;
  DistributedVectorQueryResponseV2Reader(DistributedVectorQueryResponseV2Reader&&) = delete;
  DistributedVectorQueryResponseV2Reader&
  operator=(DistributedVectorQueryResponseV2Reader&&) = delete;

  [[nodiscard]] common::Result<DistributedVectorQueryResponseV2ReadStep>
  consume(common::ByteView bytes);
  [[nodiscard]] std::size_t buffered_bytes() const noexcept;
  [[nodiscard]] bool failed() const noexcept;

private:
  query::DistributedVectorResultSchema expected_schema_;
  std::size_t maximum_frame_length_{};
  std::array<std::byte, kDistributedVectorQueryResponseV2HeaderSize> header_{};
  std::size_t header_bytes_{};
  std::vector<std::byte> frame_;
  std::size_t frame_bytes_{};
  std::optional<common::Status> failure_;
};

// Typed construction prevents a v2 response from being accepted without its expected schema.
class DistributedVectorQueryFrameV2WriteCursor {
public:
  DistributedVectorQueryFrameV2WriteCursor() = delete;
  DistributedVectorQueryFrameV2WriteCursor(const DistributedVectorQueryFrameV2WriteCursor&) =
      delete;
  DistributedVectorQueryFrameV2WriteCursor&
  operator=(const DistributedVectorQueryFrameV2WriteCursor&) = delete;
  DistributedVectorQueryFrameV2WriteCursor(
      DistributedVectorQueryFrameV2WriteCursor&& other) noexcept;
  DistributedVectorQueryFrameV2WriteCursor&
  operator=(DistributedVectorQueryFrameV2WriteCursor&& other) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorQueryFrameV2WriteCursor>
  create_request(const DistributedVectorQueryRequestV2& request);
  [[nodiscard]] static common::Result<DistributedVectorQueryFrameV2WriteCursor>
  create_response(const DistributedVectorQueryResponseV2& response,
                  const query::DistributedVectorResultSchema& expected_schema);
  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes) noexcept;
  [[nodiscard]] std::size_t written_bytes() const noexcept;
  [[nodiscard]] bool complete() const noexcept;

private:
  explicit DistributedVectorQueryFrameV2WriteCursor(std::vector<std::byte> encoded_frame) noexcept;
  std::vector<std::byte> encoded_frame_;
  std::size_t written_bytes_{};
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_QUERY_TRANSPORT_V2_HPP_
