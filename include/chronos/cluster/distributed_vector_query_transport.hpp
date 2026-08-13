#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_QUERY_TRANSPORT_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_QUERY_TRANSPORT_HPP_

#include "chronos/cluster/distributed_query_transport.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/distributed_vector_exchange.hpp"
#include "chronos/query/distributed_vector_fragment.hpp"
#include "chronos/raft/types.hpp"

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

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_QUERY_TRANSPORT_HPP_
