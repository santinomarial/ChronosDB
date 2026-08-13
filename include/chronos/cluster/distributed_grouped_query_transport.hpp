#ifndef CHRONOS_CLUSTER_DISTRIBUTED_GROUPED_QUERY_TRANSPORT_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_GROUPED_QUERY_TRANSPORT_HPP_

#include "chronos/cluster/distributed_query_transport.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/distributed_fragment_dispatch.hpp"
#include "chronos/query/distributed_grouped_exchange.hpp"
#include "chronos/raft/types.hpp"

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

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_GROUPED_QUERY_TRANSPORT_HPP_
