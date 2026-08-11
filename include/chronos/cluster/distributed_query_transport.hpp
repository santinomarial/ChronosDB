#ifndef CHRONOS_CLUSTER_DISTRIBUTED_QUERY_TRANSPORT_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_QUERY_TRANSPORT_HPP_

#include "chronos/cluster/remote_tablet_reconfiguration.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/query/distributed.hpp"
#include "chronos/query/distributed_fragment_dispatch.hpp"
#include "chronos/raft/types.hpp"

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
  std::optional<query::ExchangeMessage> message;
  std::optional<DistributedQueryLeaderHint> leader_hint;
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

struct DistributedQueryReceiverConfig {
  raft::NodeId local_node_id{};
  const ClusterNodePrincipalAuthorizer* authorizer{};
  DistributedQueryWorkerService* worker{};
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

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_QUERY_TRANSPORT_HPP_
