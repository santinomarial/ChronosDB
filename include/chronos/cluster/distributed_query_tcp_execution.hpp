#ifndef CHRONOS_CLUSTER_DISTRIBUTED_QUERY_TCP_EXECUTION_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_QUERY_TCP_EXECUTION_HPP_

#include "chronos/cluster/distributed_query_execution.hpp"
#include "chronos/cluster/distributed_query_tcp_client.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"
#include "chronos/query/distributed.hpp"
#include "chronos/query/distributed_vector_fragment.hpp"
#include "chronos/raft/metadata.hpp"
#include "chronos/raft/types.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace chronos::cluster {

struct DistributedQueryNodeRoute {
  raft::NodeId node_id{};
  std::vector<network::Ipv4Endpoint> endpoints;
  const network::TlsClientContext* tls_context{};
};

struct DistributedQueryNodeTlsContext {
  raft::NodeId node_id{};
  const network::TlsClientContext* tls_context{};
};

struct DistributedQueryRouteResolutionLimits {
  std::size_t maximum_routes{65'536U};
  std::size_t maximum_endpoint_bytes{raft::MetadataLimits{}.maximum_endpoint_bytes};
  std::size_t maximum_addresses_per_route{16U};
};

// Resolves only serving nodes named by the immutable dispatches. The committed catalog and TLS
// context spans must be canonical by node ID. Selected lowercase DNS endpoints are resolved into a
// fresh bounded ordered IPv4 candidate set before execution; this blocking system lookup must not
// run on the execution's event-loop thread.
[[nodiscard]] common::Result<std::vector<DistributedQueryNodeRoute>>
resolve_distributed_query_node_routes(
    const raft::MetadataCatalogSnapshot& catalog,
    std::span<const query::DistributedAggregateFragmentDispatch> dispatches,
    std::span<const DistributedQueryNodeTlsContext> tls_contexts,
    DistributedQueryRouteResolutionLimits limits = {});

// Applies the identical committed node/TLS join to the immutable dispatches retained by a
// CompatibleDistributedVectorSnapshotV2 owner.
[[nodiscard]] common::Result<std::vector<DistributedQueryNodeRoute>>
resolve_distributed_query_node_routes(
    const raft::MetadataCatalogSnapshot& catalog,
    std::span<const query::DistributedVectorFragmentDispatch> dispatches,
    std::span<const DistributedQueryNodeTlsContext> tls_contexts,
    DistributedQueryRouteResolutionLimits limits = {});

struct DistributedQueryTcpExecutionConfig {
  network::ConnectionAuthenticator* authenticator{};
  const ClusterNodePrincipalAuthorizer* node_authorizer{};
  std::vector<DistributedQueryNodeRoute> routes;
  DistributedQueryTlsClientLimits carrier_limits;
  std::chrono::milliseconds connect_timeout{5000};
  std::optional<std::chrono::steady_clock::time_point> execution_deadline;
  std::size_t maximum_rebindings{3U};
};

struct DistributedQueryTcpExecutionMetrics {
  std::uint64_t attempts_started{};
  std::uint64_t retries_started{};
  std::uint64_t transport_completed_attempts{};
  std::uint64_t transport_failed_attempts{};
  std::uint64_t rebindings_started{};
  std::size_t active_attempts{};
};

enum class DistributedQueryTcpExecutionState : std::uint8_t {
  kRunning = 1,
  kComplete = 2,
  kFailed = 3,
  kCancelled = 4,
};

// Single-threaded poll owner for one compatible multi-tablet execution. It owns the execution and
// therefore its pinned Manifest epoch. Authentication policy and route TLS contexts are borrowed
// and must outlive this owner. Route targets are immutable for the execution; leader hints require
// explicit rebinding into a new execution.
class DistributedQueryTcpExecution {
public:
  DistributedQueryTcpExecution() noexcept;
  ~DistributedQueryTcpExecution();
  DistributedQueryTcpExecution(const DistributedQueryTcpExecution&) = delete;
  DistributedQueryTcpExecution& operator=(const DistributedQueryTcpExecution&) = delete;
  DistributedQueryTcpExecution(DistributedQueryTcpExecution&&) noexcept;
  DistributedQueryTcpExecution& operator=(DistributedQueryTcpExecution&&) noexcept;

  [[nodiscard]] static common::Result<DistributedQueryTcpExecution>
  create(DistributedQueryExecution execution, DistributedQueryTcpExecutionConfig config);
  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status cancel();
  [[nodiscard]] common::Status rebind(DistributedQueryExecution execution,
                                      DistributedQueryTcpExecutionConfig config);

  [[nodiscard]] DistributedQueryTcpExecutionState state() const noexcept;
  [[nodiscard]] DistributedQueryTcpExecutionMetrics metrics() const noexcept;
  [[nodiscard]] common::Result<query::MergeableAggregateState> result() const;
  [[nodiscard]] const common::Status& failure() const noexcept;
  [[nodiscard]] const query::CompatibleDistributedAggregateSnapshot& snapshot() const;
  [[nodiscard]] common::Result<std::optional<DistributedQueryLeaderHint>>
  suggested_leader(const schema::TabletId& tablet_id) const;

private:
  class Impl;
  explicit DistributedQueryTcpExecution(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_QUERY_TCP_EXECUTION_HPP_
