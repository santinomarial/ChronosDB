#ifndef CHRONOS_SERVICE_NATIVE_DISTRIBUTED_GROUPED_SHUFFLE_JOB_PROVIDER_HPP_
#define CHRONOS_SERVICE_NATIVE_DISTRIBUTED_GROUPED_SHUFFLE_JOB_PROVIDER_HPP_

#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"
#include "chronos/service/native_protocol_service.hpp"

#include <chrono>
#include <cstddef>
#include <memory>
#include <span>

namespace chronos::service {

struct NativeDistributedGroupedShuffleJobProviderConfig {
  raft::NodeId coordinator_node_id{};
  network::TlsServerConfig result_tls;
  network::ConnectionAuthenticator* authenticator{};
  const cluster::ClusterNodePrincipalAuthorizer* node_authorizer{};
  network::TcpListenerConfig result_listener;
  cluster::DistributedVectorGroupedAggregateShuffleAuthorityLimits authority;
  cluster::DistributedVectorGroupedAggregateShuffleJobControlTlsLimits carrier_limits;
  cluster::DistributedVectorGroupedAggregateShuffleJobControlTcpRetryLimits prepare_retry;
  cluster::DistributedVectorGroupedAggregateShuffleJobControlTcpRetryLimits route_install_retry;
  cluster::DistributedVectorGroupedAggregateShuffleJobControlTcpRetryLimits seal_retry;
  cluster::DistributedVectorGroupedAggregateShuffleJobControlTcpRetryLimits cancel_retry;
  std::chrono::milliseconds connect_timeout{5000};
  std::chrono::milliseconds reducer_execution_timeout{30000};
  std::size_t maximum_reducer_nodes{4096U};
  std::size_t maximum_retained_result_streams{1024U};
  std::size_t maximum_result_accepts_per_poll{32U};
  std::size_t maximum_collected_encoded_bytes{
      cluster::kDefaultDistributedVectorGroupedAggregateShuffleResultCollectorBytes};
  std::size_t maximum_batch_working_bytes{query::kDefaultVectorChunkMemoryLimit};
  std::size_t maximum_working_memory_bytes{
      cluster::kDefaultDistributedVectorGroupedAggregateShuffleCollectedResultWorkingBytes};
};

// Selects the independent-process reducer-job lifecycle only when this coordinator is a gateway:
// no source or destination is local. Job-control and result protocols deliberately reject self
// routes, so queries with a local fragment remain on the established direct grouped lifecycle.
// The authenticator and authorizer are borrowed and must outlive this provider and every prepared
// synchronous Native execution. TLS credentials are retained through shared ownership.
class NativeDistributedGroupedShuffleJobProvider final
    : public NativeDistributedGroupedShuffleProvider {
public:
  NativeDistributedGroupedShuffleJobProvider() noexcept;
  ~NativeDistributedGroupedShuffleJobProvider() override;
  NativeDistributedGroupedShuffleJobProvider(const NativeDistributedGroupedShuffleJobProvider&) =
      delete;
  NativeDistributedGroupedShuffleJobProvider&
  operator=(const NativeDistributedGroupedShuffleJobProvider&) = delete;
  NativeDistributedGroupedShuffleJobProvider(NativeDistributedGroupedShuffleJobProvider&&) noexcept;
  NativeDistributedGroupedShuffleJobProvider&
  operator=(NativeDistributedGroupedShuffleJobProvider&&) noexcept;

  [[nodiscard]] static common::Result<NativeDistributedGroupedShuffleJobProvider>
  create(NativeDistributedGroupedShuffleJobProviderConfig config);

  [[nodiscard]] common::Result<NativeDistributedGroupedShufflePlan>
  prepare(std::span<const query::DistributedMutableVectorFragment> fragments,
          std::span<const query::VectorGroupKeyDefinition> keys,
          std::span<const query::VectorAggregateDefinition> aggregates,
          std::span<const cluster::DistributedQueryNodeRoute> routes,
          std::chrono::steady_clock::time_point execution_deadline) override;

private:
  class Impl;
  explicit NativeDistributedGroupedShuffleJobProvider(
      std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_NATIVE_DISTRIBUTED_GROUPED_SHUFFLE_JOB_PROVIDER_HPP_
