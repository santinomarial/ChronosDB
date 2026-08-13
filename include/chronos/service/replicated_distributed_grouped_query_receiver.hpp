#ifndef CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_GROUPED_QUERY_RECEIVER_HPP_
#define CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_GROUPED_QUERY_RECEIVER_HPP_

#include "chronos/cluster/distributed_grouped_query_transport.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/service/replicated_distributed_query_worker.hpp"

#include <cstddef>
#include <memory>
#include <vector>

namespace chronos::service {

struct ReplicatedDistributedGroupedQueryReceiverConfig {
  ReplicatedDistributedGroupedQueryWorkerConfig worker;
  const cluster::ClusterNodePrincipalAuthorizer* node_authorizer{};
  const cluster::DistributedQueryLeaderHintProvider* leader_hint_provider{};
  std::size_t maximum_response_frames{1024U};
};

// Move-only owner of the stable real-CSEG grouped worker and authenticated receiver addresses.
// Borrowed storage, authority provider, authorizer, and optional hint provider must outlive it.
class ReplicatedDistributedGroupedQueryReceiver {
public:
  ReplicatedDistributedGroupedQueryReceiver() = delete;
  ~ReplicatedDistributedGroupedQueryReceiver();
  ReplicatedDistributedGroupedQueryReceiver(const ReplicatedDistributedGroupedQueryReceiver&) =
      delete;
  ReplicatedDistributedGroupedQueryReceiver&
  operator=(const ReplicatedDistributedGroupedQueryReceiver&) = delete;
  ReplicatedDistributedGroupedQueryReceiver(ReplicatedDistributedGroupedQueryReceiver&&) noexcept;
  ReplicatedDistributedGroupedQueryReceiver&
  operator=(ReplicatedDistributedGroupedQueryReceiver&&) noexcept;

  [[nodiscard]] static common::Result<ReplicatedDistributedGroupedQueryReceiver>
  create(ReplicatedDistributedGroupedQueryReceiverConfig config);
  [[nodiscard]] common::Result<std::vector<std::vector<std::byte>>>
  receive(common::ByteView request_bytes,
          const network::PeerAuthenticationResult& authenticated_peer);

private:
  class Impl;
  explicit ReplicatedDistributedGroupedQueryReceiver(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_REPLICATED_DISTRIBUTED_GROUPED_QUERY_RECEIVER_HPP_
