#ifndef CHRONOS_SERVICE_REPLICATED_PEER_AUTHORITY_HPP_
#define CHRONOS_SERVICE_REPLICATED_PEER_AUTHORITY_HPP_

#include "chronos/cluster/remote_tablet_reconfiguration.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/raft/types.hpp"
#include "chronos/service/replicated_peer_config.hpp"

#include <memory>
#include <span>
#include <vector>

namespace chronos::service {

// Immutable certificate/IP-to-node authority shared by inbound and outbound Raft TLS carriers.
// The configured node ID is also the stable nonzero principal ID. All reads are const after create,
// so authenticate/authorize may safely be called concurrently for this concrete implementation.
class ReplicatedPeerAuthority final : public network::ConnectionAuthenticator,
                                      public cluster::ClusterNodePrincipalAuthorizer {
public:
  ReplicatedPeerAuthority() = delete;
  ~ReplicatedPeerAuthority() override;
  ReplicatedPeerAuthority(const ReplicatedPeerAuthority&) = delete;
  ReplicatedPeerAuthority& operator=(const ReplicatedPeerAuthority&) = delete;
  ReplicatedPeerAuthority(ReplicatedPeerAuthority&&) noexcept;
  ReplicatedPeerAuthority& operator=(ReplicatedPeerAuthority&&) noexcept;

  [[nodiscard]] static common::Result<ReplicatedPeerAuthority>
  create(raft::NodeId local_node_id, std::vector<ReplicatedPeer> peers);

  [[nodiscard]] common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest& request) override;
  [[nodiscard]] common::Result<bool> authorize_node(std::uint64_t principal_id,
                                                    raft::NodeId claimed_node_id) const override;

  [[nodiscard]] raft::NodeId local_node_id() const noexcept;
  [[nodiscard]] const ReplicatedPeer& local_peer() const noexcept;
  [[nodiscard]] const ReplicatedPeer* find_peer(raft::NodeId node_id) const noexcept;
  [[nodiscard]] std::span<const ReplicatedPeer> peers() const noexcept;

private:
  class Impl;
  explicit ReplicatedPeerAuthority(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_REPLICATED_PEER_AUTHORITY_HPP_
