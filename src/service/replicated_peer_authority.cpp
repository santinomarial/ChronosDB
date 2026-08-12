#include "chronos/service/replicated_peer_authority.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <ranges>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::service {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status unauthenticated(const char* message) {
  return {common::StatusCode::kUnauthenticated, message};
}

} // namespace

class ReplicatedPeerAuthority::Impl {
public:
  Impl(const raft::NodeId local, std::vector<ReplicatedPeer> configured,
       const std::size_t local_position) noexcept
      : local_node(local), configured_peers(std::move(configured)), local_index(local_position) {}

  [[nodiscard]] const ReplicatedPeer* find(const raft::NodeId node_id) const noexcept {
    const auto found =
        std::ranges::lower_bound(configured_peers, node_id, {}, &ReplicatedPeer::node_id);
    return found != configured_peers.end() && found->node_id == node_id ? &*found : nullptr;
  }

  raft::NodeId local_node{};
  std::vector<ReplicatedPeer> configured_peers;
  std::size_t local_index{};
};

ReplicatedPeerAuthority::ReplicatedPeerAuthority(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
ReplicatedPeerAuthority::~ReplicatedPeerAuthority() = default;
ReplicatedPeerAuthority::ReplicatedPeerAuthority(ReplicatedPeerAuthority&&) noexcept = default;
ReplicatedPeerAuthority&
ReplicatedPeerAuthority::operator=(ReplicatedPeerAuthority&&) noexcept = default;

common::Result<ReplicatedPeerAuthority>
ReplicatedPeerAuthority::create(const raft::NodeId local_node_id,
                                std::vector<ReplicatedPeer> peers) {
  if (local_node_id == 0U || peers.empty())
    return common::make_unexpected(invalid("replicated peer authority configuration is empty"));
  try {
    std::size_t local_index = peers.size();
    for (std::size_t index = 0U; index < peers.size(); ++index) {
      const ReplicatedPeer& peer = peers[index];
      if (peer.node_id == 0U || peer.endpoint.port == 0U || peer.tls_server_identity.empty())
        return common::make_unexpected(invalid("replicated peer authority entry is invalid"));
      if (index != 0U && peers[index - 1U].node_id >= peer.node_id)
        return common::make_unexpected(
            invalid("replicated peer authority node IDs are not strictly increasing"));
      if (std::ranges::any_of(std::span{peers}.first(index), [&](const ReplicatedPeer& previous) {
            return previous.endpoint == peer.endpoint ||
                   previous.certificate_sha256 == peer.certificate_sha256;
          }))
        return common::make_unexpected(
            invalid("replicated peer authority contains duplicate route authority"));
      if (peer.node_id == local_node_id)
        local_index = index;
    }
    if (local_index == peers.size())
      return common::make_unexpected(invalid("replicated peer authority omits the local node"));
    return ReplicatedPeerAuthority{
        std::make_unique<Impl>(local_node_id, std::move(peers), local_index)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("replicated peer authority allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("replicated peer authority exceeds limits"));
  }
}

common::Result<network::PeerAuthenticationResult>
ReplicatedPeerAuthority::authenticate(const network::PeerAuthenticationRequest& request) {
  if (!request.transport_authenticated || !request.peer_certificate_sha256.has_value())
    return common::make_unexpected(
        unauthenticated("replicated peer authority requires a verified TLS certificate"));
  const auto found = std::ranges::find(impl_->configured_peers, *request.peer_certificate_sha256,
                                       &ReplicatedPeer::certificate_sha256);
  if (found == impl_->configured_peers.end() || found->endpoint.address != request.ipv4_address)
    return network::PeerAuthenticationResult{};
  return network::PeerAuthenticationResult{.authorized = true, .principal_id = found->node_id};
}

common::Result<bool>
ReplicatedPeerAuthority::authorize_node(const std::uint64_t principal_id,
                                        const raft::NodeId claimed_node_id) const {
  return principal_id != 0U && principal_id == claimed_node_id &&
         impl_->find(claimed_node_id) != nullptr;
}

raft::NodeId ReplicatedPeerAuthority::local_node_id() const noexcept {
  return impl_->local_node;
}

const ReplicatedPeer& ReplicatedPeerAuthority::local_peer() const noexcept {
  return impl_->configured_peers[impl_->local_index];
}

const ReplicatedPeer*
ReplicatedPeerAuthority::find_peer(const raft::NodeId node_id) const noexcept {
  return impl_->find(node_id);
}

std::span<const ReplicatedPeer> ReplicatedPeerAuthority::peers() const noexcept {
  return impl_->configured_peers;
}

} // namespace chronos::service
