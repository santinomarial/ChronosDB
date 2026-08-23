#ifndef CHRONOS_SERVICE_NATIVE_CLIENT_ROUTE_AUTHORITY_HPP_
#define CHRONOS_SERVICE_NATIVE_CLIENT_ROUTE_AUTHORITY_HPP_

#include "chronos/common/result.hpp"
#include "chronos/network/native_quorum_ingest_tcp_client.hpp"
#include "chronos/network/security.hpp"
#include "chronos/service/native_client_route_config.hpp"

#include <cstdint>
#include <memory>
#include <span>
#include <vector>

namespace chronos::service {

// Immutable certificate/IP-to-node authority for outbound native clients. A configured node ID is
// the stable nonzero principal ID. All retained state is immutable after create, so authenticate
// and authorize_node may safely be called concurrently for this concrete implementation.
class NativeClientRouteAuthority final : public network::ConnectionAuthenticator,
                                         public network::NativeNodePrincipalAuthorizer {
public:
  NativeClientRouteAuthority() = delete;
  ~NativeClientRouteAuthority() override;
  NativeClientRouteAuthority(const NativeClientRouteAuthority&) = delete;
  NativeClientRouteAuthority& operator=(const NativeClientRouteAuthority&) = delete;
  NativeClientRouteAuthority(NativeClientRouteAuthority&&) noexcept;
  NativeClientRouteAuthority& operator=(NativeClientRouteAuthority&&) noexcept;

  [[nodiscard]] static common::Result<NativeClientRouteAuthority>
  create(std::vector<NativeClientRoute> routes);

  [[nodiscard]] common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest& request) override;
  [[nodiscard]] common::Result<bool> authorize_node(std::uint64_t principal_id,
                                                    std::uint64_t claimed_node_id) const override;

  [[nodiscard]] const NativeClientRoute* find_route(std::uint64_t node_id) const noexcept;
  [[nodiscard]] std::span<const NativeClientRoute> routes() const noexcept;

private:
  class Impl;
  explicit NativeClientRouteAuthority(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_NATIVE_CLIENT_ROUTE_AUTHORITY_HPP_
