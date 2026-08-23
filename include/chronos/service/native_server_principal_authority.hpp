#ifndef CHRONOS_SERVICE_NATIVE_SERVER_PRINCIPAL_AUTHORITY_HPP_
#define CHRONOS_SERVICE_NATIVE_SERVER_PRINCIPAL_AUTHORITY_HPP_

#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/service/native_server_principal_config.hpp"

#include <memory>
#include <span>
#include <vector>

namespace chronos::service {

// Immutable server-side certificate-to-principal authority. It grants coarse native-protocol
// admission only: the principal file carries no node, group, leadership, placement, or operation
// authority. All state is immutable after create, so this implementation supports concurrent reads.
class NativeServerPrincipalAuthority final : public network::ConnectionAuthenticator {
public:
  NativeServerPrincipalAuthority() = delete;
  ~NativeServerPrincipalAuthority() override;
  NativeServerPrincipalAuthority(const NativeServerPrincipalAuthority&) = delete;
  NativeServerPrincipalAuthority& operator=(const NativeServerPrincipalAuthority&) = delete;
  NativeServerPrincipalAuthority(NativeServerPrincipalAuthority&&) noexcept;
  NativeServerPrincipalAuthority& operator=(NativeServerPrincipalAuthority&&) noexcept;

  [[nodiscard]] static common::Result<NativeServerPrincipalAuthority>
  create(std::vector<NativeServerPrincipal> principals);

  [[nodiscard]] common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest& request) override;

  [[nodiscard]] std::span<const NativeServerPrincipal> principals() const noexcept;

private:
  class Impl;
  explicit NativeServerPrincipalAuthority(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_NATIVE_SERVER_PRINCIPAL_AUTHORITY_HPP_
