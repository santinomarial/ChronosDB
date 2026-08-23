#ifndef CHRONOS_SERVICE_NATIVE_CLIENT_TLS_ROUTE_OWNER_HPP_
#define CHRONOS_SERVICE_NATIVE_CLIENT_TLS_ROUTE_OWNER_HPP_

#include "chronos/common/result.hpp"
#include "chronos/network/native_leader_redirect_router.hpp"
#include "chronos/service/native_client_route_authority.hpp"
#include "chronos/service/native_client_route_config.hpp"

#include <cstddef>
#include <memory>
#include <span>
#include <string>

namespace chronos::service {

struct NativeClientTlsCredentials {
  std::string certificate_chain_file;
  std::string private_key_file;
  std::string trust_store_file;
};

struct NativeClientTlsRouteFileLimits {
  NativeClientRouteConfigLimits route_config;
  std::size_t maximum_tls_file_bytes{std::size_t{16U} * 1024U * 1024U};
};

struct NativeClientTlsRouteOwnerConfig {
  std::string route_config_file;
  NativeClientTlsCredentials tls;
  NativeClientTlsRouteFileLimits limits;
};

// Address-stable owner for a securely loaded native route file, its immutable certificate/node
// authority, and one expected-identity TLS client context per route. Published leader-route
// pointers remain valid until this owner is destroyed. TLS contexts own credential material after
// creation; the configured paths need not remain valid for existing contexts.
class NativeClientTlsRouteOwner {
public:
  NativeClientTlsRouteOwner() = delete;
  ~NativeClientTlsRouteOwner();
  NativeClientTlsRouteOwner(const NativeClientTlsRouteOwner&) = delete;
  NativeClientTlsRouteOwner& operator=(const NativeClientTlsRouteOwner&) = delete;
  NativeClientTlsRouteOwner(NativeClientTlsRouteOwner&&) noexcept;
  NativeClientTlsRouteOwner& operator=(NativeClientTlsRouteOwner&&) noexcept;

  [[nodiscard]] static common::Result<NativeClientTlsRouteOwner>
  load(const NativeClientTlsRouteOwnerConfig& config);

  [[nodiscard]] NativeClientRouteAuthority& authority() noexcept;
  [[nodiscard]] const NativeClientRouteAuthority& authority() const noexcept;
  [[nodiscard]] std::span<const NativeClientRoute> configured_routes() const noexcept;
  [[nodiscard]] std::span<const network::NativeLeaderRoute> leader_routes() const noexcept;

private:
  class Impl;
  explicit NativeClientTlsRouteOwner(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_NATIVE_CLIENT_TLS_ROUTE_OWNER_HPP_
