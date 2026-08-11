#ifndef CHRONOS_NETWORK_SECURITY_HPP_
#define CHRONOS_NETWORK_SECURITY_HPP_

#include "chronos/common/result.hpp"
#include "chronos/network/tls_socket.hpp"

#include <array>
#include <cstdint>
#include <optional>

namespace chronos::network {

enum class TransportSecurityMode : std::uint8_t { kLoopbackPlaintext, kTlsRequired };

struct PeerAuthenticationRequest {
  std::array<std::uint8_t, 4> ipv4_address{};
  bool transport_authenticated{};
  std::optional<PeerCertificateSha256> peer_certificate_sha256;
};

struct PeerAuthenticationResult {
  bool authorized{};
  std::uint64_t principal_id{};
};

// The authenticator is borrowed by the reactor and must outlive it. The reactor owner thread is
// its only caller. An authorized custom result must carry a nonzero stable principal identity.
class ConnectionAuthenticator {
public:
  virtual ~ConnectionAuthenticator() = default;
  [[nodiscard]] virtual common::Result<PeerAuthenticationResult>
  authenticate(const PeerAuthenticationRequest& request) = 0;
};

struct NetworkSecurityConfig {
  TransportSecurityMode mode{TransportSecurityMode::kLoopbackPlaintext};
  ConnectionAuthenticator* authenticator{};
  std::optional<TlsServerConfig> tls;
};

[[nodiscard]] common::Status
validate_network_security_config(const NetworkSecurityConfig& config,
                                 const std::array<std::uint8_t, 4>& bind_address);
[[nodiscard]] common::Result<PeerAuthenticationResult>
authenticate_peer(const NetworkSecurityConfig& config, const PeerAuthenticationRequest& request);

} // namespace chronos::network

#endif // CHRONOS_NETWORK_SECURITY_HPP_
