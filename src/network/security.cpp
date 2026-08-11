#include "chronos/network/security.hpp"

namespace chronos::network {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] bool is_loopback(const std::array<std::uint8_t, 4>& address) noexcept {
  return address.front() == 127U;
}

} // namespace

common::Status validate_network_security_config(const NetworkSecurityConfig& config,
                                                const std::array<std::uint8_t, 4>& bind_address) {
  if (config.mode == TransportSecurityMode::kTlsRequired) {
    if (!config.tls.has_value())
      return invalid("TLS_REQUIRED needs TLS server credentials");
    if (config.authenticator == nullptr)
      return invalid("TLS_REQUIRED needs a certificate principal authenticator");
    if (config.tls->certificate_chain_file.empty() || config.tls->private_key_file.empty() ||
        config.tls->trust_store_file.empty() || !config.tls->require_client_certificate)
      return invalid("TLS_REQUIRED server credentials are invalid");
    return common::Status::ok();
  }
  if (config.mode != TransportSecurityMode::kLoopbackPlaintext)
    return invalid("transport security mode is unassigned");
  if (config.tls.has_value())
    return invalid("loopback plaintext mode must not configure TLS credentials");
  if (!is_loopback(bind_address))
    return invalid("plaintext native transport may bind only IPv4 loopback");
  return common::Status::ok();
}

common::Result<PeerAuthenticationResult>
authenticate_peer(const NetworkSecurityConfig& config, const PeerAuthenticationRequest& request) {
  if (config.mode == TransportSecurityMode::kTlsRequired &&
      (!request.transport_authenticated || !request.peer_certificate_sha256.has_value()))
    return common::make_unexpected(
        common::Status{common::StatusCode::kUnauthenticated,
                       "verified TLS peer certificate identity is required"});
  if (config.mode == TransportSecurityMode::kLoopbackPlaintext &&
      (request.transport_authenticated || request.peer_certificate_sha256.has_value() ||
       !is_loopback(request.ipv4_address)))
    return common::make_unexpected(common::Status{
        common::StatusCode::kUnauthenticated, "peer does not satisfy loopback plaintext policy"});
  if (config.authenticator == nullptr)
    return PeerAuthenticationResult{.authorized = true, .principal_id = 0U};
  auto result = config.authenticator->authenticate(request);
  if (!result.has_value())
    return common::make_unexpected(result.error());
  if (!result->authorized)
    return *result;
  if (result->principal_id == 0U)
    return common::make_unexpected(invalid("authorized peer requires a nonzero principal ID"));
  return *result;
}

} // namespace chronos::network
