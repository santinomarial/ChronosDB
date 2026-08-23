#ifndef CHRONOS_SERVICE_NATIVE_CLIENT_ROUTE_CONFIG_HPP_
#define CHRONOS_SERVICE_NATIVE_CLIENT_ROUTE_CONFIG_HPP_

#include "chronos/common/result.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace chronos::service {

inline constexpr std::string_view kNativeClientRouteConfigV1Magic{
    "CHRONOSDB_NATIVE_CLIENT_ROUTES_V1"};

struct NativeClientRouteConfigLimits {
  std::size_t maximum_bytes{std::size_t{1024U} * 1024U};
  std::size_t maximum_nodes{1024U};
  std::size_t maximum_tls_identity_bytes{253U};
};

struct NativeClientRoute {
  std::uint64_t node_id{};
  network::Ipv4Endpoint endpoint;
  std::string tls_server_identity;
  network::PeerCertificateSha256 certificate_sha256{};

  friend bool operator==(const NativeClientRoute&, const NativeClientRoute&) = default;
};

// Parses strict deployment text. The first line is kNativeClientRouteConfigV1Magic. Each remaining
// line is one positive decimal node ID, '=', a canonical IPv4 endpoint, ',', a lowercase DNS name
// or canonical IPv4 TLS identity, ',', and a lowercase hexadecimal SHA-256 certificate fingerprint.
// Lines must be strictly ordered by node ID. A final LF is optional; blank lines, comments, spaces,
// CR, duplicate endpoints, and duplicate certificate fingerprints are not accepted.
[[nodiscard]] common::Result<std::vector<NativeClientRoute>>
parse_native_client_route_config(std::string_view text, NativeClientRouteConfigLimits limits = {});

} // namespace chronos::service

#endif // CHRONOS_SERVICE_NATIVE_CLIENT_ROUTE_CONFIG_HPP_
