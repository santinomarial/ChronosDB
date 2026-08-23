#ifndef CHRONOS_SERVICE_NATIVE_SERVER_PRINCIPAL_CONFIG_HPP_
#define CHRONOS_SERVICE_NATIVE_SERVER_PRINCIPAL_CONFIG_HPP_

#include "chronos/common/result.hpp"
#include "chronos/network/tls_socket.hpp"

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

namespace chronos::service {

inline constexpr std::string_view kNativeServerPrincipalConfigV1Magic{
    "CHRONOSDB_NATIVE_SERVER_PRINCIPALS_V1"};

struct NativeServerPrincipalConfigLimits {
  std::size_t maximum_bytes{std::size_t{1024U} * 1024U};
  std::size_t maximum_principals{4096U};
};

struct NativeServerPrincipal {
  std::uint64_t principal_id{};
  network::PeerCertificateSha256 certificate_sha256{};

  friend bool operator==(const NativeServerPrincipal&, const NativeServerPrincipal&) = default;
};

// Parses strict server-side native-client authority. The first line is the v1 magic. Every
// following line is a positive canonical decimal principal ID, '=', and one lowercase hexadecimal
// SHA-256 leaf-certificate fingerprint. Principal IDs are strictly increasing and fingerprints are
// unique. Whitespace, comments, blank lines, CR, extra fields, and unbounded input fail closed.
[[nodiscard]] common::Result<std::vector<NativeServerPrincipal>>
parse_native_server_principal_config(std::string_view text,
                                     NativeServerPrincipalConfigLimits limits = {});

} // namespace chronos::service

#endif // CHRONOS_SERVICE_NATIVE_SERVER_PRINCIPAL_CONFIG_HPP_
