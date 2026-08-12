#ifndef CHRONOS_SERVICE_REPLICATED_PEER_CONFIG_HPP_
#define CHRONOS_SERVICE_REPLICATED_PEER_CONFIG_HPP_

#include "chronos/common/result.hpp"
#include "chronos/network/tcp_socket.hpp"
#include "chronos/network/tls_socket.hpp"
#include "chronos/raft/types.hpp"

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace chronos::service {

inline constexpr std::string_view kReplicatedPeerConfigV1Magic{"CHRONOSDB_REPLICATED_PEERS_V1"};

struct ReplicatedPeerConfigLimits {
  std::size_t maximum_bytes{1024U * 1024U};
  std::size_t maximum_nodes{1024U};
  std::size_t maximum_tls_identity_bytes{253U};
};

struct ReplicatedPeer {
  raft::NodeId node_id{};
  network::Ipv4Endpoint endpoint;
  std::string tls_server_identity;
  network::PeerCertificateSha256 certificate_sha256{};

  friend bool operator==(const ReplicatedPeer&, const ReplicatedPeer&) = default;
};

// Parses strict deployment text. The first line is kReplicatedPeerConfigV1Magic. Each remaining
// line is one positive decimal node ID, '=', a canonical IPv4 endpoint, ',', a lowercase DNS name
// or canonical IPv4 TLS identity, ',', and a lowercase hexadecimal SHA-256 certificate fingerprint.
// Lines must be strictly ordered by node ID. A final LF is optional; blank lines, comments, spaces,
// CR, duplicate endpoints, and duplicate certificate fingerprints are not accepted.
[[nodiscard]] common::Result<std::vector<ReplicatedPeer>>
parse_replicated_peer_config(std::string_view text, ReplicatedPeerConfigLimits limits = {});

} // namespace chronos::service

#endif // CHRONOS_SERVICE_REPLICATED_PEER_CONFIG_HPP_
