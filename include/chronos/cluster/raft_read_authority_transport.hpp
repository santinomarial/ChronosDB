#ifndef CHRONOS_CLUSTER_RAFT_READ_AUTHORITY_TRANSPORT_HPP_
#define CHRONOS_CLUSTER_RAFT_READ_AUTHORITY_TRANSPORT_HPP_

#include "chronos/cluster/raft_observation_transport.hpp"
#include "chronos/cluster/remote_tablet_reconfiguration.hpp"
#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/security.hpp"
#include "chronos/raft/multi_raft.hpp"
#include "chronos/raft/types.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace chronos::cluster {

inline constexpr std::size_t kRaftReadAuthorityRequestHeaderSize = 80U;
inline constexpr std::size_t kRaftReadAuthorityRequestSize = 84U;
inline constexpr std::size_t kRaftReadAuthorityResponseHeaderSize = 128U;
inline constexpr std::size_t kRaftReadAuthorityFrameTrailerSize = 4U;

struct RaftReadAuthorityTransportLimits {
  RaftObservationTransportLimits observation;
};

struct RaftReadAuthority {
  raft::GroupReadBarrier barrier;
  raft::RaftGroupObservation observation;

  friend bool operator==(const RaftReadAuthority&, const RaftReadAuthority&) = default;
};

struct RaftReadAuthorityRequest {
  raft::NodeId source_node_id{};
  raft::NodeId target_node_id{};
  raft::GroupId group_id;
  std::uint64_t correlation_id{};

  friend bool operator==(const RaftReadAuthorityRequest&,
                         const RaftReadAuthorityRequest&) = default;
};

struct RaftReadAuthorityResponse {
  raft::NodeId source_node_id{};
  raft::NodeId target_node_id{};
  raft::GroupId group_id;
  std::uint64_t correlation_id{};
  common::StatusCode status_code{common::StatusCode::kInternal};
  std::optional<RaftReadAuthority> authority;

  friend bool operator==(const RaftReadAuthorityResponse&,
                         const RaftReadAuthorityResponse&) = default;
};

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_raft_read_authority_request_v1(const RaftReadAuthorityRequest& request);
[[nodiscard]] common::Result<RaftReadAuthorityRequest>
decode_raft_read_authority_request_v1(common::ByteView bytes);
[[nodiscard]] common::Result<std::vector<std::byte>>
encode_raft_read_authority_response_v1(const RaftReadAuthorityResponse& response,
                                       RaftReadAuthorityTransportLimits limits = {});
[[nodiscard]] common::Result<RaftReadAuthorityResponse>
decode_raft_read_authority_response_v1(common::ByteView bytes,
                                       RaftReadAuthorityTransportLimits limits = {});

class RaftReadAuthorityService {
public:
  virtual ~RaftReadAuthorityService() = default;
  [[nodiscard]] virtual common::Result<RaftReadAuthority>
  acquire(const raft::GroupId& group_id) = 0;
};

struct RaftReadAuthorityReceiverConfig {
  raft::NodeId local_node_id{};
  const ClusterNodePrincipalAuthorizer* authorizer{};
  RaftReadAuthorityService* service{};
  RaftReadAuthorityTransportLimits limits;
};

class RaftReadAuthorityReceiver {
public:
  RaftReadAuthorityReceiver() = delete;

  [[nodiscard]] static common::Result<RaftReadAuthorityReceiver>
  create(RaftReadAuthorityReceiverConfig config);

  // Authentication is checked before request decoding. The authenticated principal must then be
  // authorized for the claimed source and the request must target the configured local node before
  // the service may issue a linearizable barrier.
  [[nodiscard]] common::Result<std::vector<std::byte>>
  receive(common::ByteView request_bytes,
          const network::PeerAuthenticationResult& authenticated_peer);

private:
  explicit RaftReadAuthorityReceiver(RaftReadAuthorityReceiverConfig config) noexcept;
  RaftReadAuthorityReceiverConfig config_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_RAFT_READ_AUTHORITY_TRANSPORT_HPP_
